#!/usr/bin/env python3

# otap_remote_api_tool.py - A tool to send OTAP-related
#                           Remote API requests via MQTT
#
# v0.10 2021-10-27
# Initial version
#
# Requires:
#   - Python v3.7 or newer
#   - Paho MQTT client Python module v1.5.1 or newer
#   - ProtoBuf Python module v3.14.0 or newer
#   - Wirepas Mesh Messaging Protocol Buffers Python modules in proto-py/

import sys
import time
import struct
import random
import argparse
import binascii
import configparser
from enum import Enum

try:
    import paho.mqtt.client as mqtt
except ModuleNotFoundError:
    print('module "mqtt" missing, install with "python3 -m pip install paho-mqtt"')
    sys.exit(1)

try:
    # Import Protocol Buffers message classes
    sys.path.insert(0, "proto-py")
    import generic_message_pb2 as generic_message
    import wp_global_pb2 as wp_global
    import data_message_pb2 as data_message
except ModuleNotFoundError:
    print('wirepas messaging protobuf2 modules missing in "proto-py"')
    sys.exit(1)


args = None
connection = None
client = None
req_fragments = None
keys = None


class Request(Enum):
    """Request types"""

    NONE = 0
    PING = 1
    CANCEL = 2
    REQUEST_INFO = 3
    CLEAR_BITS = 4
    CLEAR_BITS_CLEAR_LOCK = 5
    SET_BITS_SET_LOCK = 6
    CLEAR_BITS_SET_LOCK = 7
    UPDATE_SCRATCHPAD = 8
    RESET_NODE = 9
    AGENT = 10


class DummyAgent:
    """A dummy agent class for when no agent given"""

    def __init__(self, *args, **kwargs):
        pass

    def periodic(self):
        pass

    def on_message(self, recv_packet):
        pass

    def generate_remote_api_request(self, gw_id, sink_id, dest_address):
        pass


agent = DummyAgent()


def print_msg(msg):
    """Print normal messages"""
    print(msg)
    sys.stdout.flush()


def print_info(msg):
    """Print info messages"""
    if args.verbose > 0:
        print(msg)
        sys.stdout.flush()


def print_verbose(msg):
    """Print verbose messages"""
    if args.verbose > 1:
        print(msg)
        sys.stdout.flush()


def hexdump(b):
    """Convert binary data to ASCII HEX"""

    try:
        s = binascii.hexlify(b, " ", 1)
    except TypeError:
        # < v3.8 Python
        s = binascii.hexlify(b)

    return '"%s"' % s.decode("ascii")


def parse_feature_lock_key(key):
    """Parse and verify a Feature Lock Key"""

    key = key.strip()
    if key == "":
        return None

    try:
        key_bytes = [int(n, 16) for n in key.split()]
        if len(key_bytes) != 16:
            raise ValueError
    except ValueError:
        raise ValueError('invalid key: "%s"' % key)

    return " ".join(["%02X" % n for n in key_bytes])


def read_connection_ini(file_):
    """Read connection settings INI file"""

    global connection

    class MqttSettings:
        def __init__(
            self, host=None, port=None, username=None, password=None, use_tls=None
        ):
            self.host = host
            self.port = port
            self.username = username
            self.password = password
            self.use_tls = use_tls

        @staticmethod
        def _fields():
            return ("host", "port", "username", "password", "use_tls")

    class RemoteApi:
        def __init__(self, feature_lock_key=None):
            self.feature_lock_key = feature_lock_key

        @staticmethod
        def _fields():
            return ("feature_lock_key",)

    class Gateway:
        def __init__(self, sinks=[]):
            self.sinks = sinks

        @staticmethod
        def _fields():
            return ("sinks",)

    class Connection:
        def __init__(self, mqtt_settings=MqttSettings(), remote_api={}, gateways={}):
            self.mqtt_settings = mqtt_settings
            self.remote_api = remote_api
            self.gateways = gateways

    connection = Connection()

    ini = configparser.ConfigParser()
    ini.read_file(file_)

    if not ini.has_section("mqtt"):
        raise ValueError('missing "mqtt" section')

    for section in ini.sections():
        if section == "mqtt":
            for option in MqttSettings._fields():
                if not ini.has_option("mqtt", option):
                    raise ValueError('missing option "%s" in section "mqtt"' % option)
                if option == "port":
                    value = ini.getint("mqtt", option)
                elif option == "use_tls":
                    value = ini.getboolean("mqtt", option)
                else:
                    raw = (option == "password") and True or False
                    value = ini.get("mqtt", option, raw=raw)
                setattr(connection.mqtt_settings, option, value)
        elif section.startswith("remote_api:"):
            remote_api = section.split(":", 1)[1]

            for option in RemoteApi._fields():
                if not ini.has_option(section, option):
                    raise ValueError(
                        'missing option "%s" in section "%s"' % (option, section)
                    )

            feature_lock_key = parse_feature_lock_key(
                ini.get(section, "feature_lock_key")
            )

            connection.remote_api[remote_api] = RemoteApi(feature_lock_key)
        elif section.startswith("gateway:"):
            gateway = section.split(":", 1)[1]

            for option in Gateway._fields():
                if not ini.has_option(section, option):
                    raise ValueError(
                        'missing option "%s" in section "%s"' % (option, section)
                    )

            sinks = ini.get(section, "sinks").replace(",", " ").split()
            if len(sinks) == 0:
                # No sinks, don't add gateway
                continue

            connection.gateways[gateway] = Gateway(sinks)
        else:
            raise ValueError('invalid section "%s"' % section)

    # DEBUG: See what was read from the INI file
    if False:
        for field in connection.mqtt_settings._fields():
            print_msg("%s: %s" % (field, getattr(connection.mqtt_settings, field)))
        for key, value in connection.remote_api.items():
            for field in value._fields():
                print_msg("%s: %s: %s" % (key, field, getattr(value, field)))
        for key, value in connection.gateways.items():
            for field in value._fields():
                print_msg("%s: %s: %s" % (key, field, getattr(value, field)))

    # Count sinks
    num_sinks = 0
    for gw in connection.gateways:
        num_sinks += len(connection.gateways[gw].sinks)
    print_msg("using %d gateways, %d sinks" % (len(connection.gateways), num_sinks))


def read_node_list(file_):
    """Read a list of nodes"""

    nodes = []

    try:
        for line in file_:
            # Remove comments, commas, whitespace
            line = line.replace(";", "#")
            line = line[: line.find("#")]
            line = line.replace(",", " ").strip()

            if not line:
                continue

            nodes.extend([int(s) for s in line.split()])
    finally:
        file_.close()

    return nodes


def init_remote_api_fragments():
    """Create Remote API Request fragments and keys"""

    global req_fragments, keys

    no_key = "FF " * 16
    key = no_key
    new_key = no_key  # Default new key is "no key" if not set

    if args.feature_lock_key is not None:
        key = connection.remote_api[args.feature_lock_key].feature_lock_key
        if key is None:
            # Empty key is interpreted as "no key"
            key = no_key
    if args.new_feature_lock_key is not None:
        new_key = connection.remote_api[args.new_feature_lock_key].feature_lock_key
        if new_key is None:
            # Empty key is interpreted as "no key"
            new_key = no_key

    if args.feature_lock_key is not None:
        # Begin with Lock
        begin = "02 10" + key
    else:
        # Begin
        begin = "01 00"

    end_and_update = (
        "03 00"
        + "05 02"
        + ("%02X" % (args.update_delay & 0xFF))
        + ("%02X" % (args.update_delay >> 8))
    )

    # Remote API Request fragments
    req_fragments = {"begin": begin, "end_and_update": end_and_update}

    # Feature Lock Keys
    keys = {"no_key": no_key, "key": key, "new_key": new_key}


def parse_arguments():
    """Parse command line arguments"""

    global args

    request_names = [n.name.lower() for n in Request]

    parser = argparse.ArgumentParser(
        description="A tool to send OTAP-related Remote API requests via MQTT",
        epilog="requests: %s" % ", ".join(request_names),
    )

    parser.add_argument(
        "connection_ini",
        type=argparse.FileType("r", encoding="UTF-8"),
        help="connection details as INI file",
    )

    parser.add_argument("request", type=str, help="request to perform")

    parser.add_argument(
        "-f",
        "--node_list",
        metavar="file",
        type=argparse.FileType("r", encoding="UTF-8"),
        help="file with a list of node addresses (mutually exclusive to --broadcast)",
    )

    parser.add_argument(
        "-b",
        "--broadcast",
        action="store_true",
        help="broadcast request to all nodes (mutually exclusive to --node_list)",
    )

    parser.add_argument(
        "-k",
        "--feature_lock_key",
        metavar="key",
        type=str,
        default=None,
        help="use feature lock key for request (default: do not use feature lock key)",
    )

    parser.add_argument(
        "-l",
        "--new_feature_lock_key",
        metavar="key",
        type=str,
        default=None,
        help="feature lock key to set for requests set_bits_set_lock "
        "and clear_bits_set_lock (default: clear feature lock key)",
    )

    parser.add_argument(
        "-p",
        "--batch_size",
        metavar="num",
        type=int,
        default=10,
        help="batch size of parallel requests (when --broadcast not used, default 10)",
    )

    parser.add_argument(
        "-i",
        "--initial_delay",
        metavar="sec",
        type=int,
        default=5,
        help="initial request delay in seconds (default 5)",
    )

    parser.add_argument(
        "-d",
        "--repeat_delay",
        metavar="sec",
        type=int,
        default=0,
        help="delay between repeated requests in seconds (no repeat by default)",
    )

    parser.add_argument(
        "-u",
        "--update_delay",
        metavar="sec",
        type=int,
        default=10,
        help="update delay in seconds for update_scratchpad request (default: 10)",
    )

    parser.add_argument(
        "-s",
        "--sequence",
        metavar="seq",
        type=int,
        help="sequence number for update_scratchpad request",
    )

    parser.add_argument(
        "-a",
        "--any_source",
        action="store_true",
        help="show received packets from all gateways and sinks, "
        "even those not listed in connection_ini file",
    )

    parser.add_argument(
        "-v", "--verbose", action="count", help="verbosity, can use up to two"
    )

    args = parser.parse_args()

    if not args.verbose:
        args.verbose = 0

    if args.node_list and args.broadcast:
        parser.error("cannot use both --node_list and --broadcast")

    try:
        try:
            # Read connection settings from INI file
            read_connection_ini(args.connection_ini)
        except ValueError as e:
            parser.error("--connection_ini: %s" % str(e))
    finally:
        args.connection_ini.close()

    if args.broadcast:
        # Broadcast
        args.node_list = [None]

        print_msg("sending requests to all nodes")
    elif args.node_list:
        try:
            # Read node list from file
            args.node_list = read_node_list(args.node_list)
        except ValueError as e:
            parser.error(str(e))

        num_nodes = len(args.node_list)
        print_msg(
            "sending requests to %d node%s" % (num_nodes, num_nodes and "s" or "")
        )
    else:
        # No nodes selected
        args.node_list = []

        print_msg("not sending requests, no nodes selected")

    init_remote_api_fragments()

    try:
        if not args.request.upper().startswith("AGENT:"):
            args.request = Request[args.request.upper()]
            agent_name = None
        else:
            _, agent_name = args.request.split(":", 1)
            args.request = Request["AGENT"]
    except KeyError:
        parser.error("invalid --request: %s" % args.request)

    if (
        args.feature_lock_key is not None
        and args.feature_lock_key not in connection.remote_api
    ):
        parser.error("invalid --feature_lock_key: %s" % args.feature_lock_key)

    if args.batch_size < 1:
        parser.error("invalid --batch_size: %d" % args.batch_size)

    if args.initial_delay < 0:
        parser.error("invalid --initial_delay: %d" % args.initial_delay)

    if args.repeat_delay < 0:
        parser.error("invalid --repeat_delay: %d" % args.repeat_delay)

    if args.update_delay < 10 or args.update_delay > 32767:
        parser.error("invalid --update_delay: %d" % args.update_delay)

    if args.request == Request.UPDATE_SCRATCHPAD and not args.sequence:
        parser.error("missing --sequence with --request=update_scratchpad")

    if args.sequence and (args.sequence < 1 or args.sequence > 254):
        parser.error("invalid --sequence: %d" % args.sequence)

    # DEBUG: Show parsed command line arguments
    if False:
        print_msg(repr(args))

    if agent_name is not None:
        # Import agent class
        global agent

        if ":" in agent_name:
            agent_name, agent_param = agent_name.split(":", 1)
        else:
            agent_param = ""

        agent_class = __import__(f"agent_{agent_name}", fromlist="Agent")

        # Functions available to the agent
        agent_functions = {
            "send_remote_api_request": send_remote_api_request,
            "print_msg": print_msg,
            "print_info": print_info,
            "print_verbose": print_verbose,
        }

        # Create agent
        try:
            agent = agent_class.Agent(
                agent_param,
                agent_functions,
                connection,
                args.repeat_delay,
                args.node_list,
                req_fragments,
                keys,
            )
        except (ValueError, FileNotFoundError) as exc:
            parser.error(f"(agent) {exc}")


def on_connect(client, userdata, flags, rc):
    """Connection callback"""

    print_info("connected with result code %s, flags: %s" % (rc, flags))

    # Subscribe to topic "gw-event/received_data/#" to receive data packets.
    # The MQTT topics are documented in WP-RM-123 – WNT Gateway to Backend
    # Interface.
    #
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed
    client.subscribe("gw-event/received_data/#")


def on_disconnect(client, userdata, rc):
    """Disconnection callback"""

    print_info("disconnected with result code %s" % rc)


def parse_remote_api(source_address, payload):
    """Parse a Remote API response packet"""

    while len(payload) > 0:
        if len(payload) < 2:
            print_info("truncated tlv record")
            break

        tlv_type = payload[0]
        tlv_len = payload[1]
        tlv_payload = payload[2 : (2 + tlv_len)]

        if len(tlv_payload) != tlv_len:
            print_info("truncated tlv record")
            break

        if tlv_type == 0x99 and tlv_len >= 24:
            (
                st_len,
                st_crc,
                st_seq,
                st_type,
                st_sta,
                fw_len,
                fw_crc,
                fw_seq,
                fw_id,
                fw_maj,
                fw_min,
                fw_mnt,
                fw_dev,
            ) = struct.unpack("<LHBBBLHBLBBBB", tlv_payload[:24])

            st_str = (
                "st_len: %d, st_crc: 0x%04X, st_seq: %d"
                ", st_type: 0x%02X, st_sta: 0x%02X"
            ) % (st_len, st_crc, st_seq, st_type, st_sta)

            fw_str = (
                ", fw_len: %d, fw_crc: 0x%04X"
                ", fw_seq: %d, fw_id: 0x%08X"
                ", fw_ver: %d.%d.%d.%d"
            ) % (fw_len, fw_crc, fw_seq, fw_id, fw_maj, fw_min, fw_mnt, fw_dev)

            if tlv_len >= 39:
                (
                    app_len,
                    app_crc,
                    app_seq,
                    app_id,
                    app_maj,
                    app_min,
                    app_mnt,
                    app_dev,
                ) = struct.unpack("<LHBLBBBB", tlv_payload[24:39])
                app_str = (
                    ", app_len: %d, app_crc: 0x%04X"
                    ", app_seq: %d, app_id: 0x%08X"
                    ", app_ver: %d.%d.%d.%d"
                ) % (
                    app_len,
                    app_crc,
                    app_seq,
                    app_id,
                    app_maj,
                    app_min,
                    app_mnt,
                    app_dev,
                )
            else:
                app_str = ""

            print_info(
                "status from %d, %s%s%s" % (source_address, st_str, fw_str, app_str)
            )
        elif tlv_type == 0x8E and tlv_len == 3 and tlv_payload[:2] == b"\x04\x00":
            role = tlv_payload[2]
            base_role = role & 0x07
            role_names = {0x01: "sink", 0x02: "headnode", 0x03: "subnode"}
            role_str = [role_names.get(base_role, "unknown")]
            if role & 0x10:
                role_str.append("ll")
            if role & 0x80:
                role_str.append("ar")
            role_str = "+".join(role_str)
            print_info("node %d role %s (0x%02x) " % (source_address, role_str, role))
        elif tlv_type == 0x8E and tlv_len == 6 and tlv_payload[:2] == b"\x16\x00":
            if tlv_payload[5] & 0x80:
                print_info("otap lock bit not set on %d" % source_address)
            else:
                print_info("otap lock bit set on %d" % source_address)
        elif tlv_type == 0xF9 and tlv_len == 3 and tlv_payload == b"\x0e\x17\x00":
            print_info("feature lock key set on %d" % source_address)
        elif tlv_type == 0xFD and tlv_len == 3 and tlv_payload == b"\x0e\x17\x00":
            print_info("feature lock key not set on %d" % source_address)

        payload = payload[(tlv_len + 2) :]


def on_message(client, userdata, mqtt_msg):
    """MQTT message callback"""

    if mqtt_msg.topic.startswith("gw-event/received_data/"):
        # Wirepas Mesh data packet parse bytes to a Protocol Buffers message
        proto_msg = generic_message.GenericMessage()
        proto_msg.ParseFromString(mqtt_msg.payload)
        recv_packet = proto_msg.wirepas.packet_received_event

        if args.any_source:
            print_info(
                'data packet from gateway "%s" sink "%s"'
                % (recv_packet.header.gw_id, recv_packet.header.sink_id)
            )
        else:
            gw_id = recv_packet.header.gw_id
            if gw_id not in connection.gateways:
                # Not a message for any listed gateway
                return
            elif recv_packet.header.sink_id not in connection.gateways[gw_id].sinks:
                # Not a message for any listed sink under a listed gateway
                return

        if (
            recv_packet.source_endpoint == 240
            and recv_packet.destination_endpoint == 255
        ):
            # Remote API response packet
            print_info(
                "remote API response from %d travel time: %.3f s"
                % (recv_packet.source_address, recv_packet.travel_time_ms / 1000.0)
            )

            # Show hex dump of received packet
            print_verbose(hexdump(recv_packet.payload))

            parse_remote_api(recv_packet.source_address, recv_packet.payload)
        elif (
            recv_packet.source_endpoint == 253
            and recv_packet.destination_endpoint == 255
            and len(recv_packet.payload) >= 3
        ):
            # Node diagnostic v1
            print_info(
                "node diagnostic v1 from %d travel time: %.3f s"
                % (recv_packet.source_address, recv_packet.travel_time_ms / 1000.0)
            )

            role = recv_packet.payload[2]

            role_text = []
            if role & 0x10:
                role_text.append("low-latency")
            else:
                role_text.append("low-energy")

            if role & 0x80:
                role_text.append("auto-role")

            if role & 0x01:
                role_text.append("subnode")
            elif role & 0x02:
                role_text.append("headnode")
            elif role & 0x04:
                role_text.append("sink")
            else:
                role_text.append("unknown")

            print_info(
                "node %d is a %s" % (recv_packet.source_address, " ".join(role_text))
            )
        else:
            # Some other data packet
            print_verbose(
                "data from %d: %d bytes travel time: %.3f s "
                "source dest ep: %d %d"
                % (
                    recv_packet.source_address,
                    len(recv_packet.payload),
                    recv_packet.travel_time_ms / 1000.0,
                    recv_packet.source_endpoint,
                    recv_packet.destination_endpoint,
                )
            )

        # Tell agent that a data packet was received
        agent.on_message(recv_packet)
    else:
        # Something else, print it as is
        print_verbose("message on topic %s: '%s'" % (mqtt_msg.topic, mqtt_msg.payload))


def send_remote_api_request(payload, gw_id, sink_id, dest_address=None):
    """Remote API request send function"""

    if dest_address == None:
        target_name = " broadcast"
        dest_address = 0xFFFFFFFF  # Broadcast
    else:
        target_name = " to node %d" % dest_address

    print_info("sending data to %s:%s%s" % (gw_id, sink_id, target_name))

    payload = binascii.unhexlify(payload.replace(" ", ""))

    # Show hex dump of sent packet
    print_verbose(hexdump(payload))

    # Create an empty GenericMessage()
    proto_msg = generic_message.GenericMessage()

    # Fill out the message fields. WirepasMessage()
    # and SendPacketReq() are created automatically
    sp_req = proto_msg.wirepas.send_packet_req
    sp_req.header.req_id = random.randint(0, 2**48)  # At least 48 bits of randomness
    sp_req.header.sink_id = sink_id
    sp_req.destination_address = dest_address
    sp_req.source_endpoint = 255  # Wirepas reserved
    sp_req.destination_endpoint = 240  # Remote API
    sp_req.qos = 0  # Normal QoS
    sp_req.payload = payload  # Remote API request

    # Convert Protocol Buffers message to bytes
    payload = proto_msg.SerializeToString()

    # Publish MQTT message to the given gateway and sink
    client.publish("gw-request/send_data/%s/%s" % (gw_id, sink_id), payload)


def send_request(gw_id, sink_id, dest_address):
    """Generate and send a Remote API request"""

    if args.request == Request.NONE:
        # Do nothing
        return
    elif args.request == Request.PING:
        # Empty Remote API Ping request
        payload = "00 00"
    elif args.request == Request.CANCEL:
        # Cancel request
        payload = "04 00"
    elif args.request == Request.REQUEST_INFO:
        # Read Scratchpad Status + read node role + read Feature Lock Bits, Key
        payload = "19 00" + "0E 02 04 00" + "0E 02 16 00" + "0E 02 17 00"
    elif args.request == Request.CLEAR_BITS:
        # Clear Feature Lock Bits to all 1's
        payload = (
            req_fragments["begin"]
            + "0D 06 16 00 FF FF FF FF"
            + req_fragments["end_and_update"]
        )
    elif args.request == Request.CLEAR_BITS_CLEAR_LOCK:
        # Clear Feature Lock Bits to all 1's and clear the Feature Lock Key
        payload = (
            req_fragments["begin"]
            + "0D 06 16 00 FF FF FF FF"
            + "0D 12 17 00"
            + keys["no_key"]
            + req_fragments["end_and_update"]
        )
    elif args.request == Request.SET_BITS_SET_LOCK:
        # Set Feature Lock Bits and set the Feature Lock Key
        payload = (
            req_fragments["begin"]
            + "0D 06 16 00 FF FF FF 7F"
            + "0D 12 17 00"
            + keys["new_key"]
            + req_fragments["end_and_update"]
        )
    elif args.request == Request.CLEAR_BITS_SET_LOCK:
        # Clear Feature Lock Bits and set the Feature Lock Key
        payload = (
            req_fragments["begin"]
            + "0D 06 16 00 FF FF FF FF"
            + "0D 12 17 00"
            + keys["new_key"]
            + req_fragments["end_and_update"]
        )
    elif args.request == Request.UPDATE_SCRATCHPAD:
        # Process scratchpad after a short delay
        payload = (
            req_fragments["begin"]
            + "1A 01"
            + ("%02X" % args.sequence)
            + req_fragments["end_and_update"]
        )
    elif args.request == Request.RESET_NODE:
        # Reset node by setting cNodeRole to the value
        # it already has, by using a mask of 0x00
        payload = (
            req_fragments["begin"]
            + "0D 04 04 00 82 00"
            + req_fragments["end_and_update"]
        )
    elif args.request == Request.AGENT:
        # Tell agent that it is time send a Remote API request
        payload = agent.generate_remote_api_request(gw_id, sink_id, dest_address)
        if not payload:
            # Nothing to do
            return
    else:
        # Invalid request
        return

    send_remote_api_request(payload, gw_id, sink_id, dest_address)


def main():
    """Main program"""

    global client

    parse_arguments()

    # Create an MQTT client connection
    client = mqtt.Client(clean_session=True)
    client.enable_logger()  # Show exceptions in callbacks
    client.username_pw_set(
        connection.mqtt_settings.username, connection.mqtt_settings.password
    )
    if connection.mqtt_settings.use_tls:
        # Turn on SSL/TLS support for MQTT connection
        client.tls_set()

    # Register MQTT client callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Connect to MQTT broker
    print_msg(
        "connecting to %s:%d"
        % (connection.mqtt_settings.host, connection.mqtt_settings.port)
    )
    client.connect(connection.mqtt_settings.host, connection.mqtt_settings.port, 60)

    # TODO: Query gateways and sinks with a GetConfigsReq message

    print_verbose("running main loop...")

    last_message_time = time.time() - (args.repeat_delay - args.initial_delay)
    first_req = True
    index = 0

    while True:
        # Process network traffic dispatch callbacks and handle reconnecting.
        # Other loop*() functions are available that give a threaded or
        # manual interface
        client.loop(timeout=1.0)

        # Call periodic handler function of agent
        agent.periodic()

        # Send a Remote API request message periodically
        now = time.time()
        if (first_req or args.repeat_delay > 0) and (
            now - last_message_time >= args.repeat_delay
        ):
            # First and possibly only request
            first_req = False

            # Select next batch of nodes
            end_index = min(index + args.batch_size, len(args.node_list))
            nodes = args.node_list[index:end_index]

            # Time to send another message
            for gw_id in connection.gateways:
                for sink_id in connection.gateways[gw_id].sinks:
                    for node_addr in nodes:
                        send_request(gw_id, sink_id, dest_address=node_addr)

            last_message_time = now

            index = end_index
            if index == len(args.node_list):
                index = 0


# Run main
if __name__ == "__main__":
    sys.exit(main())
