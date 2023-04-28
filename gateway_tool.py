#!/usr/bin/env python3

# gateway_tool.py - A tool to configure gateways via MQTT

import sys
import os
import time
import struct
import argparse
from enum import Enum

import connection_ini_parser

# Force support for older protobuf classes present in Wirepas MQTT Library
# A harmless "Could not evaluate protobuf implementation type" warning is printed
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

try:
    # Import Wirepas MQTT Library
    from wirepas_mqtt_library import WirepasNetworkInterface
except ModuleNotFoundError:
    print(
        'module "wirepas_mqtt_library" missing, install with "python -m pip install wirepas-mqtt-library"'
    )
    sys.exit(1)

try:
    # Import Wirepas Mesh Messaging classes
    from wirepas_mesh_messaging.gateway_result_code import GatewayResultCode
except ModuleNotFoundError:
    print(
        'module "wirepas_mesh_messaging" missing, install with "python -m pip install wirepas-mesh-messaging"'
    )
    sys.exit(1)


args = None
connection = None
scratchpad_data = bytearray()
scratchpad_crc = 0x0000
wni = None


class Command(Enum):
    """Command types"""

    INFO = 0
    SET_APP_CONFIG = 1
    UPLOAD_SCRATCHPAD = 2


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


def parse_arguments():
    """Parse command line arguments"""

    global connection
    global args
    global scratchpad_data
    global scratchpad_crc

    command_names = [n.name.lower() for n in Command]

    parser = argparse.ArgumentParser(
        description="A tool to configure gateways via MQTT",
        epilog="commands: %s" % ", ".join(command_names),
    )

    parser.add_argument(
        "connection_ini",
        type=argparse.FileType("r", encoding="UTF-8"),
        help="connection details as INI file",
    )

    parser.add_argument("command", type=str, help="task to perform")

    parser.add_argument(
        "-t",
        "--timeout",
        metavar="sec",
        type=int,
        default=10,
        help="command timeout in seconds (default: 10)",
    )

    parser.add_argument(
        "-r",
        "--retry_count",
        metavar="num",
        type=int,
        default=5,
        help="command retry count (default: 5)",
    )

    parser.add_argument(
        "-a",
        "--app_config_seq",
        metavar="seq",
        type=int,
        help="sequence number for set_app_config command",
    )

    parser.add_argument(
        "-d",
        "--app_config_diag",
        metavar="interval",
        type=int,
        help="diagnostic interval for set_app_config command",
    )

    parser.add_argument(
        "-s",
        "--scratchpad_seq",
        metavar="seq",
        type=int,
        help="sequence number for upload_scratchpad command",
    )

    parser.add_argument(
        "-f",
        "--scratchpad_file",
        type=argparse.FileType("rb"),
        help="scratchpad file for uploading",
    )

    parser.add_argument(
        "-v", "--verbose", action="count", help="verbosity, can use up to two"
    )

    args = parser.parse_args()

    if not args.verbose:
        args.verbose = 0

    try:
        try:
            # Read connection settings from INI file
            connection = connection_ini_parser.read_connection_ini(args.connection_ini)
        except ValueError as e:
            parser.error("connection_ini: %s" % str(e))
    finally:
        args.connection_ini.close()

    try:
        args.command = Command[args.command.upper()]
    except KeyError:
        parser.error("invalid command: %s" % args.command)

    if args.command == Command.SET_APP_CONFIG and args.app_config_seq is None:
        parser.error("missing --app_config_seq with command set_app_config")

    if args.app_config_seq is not None and (
        args.app_config_seq < 0 or args.app_config_seq > 255
    ):
        parser.error("invalid --app_config_seq: %d" % args.app_config_seq)

    if args.command == Command.SET_APP_CONFIG and args.app_config_diag is None:
        parser.error("missing --app_config_diag with command set_app_config")

    if args.app_config_diag is not None and (
        args.app_config_diag < 0 or args.app_config_diag > 65535
    ):
        parser.error("invalid --app_config_diag: %d" % args.app_config_diag)

    if (
        args.command in (Command.SET_APP_CONFIG, Command.UPLOAD_SCRATCHPAD)
        and args.scratchpad_seq is None
    ):
        parser.error(
            "missing --scratchpad_seq with command set_app_config or update_scratchpad"
        )

    if args.scratchpad_seq is not None and (
        args.scratchpad_seq < 0 or args.scratchpad_seq > 255
    ):
        parser.error("invalid --scratchpad_seq: %d" % args.scratchpad_seq)

    if (
        args.command in (Command.SET_APP_CONFIG, Command.UPLOAD_SCRATCHPAD)
        and not args.scratchpad_file
    ):
        parser.error(
            "missing --scratchpad_file with command set_app_config or update_scratchpad"
        )

    if args.scratchpad_file:
        # Read scratchpad file
        scratchpad_data = args.scratchpad_file.read()
        args.scratchpad_file.close()
        if (
            scratchpad_data[:16]
            != b"SCR1\232\223\060\202\331\353\012\374\061\041\343\067"
        ):
            parser.error("invalid --scratchpad_file contents")
        (scratchpad_crc,) = struct.unpack("<H", scratchpad_data[20:22])

    # DEBUG: Show parsed command line arguments
    if False:
        print_msg(repr(args))


def command_info():
    global args
    global connection
    global wni

    num_listed_gws = 0
    num_found_gws = 0
    num_listed_sinks = 0
    num_found_sinks = 0

    for gw_id, gw in connection.gateways.items():
        num_listed_gws += 1
        listed_sinks = set(gw.sinks)
        num_listed_sinks += len(listed_sinks)

        sinks_info = {}
        for _ in range(args.retry_count):
            try:
                sinks_info_temp = wni.get_sinks(gateway=gw_id)
            except TimeoutError:
                continue

            # Convert sink info list to dict
            sinks_info_temp = dict([(sink[1], sink[2]) for sink in sinks_info_temp])
            sinks_info.update(sinks_info_temp)

            avail_sinks = set(sinks_info.keys())

            extra_sinks = avail_sinks - listed_sinks
            missing_sinks = listed_sinks - avail_sinks
            query_sinks = avail_sinks & listed_sinks

            # Try again if not all sinks found
            if missing_sinks == 0:
                break

            # Wait a bit before trying again
            time.sleep(1)

        if len(sinks_info) == 0:
            print_msg(f"gw: {gw_id}, timed out")
            continue

        num_found_gws += 1

        if False:  # DEBUG
            print_msg(
                repr(avail_sinks),
                repr(listed_sinks),
                repr(extra_sinks),
                repr(missing_sinks),
            )

        # Prepare an info message about missing or extra sinks
        gw_sink_str_list = [gw_id]

        if len(missing_sinks) > 0:
            missing_str = ",".join([sink for sink in missing_sinks])
            missing_str = f'sinks missing: "{missing_str}"'
            gw_sink_str_list.append(missing_str)

        if len(extra_sinks) > 0:
            extra_str = ",".join([sink for sink in extra_sinks])
            extra_str = f'extra sinks: "{extra_str}"'
            gw_sink_str_list.append(extra_str)

        if len(missing_sinks) > 0 or len(extra_sinks) > 0:
            print_msg(f'gw: {", ".join(gw_sink_str_list)}')

        for sink_id in query_sinks:
            sink_info = sinks_info[sink_id]

            for _ in range(args.retry_count):
                try:
                    scr_status = wni.get_scratchpad_status(gw_id, sink_id)
                    if scr_status[0] == GatewayResultCode.GW_RES_OK:
                        scr_status = scr_status[1]
                        break
                except TimeoutError:
                    scr_status = None

            if sink_info is not None and scr_status is not None:
                msg = f'gw: {gw_id}, sink: {sink_id}, started: {sink_info["started"]}, node_addr: {sink_info["node_address"]}, nw_addr: 0x{sink_info["network_address"]:08x}, nw_ch: {sink_info["network_channel"]}, st_len: {scr_status["stored_scratchpad"]["len"]}, st_crc: 0x{scr_status["stored_scratchpad"]["crc"]:04x}, st_seq: {scr_status["stored_scratchpad"]["seq"]}, app_c_seq: {sink_info["app_config_seq"]}, app_c_diag: {sink_info["app_config_diag"]}'
                num_found_sinks += 1
            else:
                msg = f"gw: {gw_id}, sink: {sink_id}, timed out"

            print_msg(msg)

    print_msg(f"listed gateways: {num_listed_gws}, gateways found: {num_found_gws}, gateways missing: {num_listed_gws - num_found_gws}")
    print_msg(f"listed sinks: {num_listed_sinks}, sinks found: {num_found_sinks}, sinks missing: {num_listed_sinks - num_found_sinks}")


def command_set_app_config():
    global args
    global connection
    global scratchpad_data
    global scratchpad_crc
    global wni

    enable_otap = args.scratchpad_seq != 0

    magic = enable_otap and 0xBA61 or 0x0000
    otap_crc = enable_otap and scratchpad_crc or 0x0000
    otap_action = enable_otap and 0x02 or 0x00

    app_config_data = bytearray(
        struct.pack("<HHBB", magic, otap_crc, args.scratchpad_seq, otap_action)
    )

    print_verbose(
        f'app config, seq: {args.app_config_seq}, diag: {args.app_config_diag}, data: {" ".join(["%02x" % n for n in app_config_data])}'
    )

    new_app_config = {
        "app_config_seq": args.app_config_seq,
        "app_config_diag": args.app_config_diag,
        "app_config_data": app_config_data,
    }

    for gw_id, gw in connection.gateways.items():
        for sink_id in gw.sinks:
            success = False
            wrong_seq = False
            for _ in range(args.retry_count):
                try:
                    # Set new app config
                    res = wni.set_sink_config(gw_id, sink_id, new_app_config)
                    if res == GatewayResultCode.GW_RES_OK:
                        success = True
                        break
                    elif res == GatewayResultCode.GW_RES_INVLAID_SEQUENCE_NUMBER:
                        wrong_seq = True
                        break
                except TimeoutError:
                    pass

                # Wait a bit before trying again
                time.sleep(1)

            if success:
                msg = f"gw: {gw_id}, sink: {sink_id}, app config set, app_c_seq: {args.app_config_seq}, app_c_diag: {args.app_config_diag}, otap_crc: 0x{otap_crc:04x}, otap_seq: {args.scratchpad_seq}, otap_action: 0x{otap_action:02x}"
            elif wrong_seq:
                msg = f"gw: {gw_id}, sink: {sink_id}, invalid app config seq"
            else:
                msg = f"gw: {gw_id}, sink: {sink_id}, timed out"

            print_msg(msg)


def command_upload_scratchpad():
    global args
    global connection
    global scratchpad_data
    global scratchpad_crc
    global wni

    for gw_id, gw in connection.gateways.items():
        for sink_id in gw.sinks:
            success = False
            for _ in range(args.retry_count):
                try:
                    # Upload scratchpad
                    res = wni.upload_scratchpad(
                        gw_id, sink_id, args.scratchpad_seq, scratchpad_data
                    )
                    if res == GatewayResultCode.GW_RES_OK:
                        success = True
                        break
                except TimeoutError:
                    pass

                # Wait a bit before trying again
                time.sleep(1)

            if success:
                msg = f"gw: {gw_id}, sink: {sink_id}, uploaded scratchpad, st_len: {len(scratchpad_data)}, st_crc: 0x{scratchpad_crc:04x}, st_seq: {args.scratchpad_seq}"
            else:
                msg = f"gw: {gw_id}, sink: {sink_id}, timed out"

            print_msg(msg)


def main():
    """Main program"""

    global args
    global connection
    global wni

    parse_arguments()

    # Connect to MQTT broker
    wni = WirepasNetworkInterface(
        connection.mqtt_settings.host,
        connection.mqtt_settings.port,
        connection.mqtt_settings.username,
        connection.mqtt_settings.password,
        insecure=not connection.mqtt_settings.use_tls,
        strict_mode=False,
    )

    print_verbose("running main loop...")

    # Perform command
    if args.command == Command.INFO:
        command_info()
    elif args.command == Command.SET_APP_CONFIG:
        command_set_app_config()
    elif args.command == Command.UPLOAD_SCRATCHPAD:
        command_upload_scratchpad()


# Run main
if __name__ == "__main__":
    sys.exit(main())
