#!/usr/bin/env python

import sys
import time
import binascii

from agent_smart_enable import Agent

import connection_ini_parser


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


def send_remote_api_request(payload, gw_id, sink_id, dest_address=None):
    if dest_address == None:
        target_name = " broadcast"
        dest_address = 0xFFFFFFFF  # Broadcast
    else:
        target_name = " to node %d" % dest_address

    print_info("sending data to %s:%s%s" % (gw_id, sink_id, target_name))

    payload = binascii.unhexlify(payload.replace(" ", ""))
    print_msg(hexdump(payload))


last_receive_time = 0


def receive_next_packet():
    global last_receive_time

    now = time.time()

    if now - last_receive_time < 5:
        return None
    last_receive_time = now

    class ReceivedPacket:
        pass

    class ReceivedPacketHeader:
        pass

    # Dummy packet metadata, Remote API Response packet
    recv_packet = ReceivedPacket()
    recv_packet.header = ReceivedPacketHeader()
    recv_packet.header.gw_id = "tollandgw00"
    recv_packet.header.sink_id = "sink1"
    recv_packet.travel_time_ms = 1234
    recv_packet.rx_time_ms_epoch = int(now * 1000) - recv_packet.travel_time_ms
    recv_packet.source_address = 1000
    recv_packet.source_endpoint = 240
    recv_packet.destination_endpoint = 255
    recv_packet.payload = b""

    # Test payload
    #    payload = "80 04 da 37 66 64 82 00 8d 06 16 00 ff ff ff ff 8d 02 17 00 9a 01 7d 83 00 85 02 0b 00"
    payload = "80 04 2b da 66 64 82 00 8d 06 16 00 ff ff ff ff 8d 02 17 00 9a 01 7d 83 00 85 02 10 00"
    recv_packet.payload = binascii.unhexlify(payload.replace(" ", ""))

    return recv_packet


class Args:
    pass


args = Args()
args.node_list = None  # None means broadcast, or allow all
args.sequence = None  # None for no automatic update request
args.repeat_delay = 30
args.update_delay = 10
args.verbose = 2

with open("tollandct.ini", "r") as ini_file:
    connection = connection_ini_parser.read_connection_ini(ini_file, require_mqtt=False)

# Feature Lock Keys
keys = {"no_key": "FF " * 16, "key": "00 " * 16, "new_key": "00 " * 16}

if True:
    # Begin with Lock
    begin = "02 10" + keys["key"]
else:
    # Begin
    begin = "01 00"

end_and_update = (
    "03 00"
    + "05 02"
    + ("%02X" % (args.update_delay & 0xFF))
    + ("%02X" % (args.update_delay >> 8))
)

req_fragments = {"begin": begin, "end_and_update": end_and_update}

agent_param = "agent_node_db_testbench.sqlite3:allowed_seqs.txt"

# Functions available to the agent
agent_functions = {
    "send_remote_api_request": send_remote_api_request,
    "print_msg": print_msg,
    "print_info": print_info,
    "print_verbose": print_verbose,
}

# Create agent
agent = Agent(
    agent_param,
    agent_functions,
    args,
    connection,
    req_fragments,
    keys,
)

while True:
    # Call periodic handler function of agent
    agent.periodic()
    time.sleep(1)

    # Receive packet
    recv_packet = receive_next_packet()

    if recv_packet is None:
        # No packet received
        continue

    if recv_packet.source_endpoint == 240 and recv_packet.destination_endpoint == 255:
        # Remote API response packet
        print_info(
            "remote API response from %d travel time: %.3f s"
            % (recv_packet.source_address, recv_packet.travel_time_ms / 1000.0)
        )

        # Show hex dump of received packet
        print_msg(hexdump(recv_packet.payload))

    agent.on_message(recv_packet)
