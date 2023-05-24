#!/usr/bin/env python3

# gateway_tool.py - A tool to configure gateways via MQTT

import sys
import os
import time
import struct
import argparse
import binascii
from enum import Enum

import connection_ini_parser

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

# Silence wirepas_mqtt_library prints
import logging

logging.getLogger().setLevel(logging.CRITICAL)


# Print upload progress every 60 seconds
PROGRESS_TIMEOUT = 60

# Magic 16-byte string for locating a combi scratchpad in Flash
SCRATCHPAD_V1_TAG = b"SCR1\232\223\060\202\331\353\012\374\061\041\343\067"

# Minimum number of bytes in a valid combi scratchpad file
SCRATCHPAD_MIN_LENGTH = 48

# Maximum number of bytes in app config data
APP_CONFIG_DATA_MAX_LENGTH = 80


args = None
connection = None
scratchpad_data = bytearray()
scratchpad_crc = 0x0000
wni = None


class Command(Enum):
    """Command types"""

    FIND_GWS_SINKS = 0
    SCRATCHPAD_INFO = 1
    SET_APP_CONFIG = 2
    UPLOAD_SCRATCHPAD = 3


class ParallelWniRequests:
    """A base class for running WirepasNetworkInterface requests in parallel"""

    def __init__(
        self, wni, batch_size, timeout, gws_sinks_to_try=None, op_text="request"
    ):
        # Initialize state variables
        self.wni = wni
        self.sinks_to_try = []  # A list of tuples of (gw_id, sink_id)
        if gws_sinks_to_try is not None:
            self.sinks_to_try.insert(gws_sinks_to_try)
        self.sinks_in_progress = {}  # A dict of tuples of (gw_id, sink_id)
        self.results = (
            []
        )  # A list of tuples of (GatewayResultCode, (gw_id, sink_id), other_data)
        self.last_progress = 0
        self.batch_size = batch_size
        self.timeout = timeout
        self.progress_timeout = PROGRESS_TIMEOUT
        self.op_text = op_text

    def add_sinks(self, gws_sinks):
        # Add given (gw_id, sink_id) list to the list of sinks to try
        self.sinks_to_try.insert(gws_sinks)

    def add_all_sinks(self, gws):
        # Collect all sinks of given gateways in the list of sinks to try
        for gw_id in gws.keys():
            for sink_id in gws[gw_id].sinks:
                self.sinks_to_try.append((gw_id, sink_id))

    def run(self):
        while (
            len(self.sinks_to_try) > 0
            or len(self.sinks_in_progress) > 0
            or len(self.results) > 0
        ):
            # Handle results of requests
            if len(self.results) > 0:
                res, gw_sink, other_data = self.results.pop(0)
                gw_id, sink_id = gw_sink
                if self.sinks_in_progress.pop(gw_sink, None) is None:
                    # Ignore duplicate responses
                    pass
                elif self.is_request_result_ok(res, gw_id, sink_id):
                    # Request OK, nothing more to do for that sink
                    self.request_done(gw_id, sink_id, other_data)
                else:
                    # Request failed, add sink back to end of sinks_to_try list
                    if gw_sink not in self.sinks_to_try:
                        self.sinks_to_try.append(gw_sink)
                    self.request_failed(res, gw_id, sink_id, other_data)
                continue

            now = time.time()

            if now - self.last_progress > self.progress_timeout:
                # Report progress periodically
                self.progress(len(self.sinks_to_try) + len(self.sinks_in_progress))
                self.last_progress = now

            # Handle request timeouts
            for gw_sink in self.sinks_in_progress:
                if now - self.sinks_in_progress[gw_sink] >= self.timeout:
                    # Request timed out
                    self.request_done_cb(None, gw_sink)
                    continue

            if len(self.sinks_in_progress) >= self.batch_size:
                # Maximum number of parallel requests in progress, wait for a bit
                time.sleep(1)
                continue

            if len(self.sinks_to_try) == 0:
                # Nothing to do, wait for a bit
                time.sleep(1)
                continue

            gw_sink = self.sinks_to_try.pop(0)
            self.sinks_in_progress[gw_sink] = time.time()

            gw_id, sink_id = gw_sink

            try:
                # Start an asynchronous request
                self.perform_request(gw_id, sink_id)
            except TimeoutError:
                # Could not start request, return sink back to sinks_to_try list
                self.sinks_in_progress.pop(gw_sink, None)
                self.sinks_to_try.append(gw_sink)
                self.request_failed(None, gw_id, sink_id, ())

    def perform_request(self, gw_id, sink_id):
        # Default implementation, should be overloaded
        print_verbose(f"gw: {gw_id}, sink: {sink_id}, performing request")
        raise TimeoutError

    def request_done_cb(self, res, gw_sink, *other):
        # Got result, handle it in the main thread
        self.results.append((res, gw_sink, other))

    def is_request_result_ok(self, res, gw_id, sink_id):
        # Default implementation, can be overloaded
        return res == GatewayResultCode.GW_RES_OK

    def request_done(self, gw_id, sink_id, other_data):
        # Default implementation, can be overloaded
        print_msg(f"gw: {gw_id}, sink: {sink_id}, {self.op_text} done")

    def request_failed(self, res, gw_id, sink_id, other_data):
        # Default implementation, can be overloaded
        res_text = (res is None) and "timed out" or f"{res}"
        print_msg(f"gw: {gw_id}, sink: {sink_id}, {self.op_text} failed: {res_text}")

    def progress(self, num_sinks):
        # Default implementation, can be overloaded
        print_info(f"{num_sinks} sinks left")


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
        "-b",
        "--batch_size",
        metavar="num",
        type=int,
        default=10,
        help="maximum number of parallel requests (default 10)",
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
        "-c",
        "--app_config_data",
        metavar="data",
        type=str,
        help="literal data for set_app_config command",
    )

    parser.add_argument(
        "-p",
        "--app_config_process",
        action="store_true",
        help="set the process flag for set_app_config command (default: propagate only)",
    )

    parser.add_argument(
        "-s",
        "--scratchpad_seq",
        metavar="seq",
        type=int,
        help="sequence number for upload_scratchpad and set_app_config commands",
    )

    parser.add_argument(
        "-f",
        "--scratchpad_file",
        type=argparse.FileType("rb"),
        help="scratchpad file for uploading",
    )

    parser.add_argument(
        "-w",
        "--write_ini_file",
        type=argparse.FileType("w"),
        help="INI file to write for find_gws_sinks command",
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

    if args.app_config_data is not None:
        # Parse literal app config data
        try:
            app_config_data = binascii.unhexlify(
                args.app_config_data.replace(" ", "").replace(",", "")
            )
            if len(args.app_config_data) > APP_CONFIG_DATA_MAX_LENGTH:
                raise ValueError
        except ValueError:
            parser.error('invalid --app_config_data: "%s"' % args.app_config_data)
        args.app_config_data = app_config_data
        need_scratchpad_cmds = (Command.UPLOAD_SCRATCHPAD,)

        if args.app_config_process:
            parser.error("--app_config_process cannot be used with --app_config_data")
        elif args.scratchpad_seq is not None or args.scratchpad_file is not None:
            parser.error(
                "--scratchpad_seq or --scratchpad_file cannot be used with --app_config_data"
            )
    else:
        # App config data for v4.x OTAP Manager, need scratchpad seq and file
        need_scratchpad_cmds = (Command.SET_APP_CONFIG, Command.UPLOAD_SCRATCHPAD)

    if args.command in need_scratchpad_cmds and args.scratchpad_seq is None:
        parser.error(
            "missing --scratchpad_seq with command set_app_config or update_scratchpad"
        )

    if args.scratchpad_seq is not None and (
        args.scratchpad_seq < 0 or args.scratchpad_seq > 255
    ):
        parser.error("invalid --scratchpad_seq: %d" % args.scratchpad_seq)

    if args.command in need_scratchpad_cmds and args.scratchpad_file is None:
        parser.error(
            "missing --scratchpad_file with command set_app_config or update_scratchpad"
        )

    if args.scratchpad_file:
        # Read scratchpad file
        scratchpad_data = args.scratchpad_file.read()
        args.scratchpad_file.close()
        if (
            len(scratchpad_data) < SCRATCHPAD_MIN_LENGTH
            or scratchpad_data[:16] != SCRATCHPAD_V1_TAG
        ):
            parser.error("invalid --scratchpad_file contents")
        (scratchpad_crc,) = struct.unpack("<H", scratchpad_data[20:22])

    # DEBUG: Show parsed command line arguments
    if False:
        print_msg(repr(args))


def command_find_gws_sinks():
    global args
    global connection
    global wni

    print_verbose(f"finding gateways")

    gws_found = set()
    for _ in range(args.retry_count):
        try:
            gws_found |= set(wni.get_gateways())
        except TimeoutError:
            # Timed out, try again
            pass

        # For enumerating gateways, repeat query for the full amount of retries
        time.sleep(1)

    gws_listed = set(connection.gateways.keys())
    gws_missing = gws_listed - gws_found
    extra_gws = gws_found - gws_listed

    # Print gateway statistics
    print_msg(
        f"listed gws: {len(gws_listed)}, gws found: {len(gws_found)}, gws missing: {len(gws_missing)}, extra gws: {len(extra_gws)}"
    )

    print_verbose(f"finding sinks")

    sinks_per_gw_found = {}

    num_sinks_listed = 0
    num_sinks_found = 0
    num_missing_sinks = 0
    num_extra_sinks = 0

    # Sort all gateways
    gws_sorted = list(gws_listed | gws_found)
    gws_sorted.sort()

    for gw_id in gws_sorted:
        if gw_id not in gws_found:
            print_msg(f"gw: {gw_id}, missing gw")
            continue

        sinks = {}

        if gw_id in gws_listed:
            sinks_listed = set(connection.gateways[gw_id].sinks)
        else:
            sinks_listed = set()

        for _ in range(args.retry_count):
            try:
                sinks_temp = wni.get_sinks(gateway=gw_id)
                sinks_temp = dict([(s[1], s[2]) for s in sinks_temp])  # To dict
            except TimeoutError:
                # Timed out, try again
                sinks_temp = {}

            sinks.update(sinks_temp)
            sinks_found = set(sinks.keys())

            if len(sinks_listed) > 0:
                if len(sinks_listed - sinks_found) == 0:
                    # No more sinks to find for this gateway
                    break

            # For extra gateways, repeat query for the full amount of retries
            time.sleep(1)

        sinks_per_gw_found[gw_id] = sinks_found

        # Sort all sinks
        sinks_sorted = list(sinks_listed | sinks_found)
        sinks_sorted.sort()

        for sink_id in sinks_sorted:
            if sink_id not in sinks_found:
                msg = "missing sink"
            else:
                sink_info = sinks[sink_id]
                try:
                    msg = f'started: {sink_info["started"]}, node_addr: {sink_info["node_address"]}, nw_addr: 0x{sink_info["network_address"]:08x}, nw_ch: {sink_info["network_channel"]}, app_c_seq: {sink_info["app_config_seq"]}, app_c_diag: {sink_info["app_config_diag"]}'
                except KeyError:
                    msg = "missing information"

            extra_gw_msg = (gw_id not in gws_listed) and ", extra gw" or ""
            extra_sink_msg = (sink_id not in sinks_listed) and ", extra sink" or ""

            print_msg(
                f"gw: {gw_id}, sink: {sink_id}, {msg}{extra_gw_msg}{extra_sink_msg}"
            )

        # Update sink statistics
        num_sinks_listed += len(sinks_listed)
        num_sinks_found += len(sinks_found)
        num_missing_sinks += len(sinks_listed - sinks_found)
        num_extra_sinks += len(sinks_found - sinks_listed)

    # Print sink statistics
    print_msg(
        f"listed sinks: {num_sinks_listed}, sinks found: {num_sinks_found}, sinks missing: {num_missing_sinks}, extra sinks: {num_extra_sinks}"
    )

    if args.write_ini_file is not None:
        # Write all listed and found gateways and sinks to an INI file fragment
        print_info(f"writing INI file fragment {args.write_ini_file.name}")
        for gw_id in gws_sorted:
            # Collect all listed and found sinks
            sinks_sorted = set()
            if gw_id in connection.gateways:
                sinks_sorted |= set(connection.gateways[gw_id].sinks)
            if gw_id in sinks_per_gw_found:
                sinks_sorted |= sinks_per_gw_found[gw_id]
            sinks_sorted = list(sinks_sorted)
            sinks_sorted.sort()

            # Mention extra / missing gateways or sinks in a comment
            comment_msg_list = []
            if gw_id not in connection.gateways:
                comment_msg_list.append(f"extra gw: {gw_id}")
            elif gw_id not in sinks_per_gw_found:
                comment_msg_list.append(f"missing gw: {gw_id}")
            else:
                sinks_missing = []
                extra_sinks = []

                for sink_id in sinks_sorted:
                    if sink_id not in sinks_per_gw_found[gw_id]:
                        # Missing sink
                        sinks_missing.append(sink_id)
                    elif sink_id not in connection.gateways[gw_id].sinks:
                        # Extra sink
                        extra_sinks.append(sink_id)

                if len(sinks_missing) > 0 or len(extra_sinks) > 0:
                    comment_msg_list.append(f"gw: {gw_id}")

                if len(sinks_missing) > 0:
                    comment_msg_list.append(f'sinks missing: {",".join(sinks_missing)}')

                if len(extra_sinks) > 0:
                    comment_msg_list.append(f'extra sinks: {",".join(extra_sinks)}')

            if len(comment_msg_list) > 0:
                # Print comment
                args.write_ini_file.write(f'# NOTE: {", ".join(comment_msg_list)}\n')

            # Print "gateway:" section with sinks
            args.write_ini_file.write(f"[gateway:{gw_id}]\n")
            args.write_ini_file.write(f'sinks: {",".join(sinks_sorted)}\n\n')
        args.write_ini_file.close()


def command_scratchpad_info():
    global args
    global connection
    global wni

    class ParallelScratchpadInfoRequests(ParallelWniRequests):
        def perform_request(self, gw_id, sink_id):
            global args

            print_verbose(f"gw: {gw_id}, sink: {sink_id}, querying scratchpad info")

            # Asynchronously query sink info
            wni.get_scratchpad_status(
                gw_id, sink_id, self.request_done_cb, (gw_id, sink_id)
            )

        def request_done(self, gw_id, sink_id, other_data):
            try:
                # Extract scratchpad info
                scr_status = other_data[0]
                msg = f'st_len: {scr_status["stored_scratchpad"]["len"]}, st_crc: 0x{scr_status["stored_scratchpad"]["crc"]:04x}, st_seq: {scr_status["stored_scratchpad"]["seq"]}'
            except (IndexError, KeyError):
                msg = f"missing info"

            # Print scratchpad info
            print_msg(f"gw: {gw_id}, sink: {sink_id}, {msg}")

        def progress(self, num_sinks):
            print_info(f"querying scratchpad info on {num_sinks} sinks")

    preq = ParallelScratchpadInfoRequests(
        wni, args.batch_size, args.timeout, op_text="scratchpad info query"
    )
    preq.add_all_sinks(connection.gateways)
    preq.run()


def command_set_app_config():
    global args
    global connection
    global wni

    class ParallelConfigRequests(ParallelWniRequests):
        def perform_request(self, gw_id, sink_id):
            global args
            nonlocal new_sink_config

            print_verbose(f"gw: {gw_id}, sink: {sink_id}, setting sink config")

            # Asynchronously set sink configuration
            self.wni.set_sink_config(
                gw_id,
                sink_id,
                new_sink_config,
                self.request_done_cb,
                (gw_id, sink_id),
            )

        def is_request_result_ok(self, res, gw_id, sink_id):
            if res == GatewayResultCode.GW_RES_INVLAID_SEQUENCE_NUMBER:
                # Wrong sequence number, do not repeat request for that sink
                print_msg(f"gw: {gw_id}, sink: {sink_id}, invalid app config seq")
                return True
            return super().is_request_result_ok(res, gw_id, sink_id)

        def progress(self, num_sinks):
            global args
            nonlocal otap_crc
            nonlocal otap_action

            if args.app_config_data is None:
                msg = f", app_c_seq: {args.app_config_seq}, app_c_diag: {args.app_config_diag}, otap_crc: 0x{otap_crc:04x}, otap_seq: {args.scratchpad_seq}, otap_action: 0x{otap_action:02x}"
            else:
                msg = ""

            print_info(f"setting sink config on {num_sinks} sinks{msg}")

    enable_otap = args.scratchpad_seq != 0

    if args.app_config_data is not None:
        # Literal app config data
        app_config_data = args.app_config_data
        otap_crc = 0x0000  # Dummy, unused
        otap_action = 0x00  # Dummy, unused
    else:
        # App config data for v4.x OTAP Manager
        magic = enable_otap and 0xBA61 or 0x0000
        otap_crc = enable_otap and scratchpad_crc or 0x0000
        otap_action = enable_otap and (args.app_config_process and 0x02 or 0x01) or 0x00

        app_config_data = bytearray(
            struct.pack("<HHBB", magic, otap_crc, args.scratchpad_seq, otap_action)
        )

    print_verbose(
        f'app config, seq: {args.app_config_seq}, diag: {args.app_config_diag}, data: {" ".join(["%02x" % n for n in app_config_data])}'
    )

    new_sink_config = {
        "app_config_seq": args.app_config_seq,
        "app_config_diag": args.app_config_diag,
        "app_config_data": app_config_data,
    }

    preq = ParallelConfigRequests(
        wni, args.batch_size, args.timeout, op_text="sink config"
    )
    preq.add_all_sinks(connection.gateways)
    preq.run()


def command_upload_scratchpad():
    global args
    global connection
    global wni

    class ParallelUploadRequests(ParallelWniRequests):
        def perform_request(self, gw_id, sink_id):
            global args
            global scratchpad_data

            print_verbose(f"gw: {gw_id}, sink: {sink_id}, uploading scratchpad")

            # Start an asynchronous scratchpad upload
            self.wni.upload_scratchpad(
                gw_id,
                sink_id,
                args.scratchpad_seq,
                scratchpad_data,
                self.request_done_cb,
                (gw_id, sink_id),
                timeout=args.timeout * 9 // 10,  # A bit shorter than request timeout
            )

        def progress(self, num_sinks):
            global args
            global scratchpad_data
            global scratchpad_crc

            print_info(
                f"uploading scratchpad to {num_sinks} sinks, st_len: {len(scratchpad_data)}, st_crc: 0x{scratchpad_crc:04x}, st_seq: {args.scratchpad_seq}"
            )

    preq = ParallelUploadRequests(wni, args.batch_size, args.timeout, op_text="upload")
    preq.add_all_sinks(connection.gateways)
    preq.run()


def main():
    """Main program"""

    global args
    global connection
    global wni

    parse_arguments()

    # HACK: Increase gateway request timeouts
    WirepasNetworkInterface._TIMEOUT_NETWORK_CONNECTION_S = 10  # 4 originally
    WirepasNetworkInterface._TIMEOUT_GW_CONFIG_S = args.timeout  # 2 originally
    WirepasNetworkInterface._TIMEOUT_GW_STATUS_S = args.timeout  # 2 originally
    WirepasNetworkInterface._wait_for_response_old = (
        WirepasNetworkInterface._wait_for_response
    )

    def _wait_for_response_new(self, cb, req_id, timeout=args.timeout, param=None):
        return self._wait_for_response_old(cb, req_id, timeout, param)

    WirepasNetworkInterface._wait_for_response = _wait_for_response_new

    # Connect to MQTT broker
    wni = WirepasNetworkInterface(
        connection.mqtt_settings.host,
        connection.mqtt_settings.port,
        connection.mqtt_settings.username,
        connection.mqtt_settings.password,
        insecure=not connection.mqtt_settings.use_tls,
        strict_mode=True,
        clean_session=True,
        # TODO: Not implemented yet in release version of wirepas_mqtt_library
        #        gw_timeout_s=args.timeout,
    )

    # BUG: Dummy request needed, otherwise the first request fails
    wni.get_sinks(gateway="DUMMY")

    # Perform command
    cmd_func = globals()["command_" + args.command.name.lower()]
    cmd_func()


# Run main
if __name__ == "__main__":
    sys.exit(main())
