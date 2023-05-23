#!/usr/bin/env python3

# agent_smart_enable.py - An intelligent agent to handle sending Remote API
#                         requests and parsing responses, to enable OTAP on
#                         large networks in a controlled manner

import time
import struct

import node_db
import remote_api
from remote_api import RemoteApiResponseType


class Agent:
    """An intelligent agent to handle Remote API requests and responses"""

    # Interval for sending periodic requests, seconds
    PERIODIC_INTERVAL = 10  # 10 seconds

    # Request timeout, seconds
    REQUEST_TIMEOUT = 3 * 60  # Three minutes

    def __init__(self, param, functions, args, connection, req_fragments, keys):
        def not_impl(*args, **kwargs):
            pass

        # Unpack functions and use an empty function if no function given
        self.send_remote_api_request = functions.get(
            "send_remote_api_request", not_impl
        )
        self.print_msg = functions.get("print_msg", not_impl)
        self.print_info = functions.get("print_info", not_impl)
        self.print_verbose = functions.get("print_verbose", not_impl)

        if False:  # DEBUG
            self.print_msg("agent: __init__()")

        self.connection = connection
        self.node_list = args.node_list  # None means broadcast, or allow all
        self.update_seq = args.sequence  # None for no automatic update request
        self.req_fragments = req_fragments
        self.keys = keys

        params = param.split(":")
        if len(params) != 2:
            raise ValueError("invalid number of parameters")

        self.db = node_db.NodeDb(params[0])

        self.allowed_seqs = _read_allowed_seqs(params[1])
        self.request_repeat_interval = args.repeat_delay
        self.last_periodic_ts = time.time()  # Starts after one periodic interval

    def periodic(self):
        """Periodic handler function, called every second or so"""

        if False:  # DEBUG
            self.print_msg("agent: periodic()")

        # Check if it is time to carry out periodic tasks
        now = time.time()
        if now - self.last_periodic_ts < self.PERIODIC_INTERVAL:
            # Not yet
            return
        self.last_periodic_ts = now

        if self.request_repeat_interval == 0:
            # Not sending periodic update requests
            return

        # Find a node the oldest request time or missing info
        timeout = self.request_repeat_interval * 3 // 2  # 50% extra time
        node_addr = self.db.find_node_oldest_req(now, timeout)
        if node_addr is None:
            # No nodes found
            return

        self.print_info(f"node {node_addr} periodic request (all gateways and sinks)")

        # Create an info update request
        req_ts, payload = self._generate_info_req()

        # Send to all gateways and all sinks
        for gw_id in self.connection.gateways:
            for sink_id in self.connection.gateways[gw_id].sinks:
                self.send_remote_api_request(payload, gw_id, sink_id, node_addr)

        # Update node information with request timestamp and phase
        self.db.open_transaction()
        self.db.add_or_update_node(
            node_addr,
            phase=node_db.Phase.INFO_REQ,
            last_req=req_ts,
        )
        self.db.commit()

    def on_message(self, recv_packet):
        """Message reception handler function, called on every received packet"""

        # DEBUG: Print received packet
        if False:
            self.print_msg("agent: on_message()")
            self.print_msg(repr(recv_packet))

        now = time.time()
        node_addr = recv_packet.source_address

        if self.node_list is not None and node_addr not in self.node_list:
            # Not in node list and not broadcast, leave
            return

        # TODO: Detect if the gateway clock is not set correctly
        #       rx_time = (recv_packet.rx_time_ms_epoch -
        #                  recv_packet.travel_time_ms) / 1000.0
        rx_time = now - recv_packet.travel_time_ms / 1000.0

        last_seen = rx_time  # Update timestamp, unless newer packets already seen
        phase = node_db.Phase.INIT
        last_req = None
        last_info = None
        seq_old = None

        # See if the node is known already
        node_info = self.db.find_node(node_addr)
        if node_info is not None:
            last_info = node_info["last_info"]
            seq_old = node_info["st_seq"]

            last_seen_old = node_info["last_seen"]
            if last_seen_old is not None and last_seen_old >= last_seen:
                # Newer packets have already been seen, do not touch timestamp
                last_seen = None

            # Get current phase, default to "INIT"
            if node_info["phase"] is not None:
                phase = node_info["phase"]
        else:
            self.print_info(f"new node {node_addr} seen")

        # Run state machine
        phase, updates = self._state_machine(now, phase, node_info, recv_packet)

        if "last_req" not in updates and node_info is not None:
            # If no new request was sent, check request timeout and periodic
            # info timeout here, and run the state machine again if needed
            if phase != node_db.Phase.DONE:
                # Handle request timeouts
                last_req = node_info["last_req"]
                if last_req is None or (now - last_req) >= self.REQUEST_TIMEOUT:
                    self.print_info(f"node {node_addr} request timeout")
                    phase = node_db.Phase.INIT
            elif self.request_repeat_interval > 0:
                # Request info periodically
                last_info = node_info["last_info"]
                if (
                    last_info is not None  # Shouldn't happen with Phase.DONE
                    and (now - last_info) >= self.request_repeat_interval
                ):
                    self.print_info(f"node {node_addr} periodic request")
                    phase = node_db.Phase.INIT

            if phase == node_db.Phase.INIT:
                # Run state machine again
                phase, updates2 = self._state_machine(now, phase, node_info, recv_packet)

                # Combine updates from both state machine runs
                updates.update(updates2)

        seq = updates.get("st_seq", None)
        if seq_old is not None and seq is not None and seq_old != seq:
            self.print_msg(
                f"otap seq number changed from {seq_old} to {seq} on node {node_addr}"
            )

        # Update node information
        self.db.open_transaction()
        self.db.add_or_update_node(
            node_addr,
            last_seen=last_seen,
            phase=phase,
            node_role=updates.get("node_role", None),
            lock_status=updates.get("lock_status", None),
            last_req=updates.get("last_req", None),
            last_resp=updates.get("last_resp", None),
            last_info=updates.get("last_info", None),
            st_len=updates.get("st_len", None),
            st_crc=updates.get("st_crc", None),
            st_seq=updates.get("st_seq", None),
            st_type=updates.get("st_type", None),
            st_status=updates.get("st_sta", None),
            st_blob=updates.get("st_blob", None),
        )
        self.db.commit()

    def generate_remote_api_request(self, gw_id, sink_id, node_addr):
        """Send handler function, called when it is time to send a request to a given node"""

        if False:  # DEBUG
            self.print_msg("agent: send_remote_api_request()")

        # The smart enable agent has no use for this function
        return None

    def _state_machine(self, now, phase, node_info, recv_packet):
        """State machine for locking / unlocking nodes"""

        updates = {}

        node_addr = recv_packet.source_address
        source_endpoint = recv_packet.source_endpoint
        dest_endpoint = recv_packet.destination_endpoint

        # TODO: Detect if the gateway clock is not set correctly
        #       rx_time = (recv_packet.rx_time_ms_epoch -
        #                  recv_packet.travel_time_ms) / 1000.0
        rx_time = now - recv_packet.travel_time_ms / 1000.0

        if phase == node_db.Phase.INIT:
            self.print_info(f"sending info request to {node_addr}")

            # New node, send info request
            last_req = self._send_info_req(recv_packet)
            updates["last_req"] = last_req

            # Advance to the next phase and wait for response
            return node_db.Phase.INFO_REQ, updates

        if source_endpoint != 240 or dest_endpoint != 255:
            # Not a Remote API response packet, leave
            return phase, updates

        if phase == node_db.Phase.INFO_REQ:
            # Remote API response packet received, parse
            # it to see if it is the info response
            info = self._parse_info_resp(node_info, recv_packet)
            if not info:
                return phase, updates

            updates["last_resp"] = rx_time
            updates["last_info"] = rx_time
            updates.update(info)

            # Check stored sequence number and lock / unlock accordingly
            lock = info["st_seq"] not in self.allowed_seqs

            # Targeted scratchpad update request
            scr_update = (
                self.update_seq is not None
                and info["st_sta"] == 0xFF
                and info["st_seq"] == self.update_seq
            )

            if scr_update:
                # Always send lock / unlock request if scratchpad update requested
                pass
            elif lock and (info["lock_status"] == node_db.OtapLockStatus.LOCKED):
                self.print_info(f"node {node_addr} already locked, nothing to do")
                return node_db.Phase.DONE, updates
            elif not lock and (info["lock_status"] == node_db.OtapLockStatus.UNLOCKED):
                self.print_info(f"node {node_addr} already unlocked, nothing to do")
                return node_db.Phase.DONE, updates

            self.print_info(
                f'got info response from {node_addr}, sending {scr_update and f"seq {self.update_seq} update and " or ""}{lock and "lock" or "unlock"} request'
            )

            # Info response OK, send lock or unlock request
            last_req = self._send_lock_unlock_req(
                recv_packet, info["lock_status"], lock, scr_update
            )
            updates["last_req"] = last_req

            # Advance to the next phase and wait for response
            return node_db.Phase.LOCK_UNLOCK_REQ, updates

        if phase == node_db.Phase.LOCK_UNLOCK_REQ:
            # Lock / unlock response received
            lock_bits_set = self._parse_lock_unlock_resp(node_info, recv_packet)
            if lock_bits_set is not None:
                self.print_info(
                    f"got lock / unlock response from {node_addr}, configuration done"
                )
                updates["lock_status"] = (
                    lock_bits_set
                    and node_db.OtapLockStatus.LOCKED
                    or node_db.OtapLockStatus.UNLOCKED
                )
                updates["last_resp"] = rx_time
                return node_db.Phase.DONE, updates

        # Something else, do nothing
        return phase, updates

    def _parse_common_resp(self, node_info, recv_packet, resp_len=None):
        try:
            resp = remote_api.parse_response(recv_packet.payload)
        except ValueError as exc:
            # Invalid response, ignore it
            self.print_info(exc)
            return None

        if False:  # DEBUG
            self.print_msg(repr(resp))

        if resp_len is not None and len(resp) != resp_len:
            # Not a valid Remote API response packet
            self.print_verbose("invalid remote api response packet")
            return None

        # Check first response type and payload length
        if (
            resp[0]["type"] != RemoteApiResponseType.PING
            or len(resp[0]["payload"]) != 4
        ):
            # No PING response as the first response in the packet
            self.print_verbose("missing ping payload in remote api response packet")
            return None

        # Check request timestamp
        (req_time,) = struct.unpack("<L", resp[0]["payload"])
        if req_time != node_info["last_req"]:
            # Not a response to the last request
            self.print_verbose("remote api response packet timestamp not latest")
            return None

        # Request timestamp is OK
        return resp

    def _generate_info_req(self):
        req_ts = int(time.time())

        # Ping with timestamp + read Scratchpad Status + read node role +
        # read Feature Lock Bits + read Feature Lock Key
        return req_ts, (
            "00 04"
            + _timestamp_as_hex(req_ts)
            + "19 00"
            + "0E 02 04 00"
            + "0E 02 16 00"
            + "0E 02 17 00"
        )

    def _send_info_req(self, recv_packet):
        gw_id = recv_packet.header.gw_id
        sink_id = recv_packet.header.sink_id
        node_addr = recv_packet.source_address
        req_ts, payload = self._generate_info_req()
        self.send_remote_api_request(payload, gw_id, sink_id, node_addr)
        return req_ts

    def _parse_info_resp(self, node_info, recv_packet):
        resp = self._parse_common_resp(node_info, recv_packet, 5)
        if resp is None:
            # Not a valid Remote API response packet
            return None

        info = {}

        try:
            if resp[1]["type"] != RemoteApiResponseType.MSAP_SCRATCHPAD_STATUS:
                raise ValueError

            # Check stored scratchpad sequence number and other info
            info.update(resp[1])

            if resp[2]["type"] != RemoteApiResponseType.READ_CSAP_ATTRIBUTE:
                raise ValueError

            info["node_role"] = resp[2]["value"]

            if resp[3]["type"] != RemoteApiResponseType.READ_CSAP_ATTRIBUTE:
                raise ValueError

            # Check if Feature Lock Bits set
            lock_bits_set = resp[3]["value"] & 0x80000000 == 0

            if resp[4]["type"] not in (
                RemoteApiResponseType.WRITE_ONLY_ATTRIBUTE,
                RemoteApiResponseType.INVALID_VALUE,
            ):
                raise ValueError

            # Check if Feature Lock Key set
            lock_key_set = resp[4]["type"] == RemoteApiResponseType.WRITE_ONLY_ATTRIBUTE

            if lock_bits_set and lock_key_set:
                lock_status = node_db.OtapLockStatus.LOCKED
            elif not lock_bits_set and lock_key_set:
                lock_status = node_db.OtapLockStatus.UNLOCKED_KEY_SET
            elif lock_bits_set and not lock_key_set:
                lock_status = node_db.OtapLockStatus.UNLOCKED_BITS_SET
            else:
                lock_status = node_db.OtapLockStatus.UNLOCKED

            info["lock_status"] = lock_status
        except ValueError:
            self.print_verbose("invalid remote api info response packet format")
            return None

        return info

    def _send_lock_unlock_req(self, recv_packet, lock_status, new_lock, scr_update):
        gw_id = recv_packet.header.gw_id
        sink_id = recv_packet.header.sink_id

        node_addr = recv_packet.source_address

        req_ts = int(time.time())

        if lock_status in (
            node_db.OtapLockStatus.LOCKED,
            node_db.OtapLockStatus.UNLOCKED_KEY_SET,
        ):
            begin_req = self.req_fragments["begin"]
        else:
            # Feature Lock Key not set, must use plain Begin request
            begin_req = "01 00"

        if scr_update:
            # MSAP Scratchpad Update
            update_req = "1A 01 %02X" % self.update_seq
        else:
            update_req = ""

        # Ping with timestamp + Begin + Set Feature Lock Bits +
        # Set Feature Lock Key + End + Update
        payload = (
            "00 04"
            + _timestamp_as_hex(req_ts)
            + begin_req
            + (new_lock and "0D 06 16 00 FF FF FF 7F" or "0D 06 16 00 FF FF FF FF")
            + "0D 12 17 00"
            + (new_lock and self.keys["new_key"] or self.keys["no_key"])
            + update_req
            + self.req_fragments["end_and_update"]
        )
        self.send_remote_api_request(payload, gw_id, sink_id, node_addr)
        return req_ts

    def _parse_lock_unlock_resp(self, node_info, recv_packet):
        resp = self._parse_common_resp(node_info, recv_packet, None)
        if resp is None:
            # Not a valid Remote API response packet
            return None

        if False:  # DEBUG
            self.print_msg(repr(resp))

        try:
            if len(resp) not in (6, 7):
                raise ValueError

            if (
                resp[1]["type"]
                not in (
                    RemoteApiResponseType.BEGIN,
                    RemoteApiResponseType.BEGIN_WITH_LOCK,
                )
                or resp[2]["type"] != RemoteApiResponseType.WRITE_CSAP_ATTRIBUTE
                or resp[3]["type"] != RemoteApiResponseType.WRITE_CSAP_ATTRIBUTE
            ):
                raise ValueError

            # Optional MSAP Scratchpad Update response
            n = 4
            if resp[n]["type"] == RemoteApiResponseType.MSAP_SCRATCHPAD_UPDATE:
                n += 1

            if (
                resp[n]["type"] != RemoteApiResponseType.END
                or resp[n + 1]["type"] != RemoteApiResponseType.UPDATE
            ):
                raise ValueError
        except ValueError:
            # Not a valid lock / unlock response
            self.print_verbose(
                "invalid remote api lock / unlock response packet format"
            )
            return None

        # Check if Feature Lock Bits set
        lock_bits_set = resp[2]["value"] & 0x80000000 == 0

        # Valid lock / unlock response
        return lock_bits_set


def _timestamp_as_hex(ts):
    return " ".join(["%02X" % b for b in struct.pack("<L", ts)])


def _version_as_uint32_le(major, minor, maint, devel):
    """Convert a major.minor.maintenance.development version number to a little-endian 32-bit integer"""

    if (
        (major < 0 or major > 255)
        or (minor < 0 or minor > 255)
        or (maint < 0 or maint > 255)
        or (devel < 0 or devel > 255)
    ):
        raise ValueError("each version number component must be 0..255")

    # For the most efficient use of an SQLite3 database,
    # convert version number to a little-endian 32-bit integer
    return (major << 24) | (minor << 16) | (maint << 8) | devel


def _read_allowed_seqs(filename):
    """Read a list of allowed sequence numbers"""

    seqs = []

    with open(filename, "r") as file_:
        for line in file_:
            # Remove comments, commas, whitespace
            line = line.replace(";", "#")
            line = line[: line.find("#")]
            line = line.replace(",", " ").strip()

            if not line:
                continue

            seqs.extend([int(s) for s in line.split()])

    return seqs
