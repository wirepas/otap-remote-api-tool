#!/usr/bin/env python3

# agent_smart_enable.py - An intelligent agent to handle sending Remote API
#                         requests and parsing responses, to enable OTAP on
#                         large networks in a controlled manner

import time
import struct
from enum import Enum

import node_db


class RemoteApiResponse(Enum):
    """Remote API response types"""

    PING = 0x80
    BEGIN = 0x81
    BEGIN_WITH_LOCK = 0x82
    END = 0x83
    CANCEL = 0x84
    UPDATE = 0x85
    WRITE_MSAP_ATTRIBUTE = 0x8B
    READ_MSAP_ATTRIBUTE = 0x8C
    WRITE_CSAP_ATTRIBUTE = 0x8D
    READ_CSAP_ATTRIBUTE = 0x8E
    MSAP_SCRATCHPAD_STATUS = 0x99
    MSAP_SCRATCHPAD_UPDATE = 0x9A
    ACCESS_DENIED = 0xF8
    WRITE_ONLY_ATTRIBUTE = 0xF9
    INVALID_BROADCAST_REQUEST = 0xFA
    INVALID_BEGIN = 0xFB
    NO_SPACE_FOR_RESPONSE = 0xFC
    INVALID_VALUE = 0xFD
    INVALID_LENGTH = 0xFE
    UNKNOWN_REQUEST = 0xFF


class Agent:
    """An intelligent agent to handle Remote API requests and responses"""

    # Request timeout, seconds
    REQUEST_TIMEOUT = 5 * 60  # Five minutes

    def __init__(
        self, param, functions, connection, repeat_delay, node_list, req_fragments, keys
    ):
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
        self.node_list = node_list
        self.req_fragments = req_fragments
        self.keys = keys

        params = param.split(":")
        if len(params) != 2:
            raise ValueError("invalid number of parameters")

        self.db = node_db.NodeDb(params[0])

        self.allowed_seqs = _read_allowed_seqs(params[1])
        self.request_repeat_interval = repeat_delay

    def periodic(self):
        """Periodic handler function, called every second or so"""

        if False:  # DEBUG
            self.print_msg("agent: periodic()")

        # TODO: Periodic maintenance functions

    def on_message(self, recv_packet):
        """Message reception handler function, called on every received packet"""

        # DEBUG: Print received packet
        if False:
            self.print_msg("agent: on_message()")
            self.print_msg(repr(recv_packet))

        source_address = recv_packet.source_address

        rx_time = recv_packet.rx_time_ms_epoch / 1000.0
        last_seen = rx_time  # Update timestamp, unless newer packets already seen
        phase = node_db.Phase.INIT
        last_req = None
        last_info = None
        seq_old = None

        # See if the node is known already
        node_info = self.db.find_node(source_address)
        if node_info:
            last_info = node_info["last_info"]
            seq_old = node_info["st_seq"]

            last_seen_old = node_info["last_seen"]
            if last_seen_old is not None and last_seen_old >= last_seen:
                # Newer packets have already been seen, do not touch timestamp
                last_seen = None

            # Get current phase, default to "INIT"
            if node_info["phase"] is not None:
                phase = node_db.Phase(node_info["phase"])

            now = time.time()

            # Handle request timeouts
            last_req = node_info["last_req"]
            if phase != node_db.Phase.DONE and (
                last_req is None or (now - last_req) >= self.REQUEST_TIMEOUT
            ):
                phase = node_db.Phase.INIT
                self.print_info(f"node {source_address} request timeout")

            if self.request_repeat_interval:
                # Request info periodically
                last_info = node_info["last_info"]
                if (
                    phase == node_db.Phase.DONE
                    and last_info is not None  # Shouldn't happen with Phase.DONE
                    and (now - last_info) >= self.request_repeat_interval
                ):
                    phase = node_db.Phase.INIT
                    self.print_info(f"node {source_address} periodic request")
        else:
            self.print_info(f"new node {source_address} seen")

        # Run state machine
        phase, updates = self._state_machine(phase, node_info, recv_packet)

        seq = updates.get("st_seq", None)
        if seq_old is not None and seq is not None:
            if seq_old != seq:
                self.print_msg(
                    f"otap seq number changed from {seq_old} to {seq} on node {source_address}"
                )

        last_info_new = updates.get("last_info", None)
        if last_info is None or last_info_new is None or last_info_new > last_info:
            # Update node information, if it is newer
            self.db.open_transaction()
            self.db.add_or_update_node(
                recv_packet.source_address,
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

    def generate_remote_api_request(self, gw_id, sink_id, dest_address):
        """Send handler function, called when it is time to send a request"""

        if False:  # DEBUG
            self.print_msg("agent: send_remote_api_request()")

        # TODO: Send Remote API requests from node list

    def _state_machine(self, phase, node_info, recv_packet):
        """State machine for locking / unlocking nodes"""

        updates = {}

        source_address = recv_packet.source_address
        source_endpoint = recv_packet.source_endpoint
        dest_endpoint = recv_packet.destination_endpoint
        rx_time = recv_packet.rx_time_ms_epoch / 1000.0

        if phase == node_db.Phase.INIT:
            self.print_info(f"sending info request to {source_address}")

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

            # Check stored sequence number and lock / unlock
            lock = info["st_seq"] not in self.allowed_seqs

            if lock and (info["lock_status"] == node_db.OtapLockStatus.LOCKED):
                self.print_info(f"node {source_address} already locked, nothing to do")
                return node_db.Phase.DONE, updates
            elif not lock and (info["lock_status"] == node_db.OtapLockStatus.UNLOCKED):
                self.print_info(
                    f"node {source_address} already unlocked, nothing to do"
                )
                return node_db.Phase.DONE, updates

            self.print_info(
                f"got info response from {source_address}, sending {lock and 'lock' or 'unlock'} request"
            )

            # Info response OK, send lock or unlock request
            last_req = self._send_lock_unlock_req(recv_packet, lock)
            updates["last_req"] = last_req

            # Advance to the next phase and wait for response
            return node_db.Phase.LOCK_UNLOCK_REQ, updates

        if phase == node_db.Phase.LOCK_UNLOCK_REQ:
            # Lock / unlock response received
            if self._parse_lock_unlock_resp(node_info, recv_packet):
                self.print_info(
                    f"got lock / unlock response from {source_address}, configuration done"
                )
                updates["last_resp"] = rx_time
                return node_db.Phase.DONE, updates

        # Something else, do nothing
        return phase, updates

    def _parse_common_resp(self, node_info, recv_packet, resp_len=None):
        try:
            resp = _parse_remote_api_response(recv_packet.payload)
        except ValueError as exc:
            # Invalid response, ignore it
            self.print_info(exc)
            return None

        if False:  # DEBUG
            self.print_msg(repr(resp))

        if not resp or (resp_len is not None and len(resp) != resp_len):
            # Not a valid Remote API response packet
            self.print_verbose("invalid remote api response packet")
            return None

        # Check first response type and payload length
        if resp[0]["type"] != RemoteApiResponse.PING or len(resp[0]["payload"]) != 4:
            # No PING response as the first response in the packet
            self.print_verbose("missing ping payload in remote api response packet")
            return None

        # Check request timestamp
        (req_time,) = struct.unpack("<L", resp[0]["payload"])
        if req_time != node_info["last_req"]:
            # Not a response to the last request
            self.print_verbose("remote api response packet timestamp not latest")
            self.print_verbose(f"{repr(req_time)}, {repr(node_info['last_req'])}")
            return None

        # Request timestamp is OK
        return resp

    def _send_info_req(self, recv_packet):
        gw_id = recv_packet.header.gw_id
        sink_id = recv_packet.header.sink_id

        source_address = recv_packet.source_address

        req_ts = int(time.time())

        # Ping with timestamp + read Scratchpad Status + read node role +
        # read Feature Lock Bits + read Feature Lock Key
        payload = (
            "00 04"
            + _timestamp_as_hex(req_ts)
            + "19 00"
            + "0E 02 04 00"
            + "0E 02 16 00"
            + "0E 02 17 00"
        )

        self.send_remote_api_request(payload, gw_id, sink_id, source_address)
        return req_ts

    def _parse_info_resp(self, node_info, recv_packet):
        resp = self._parse_common_resp(node_info, recv_packet, 5)
        if not resp:
            return resp

        if resp[1]["type"] != RemoteApiResponse.MSAP_SCRATCHPAD_STATUS:
            self.print_verbose("invalid remote api info response packet format")
            return None

        # Check stored scratchpad sequence number and other info
        info = {}
        info.update(resp[1])

        if resp[2]["type"] != RemoteApiResponse.READ_CSAP_ATTRIBUTE:
            self.print_verbose("invalid remote api info response packet format")
            return None

        info["node_role"] = resp[2]["value"]

        if resp[3]["type"] != RemoteApiResponse.READ_CSAP_ATTRIBUTE:
            self.print_verbose("invalid remote api info response packet format")
            return None

        # Check if Feature Lock Bits set
        lock_bits_set = resp[3]["value"] & 0x80000000 == 0

        if resp[4]["type"] not in (
            RemoteApiResponse.WRITE_ONLY_ATTRIBUTE,
            RemoteApiResponse.INVALID_VALUE,
        ):
            self.print_verbose("invalid remote api info response packet format")
            return None

        # Check if Feature Lock Key set
        lock_key_set = resp[4]["type"] == RemoteApiResponse.WRITE_ONLY_ATTRIBUTE

        if lock_bits_set and lock_key_set:
            lock_status = node_db.OtapLockStatus.LOCKED
        elif not lock_bits_set and lock_key_set:
            lock_status = node_db.OtapLockStatus.UNLOCKED_KEY_SET
        elif lock_bits_set and not lock_key_set:
            lock_status = node_db.OtapLockStatus.UNLOCKED_BITS_SET
        else:
            lock_status = node_db.OtapLockStatus.UNLOCKED

        info["lock_status"] = lock_status

        return info

    def _send_lock_unlock_req(self, recv_packet, lock):
        gw_id = recv_packet.header.gw_id
        sink_id = recv_packet.header.sink_id

        source_address = recv_packet.source_address

        req_ts = int(time.time())

        # Ping with timestamp + Begin + Set Feature Lock Bits +
        # Set Feature Lock Key + End + Update
        payload = (
            "00 04"
            + _timestamp_as_hex(req_ts)
            + self.req_fragments["begin"]
            + (lock and "0D 06 16 00 FF FF FF 7F" or "0D 06 16 00 FF FF FF FF")
            + "0D 12 17 00"
            + (lock and self.keys["new_key"] or self.keys["no_key"])
            + self.req_fragments["end_and_update"]
        )
        self.send_remote_api_request(payload, gw_id, sink_id, source_address)
        return req_ts

    def _parse_lock_unlock_resp(self, node_info, recv_packet):
        resp = self._parse_common_resp(node_info, recv_packet, 6)
        if not resp:
            return False

        if False:  # DEBUG
            self.print_msg(repr(resp))

        if (
            resp[1]["type"]
            not in (RemoteApiResponse.BEGIN, RemoteApiResponse.BEGIN_WITH_LOCK)
            or resp[2]["type"] != RemoteApiResponse.WRITE_CSAP_ATTRIBUTE
            or resp[3]["type"] != RemoteApiResponse.WRITE_CSAP_ATTRIBUTE
            or resp[4]["type"] != RemoteApiResponse.END
            or resp[5]["type"] != RemoteApiResponse.UPDATE
        ):
            # Not a valid lock / unlock response
            self.print_verbose(
                "invalid remote api lock / unlock response packet format"
            )
            return False

        # Valid lock / unlock response
        return True


def _parse_remote_api_response(payload):
    """Parse a Remote API response to a list of dicts"""

    records = []

    while len(payload) > 0:
        if len(payload) < 2:
            raise ValueError("truncated tlv record")

        tlv_type = payload[0]
        tlv_len = payload[1]
        tlv_payload = payload[2 : (2 + tlv_len)]

        if len(tlv_payload) != tlv_len:
            raise ValueError("truncated tlv record")

        resp = None

        if tlv_type == RemoteApiResponse.PING.value and tlv_len <= 16:
            # Ping response
            resp = {"type": RemoteApiResponse.PING, "payload": tlv_payload}
        elif tlv_type in (
            RemoteApiResponse.BEGIN.value,
            RemoteApiResponse.BEGIN_WITH_LOCK.value,
            RemoteApiResponse.END.value,
        ):
            # Begin, Begin_witk_lock or End response
            resp = {"type": RemoteApiResponse(tlv_type)}
        elif tlv_type == RemoteApiResponse.UPDATE.value and tlv_len == 2:
            # Update response
            update_time = struct.unpack("<H", tlv_payload)
            resp = {"type": RemoteApiResponse.UPDATE, "update_time": update_time}
        elif (
            tlv_type == RemoteApiResponse.MSAP_SCRATCHPAD_STATUS.value and tlv_len >= 24
        ):
            # MSAP Scratchpad Status response
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

            resp = {
                "type": RemoteApiResponse.MSAP_SCRATCHPAD_STATUS,
                "st_len": st_len,
                "st_crc": st_crc,
                "st_seq": st_seq,
                "st_type": st_type,
                "st_sta": st_sta,
                "fw_len": fw_len,
                "fw_crc": fw_crc,
                "fw_seq": fw_seq,
                "fw_id": fw_id,
                "fw_maj": fw_maj,
                "fw_min": fw_min,
                "fw_mnt": fw_mnt,
                "fw_dev": fw_dev,
            }

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

            resp.update(
                {
                    "app_len": app_len,
                    "app_crc": app_crc,
                    "app_seq": app_seq,
                    "app_id": app_id,
                    "app_maj": app_maj,
                    "app_min": app_min,
                    "app_mnt": app_mnt,
                    "app_dev": app_dev,
                }
            )
        elif tlv_type in (
            RemoteApiResponse.WRITE_CSAP_ATTRIBUTE.value,
            RemoteApiResponse.READ_CSAP_ATTRIBUTE.value,
        ):
            # Write or Read CSAP Attribute response
            (attribute,) = struct.unpack("<H", tlv_payload[:2])

            value = tlv_payload[2:]

            if len(value) == 1:
                value = value[0]
            elif len(value) == 2:
                (value,) = struct.unpack("<H", value)
            elif len(value) == 3:
                value, value_msb = struct.unpack("<HB", value)
                value |= value_msb << 16
            elif len(value) == 4:
                (value,) = struct.unpack("<L", value)
            else:
                # Leave as bytes
                pass

            resp = {
                "type": RemoteApiResponse(tlv_type),
                "attribute": attribute,
                "value": value,
            }
        elif tlv_type in (
            RemoteApiResponse.ACCESS_DENIED.value,
            RemoteApiResponse.WRITE_ONLY_ATTRIBUTE.value,
            RemoteApiResponse.INVALID_BROADCAST_REQUEST.value,
            RemoteApiResponse.INVALID_BEGIN.value,
            RemoteApiResponse.NO_SPACE_FOR_RESPONSE.value,
            RemoteApiResponse.INVALID_VALUE.value,
            RemoteApiResponse.INVALID_LENGTH.value,
            RemoteApiResponse.UNKNOWN_REQUEST.value,
        ):
            request = tlv_payload[0]

            resp = {"type": RemoteApiResponse(tlv_type), "request": request}

            if tlv_len >= 3:
                attribute = struct.unpack("<H", tlv_payload[1:])
                resp.update({"attribute": attribute})
        else:
            raise ValueError("invalid tlv type or length")

        records.append(resp)
        payload = payload[(tlv_len + 2) :]

    return records


def _timestamp_as_hex(ts):
    return " ".join(["%02X" % b for b in struct.pack("<L", ts)])


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
