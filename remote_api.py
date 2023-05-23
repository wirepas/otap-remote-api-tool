#!/usr/bin/env python3

# remote_api.py - Remote API response parser

import struct
from enum import Enum


class RemoteApiResponseType(Enum):
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


def parse_response(payload):
    """Parse a Remote API response to a list of dicts"""

    records = []

    while len(payload) > 0:
        if len(payload) < 2:
            raise ValueError("truncated tlv record")

        try:
            tlv_type = RemoteApiResponseType(payload[0])
        except ValueError:
            raise ValueError("unknown tlv record type: 0x{payload[0]:02x}")

        tlv_len = payload[1]
        tlv_payload = payload[2 : (2 + tlv_len)]

        if len(tlv_payload) != tlv_len:
            raise ValueError("truncated tlv record")

        resp = {"type": tlv_type}

        if tlv_type == RemoteApiResponseType.PING and tlv_len <= 16:
            # Ping response
            resp["payload"] = tlv_payload
        elif (
            tlv_type
            in (
                RemoteApiResponseType.BEGIN,
                RemoteApiResponseType.BEGIN_WITH_LOCK,
                RemoteApiResponseType.END,
                RemoteApiResponseType.CANCEL,
            )
            and tlv_len == 0
        ):
            # Begin, Begin with Lock, End or Cancel response (no payload)
            pass
        elif tlv_type == RemoteApiResponseType.UPDATE and tlv_len == 2:
            # Update response
            (update_time,) = struct.unpack("<H", tlv_payload)
            resp["update_time"] = update_time
        elif tlv_type == RemoteApiResponseType.MSAP_SCRATCHPAD_STATUS and tlv_len >= 24:
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

            scr_status = {
                "type": RemoteApiResponseType.MSAP_SCRATCHPAD_STATUS,
                "st_len": st_len,
                "st_crc": st_crc,
                "st_seq": st_seq,
                "st_type": st_type,
                "st_sta": st_sta,
                "fw_len": fw_len,
                "fw_crc": fw_crc,
                "fw_seq": fw_seq,
                "fw_id": fw_id,
                "fw_ver": (fw_maj, fw_min, fw_mnt, fw_dev),
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

                scr_status.update(
                    {
                        "app_len": app_len,
                        "app_crc": app_crc,
                        "app_seq": app_seq,
                        "app_id": app_id,
                        "app_ver": (app_maj, app_min, app_mnt, app_dev),
                    }
                )

                resp.update(scr_status)
        elif tlv_type == RemoteApiResponseType.MSAP_SCRATCHPAD_UPDATE and tlv_len == 1:
            # MSAP Scratchpad Update response
            (st_seq,) = struct.unpack("<B", tlv_payload)
            resp["st_seq"] = st_seq
        elif tlv_type in (
            RemoteApiResponseType.WRITE_MSAP_ATTRIBUTE,
            RemoteApiResponseType.READ_MSAP_ATTRIBUTE,
            RemoteApiResponseType.WRITE_CSAP_ATTRIBUTE,
            RemoteApiResponseType.READ_CSAP_ATTRIBUTE,
        ):
            # Write or Read MSAP or CSAP Attribute response
            (attribute,) = struct.unpack("<H", tlv_payload[:2])
            resp["attribute"] = attribute

            value = tlv_payload[2:]

            # Convert 1- to 4-byte attributes to integer
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

            resp["value"] = value
        elif tlv_type in (
            RemoteApiResponseType.ACCESS_DENIED,
            RemoteApiResponseType.WRITE_ONLY_ATTRIBUTE,
            RemoteApiResponseType.INVALID_BROADCAST_REQUEST,
            RemoteApiResponseType.INVALID_BEGIN,
            RemoteApiResponseType.NO_SPACE_FOR_RESPONSE,
            RemoteApiResponseType.INVALID_VALUE,
            RemoteApiResponseType.INVALID_LENGTH,
            RemoteApiResponseType.UNKNOWN_REQUEST,
        ) and tlv_len in (1, 3):
            # Error response
            request = tlv_payload[0]
            resp["request"] = request

            if tlv_len == 3:
                (attribute,) = struct.unpack("<H", tlv_payload[1:])
                resp["attribute"] = attribute
        else:
            raise ValueError("invalid tlv type or length")

        records.append(resp)
        payload = payload[(tlv_len + 2) :]

    return records
