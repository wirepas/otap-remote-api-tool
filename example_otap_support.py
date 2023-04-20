#!/usr/bin/env python3

import sys
import time


try:
    from wirepas_mqtt_library import WirepasNetworkInterface
except ModuleNotFoundError:
    print(
        'module "wirepas_mqtt_library" missing, install with "python -m pip install wirepas-mqtt-library"'
    )
    sys.exit(1)


def hexdump(bytes):
    return " ".join(["%02X" % n for n in bytes])


def to_bytearray(s):
    return bytearray([int(n, 16) for n in s.split()])


# Connect to an MQTT broker
wni = WirepasNetworkInterface(
    "example.prod-wirepas.com",
    8883,
    "mqttmasteruser",
    "ExamplePasswordHere!",
    strict_mode=False,
)

if False:
    # Clear stale or invalid gateway status
    wni.clear_gateway_status("EXAMPLE-00001")
    wni.clear_gateway_status("EXAMPLE-00002")

    sys.exit(0)

if True:
    # Print gateways
    gws = wni.get_gateways()
    print(repr(gws))

    if False: # DEBUG
        sys.exit(0)
else:
    # Gateways:
    #
    # "EXAMPLE-00001"
    # "EXAMPLE-00002"

    # Select specific gateways
    gws = ["EXAMPLE-00001"]

if True:
    for gw_id in gws:
        # Print sinks connected to a gateway
        print("gateway_id: %s" % gw_id)
        sinks = wni.get_sinks(gateway=gw_id)
        for sink in sinks:
            sink_info = sink[2]
            sink_info["app_config_data"] = hexdump(sink_info["app_config_data"])
            print(
                "  sink_id: {sink_id}\n"
                "  node_address: {node_address}\n"
                "  network_address: 0x{network_address:06x}\n"
                "  network_channel: {network_channel}\n"
                "  app_config_seq: {app_config_seq}\n"
                "  app_config_diag: {app_config_diag}\n"
                "  app_config_data: {app_config_data}\n".format(**sink_info)
            )

    sys.exit(0)

# Use the first gateway
gw_id = gws[0]

# Use the first sink on the gateway
sink_id = "sink0"

# Get sink information
sinks = wni.get_sinks(gateway=gw_id)
for sink in sinks:
    if sink[1] == sink_id:
        sink = sink[2]
        break
else:
    print('sink "%s" not found on gateway "%s"' % (sink_id, gw_id))
    sys.exit(1)

if False:
    # Set app config data
    app_config_data = "00 00 00 00 00 00 00 00"

    # Increment app config data sequence number
    app_config_seq = sink["app_config_seq"] + 1
    if app_config_seq >= 255:
        app_config_seq = 0

    # Keep diagnostic interval as-is
    app_config_diag = sink["app_config_diag"]

    print(
        "app_config_seq: {app_config_seq}\n"
        "app_config_diag: {app_config_diag}\n"
        "app_config_data: {app_config_data}".format(**locals())
    )

    print(
        wni.set_sink_config(
            gw_id,
            sink["sink_id"],
            {
                "app_config_seq": app_config_seq,
                "app_config_diag": app_config_diag,
                "app_config_data": to_bytearray(app_config_data),
            },
        )
    )

    sys.exit(1)

if False:
    # Get sink scratchpad status
    scr_info = wni.get_scratchpad_status(gw_id, sink_id)[1]

    scr_seq = scr_info["stored_scratchpad"]["seq"]
    scr_crc = scr_info["stored_scratchpad"]["crc"]
    scr_len = scr_info["stored_scratchpad"]["len"]

    print(
        "scratchpad seq: {scr_seq}\n"
        "scratchpad crc: 0x{scr_crc:04X}\n"
        "scratchpad length: {scr_len}".format(**locals())
    )

    if False:
        # Increment scratchpad sequence number
        scr_seq += 1
        if scr_seq >= 255:
            scr_seq = 1

        # Load scratchpad file
        with open("art00001_app1_wpc_stack.otap", "rb") as f:
            scr_data = f.read()

        print("uploading scratchpad...")

        # Upload new scratchpad to sink
        print(wni.upload_scratchpad(gw_id, sink_id, scr_seq, scr_data))

    sys.exit(1)


def on_data_rx(data):
    print(
        "received, nw: 0x%06x, from: %d, ep: %d:%d, time: %.3f s, %d bytes\n%s"
        % (
            data.network_address,
            data.source_address,
            data.source_endpoint,
            data.destination_endpoint,
            data.travel_time_ms / 1000.0,
            len(data.data_payload),
            hexdump(data.data_payload),
        )
    )


# Start receiving packets
print("waiting for data packets...")
wni.register_data_cb(on_data_rx)

if False:
    # Remote API Ping request
    data_to_send = "00 00"

    # Destination address: broadcast
    destination_address = 0xFFFFFFFF

    # Remote API endpoints
    source_endpoint = 255
    destination_endpoint = 240

    # Send Remote API request
    print("sending remote api request...")
    print(
        wni.send_message(
            gw_id,
            sink_id,
            destination_address,
            source_endpoint,
            destination_endpoint,
            bytes(to_bytearray(data_to_send)),
        )
    )

# Print received packets until interrupted
while True:
    time.sleep(1)
