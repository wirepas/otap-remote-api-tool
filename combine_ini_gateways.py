#!/usr/bin/env python3

import sys
import os
import configparser


pgmname = os.path.split(sys.argv[0])[-1]

if len(sys.argv) < 3:
    sys.stderr.write(f"Usage: {pgmname} output_file input_file...\n")
    sys.exit(2)

output_filename = sys.argv[1]

gateways = {}

for filename in sys.argv[2:]:
    ini = configparser.ConfigParser()
    with open(filename, "r") as f:
        ini.read_file(f)

    for section in ini.sections():
        if section.startswith("gateway:"):
            gateway = section.split(":", 1)[1]

            if ini.has_option(section, "sinks"):
                sinks = set(ini.get(section, "sinks").replace(",", " ").split())
            else:
                sinks = set()

            if gateway in gateways:
                sinks |= gateways[gateway]

            gateways[gateway] = sinks

gateways_sorted = list(gateways.keys())
gateways_sorted.sort()

with open(output_filename, "w") as f:
    for gateway in gateways_sorted:
        f.write(f"[gateway:{gateway}]\n")
        sinks = list(gateways[gateway])
        if len(sinks) > 0:
            sinks.sort()
            f.write("sinks: " + ",".join(sinks) + ",\n")
        f.write("\n")
