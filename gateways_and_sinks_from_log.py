#!/usr/bin/env python3

import sys
import os
import re

PATTERNS = [
    r'^data packet from gateway "([^"]*)" sink "([^"]*)"$',
    r"^gw: ([-_.0-9A-Za-z]+), sink: ([-_.0-9A-Za-z]+), timed out$",
]

if len(sys.argv) == 2:
    infile = open(sys.argv[1], "r")
elif len(sys.argv) == 1:
    infile = sys.stdin
else:
    sys.stderr.write("Usage: %s [input_file]\n" % (os.path.split(sys.argv[0])[-1]))
    exit(1)

gateways = {}

for line in infile:
    line = line.strip()
    for patt in PATTERNS:
        match = re.match(patt, line)
        if match:
            break
    if not match:
        continue

    gateway = match[1]
    sink = match[2]

    if gateway not in gateways:
        gateways[gateway] = set()
    gateways[gateway].add(sink)

gateways_sorted = list(gateways.keys())
gateways_sorted.sort()

for gateway in gateways_sorted:
    print(f"[gateway:{gateway}]")
    sinks = list(gateways[gateway])
    if len(sinks) > 0:
        sinks.sort()
        print("sinks: " + ",".join(sinks) + ",")
    print()
