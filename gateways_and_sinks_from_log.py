#!/usr/bin/env python3

import sys
import re

PATTERN = r'^data packet from gateway "([^"]*)" sink "([^"]*)"$'

infile = open(sys.argv[1], "r")

gateways = {}

for line in infile:
    match = re.match(PATTERN, line.strip())
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
