# OTAP Remote API Tool

## Introduction

OTAP Remote API Tool can help diagnose and fix OTAP issues with Wirepas Mesh
v4.x networks.


## Prerequisites

Installing _otap_remote_api_tool.py_ on a computer requires the following:

- A Linux or macOS computer
- Python v3.9 or newer, and online access to [PyPI](https://pypi.org/) (for
  [paho-mqtt](https://pypi.org/project/paho-mqtt/) and
  [protobuf](https://pypi.org/project/protobuf/) packages)
  * May work with Python v3.7 and v3.8, but not tested
- Git, and online access to Github
  ([wirepas/backend-apis](https://github.com/wirepas/backend-apis) repository)
- Google
  [Protocol Buffers compiler](https://github.com/protocolbuffers/protobuf#protobuf-runtime-installation)
  (_protoc_)
- Optionally, the _sqlite3_ command line tool
- Solid understanding on Wirepas
  [Remote API](https://developer.wirepas.com/support/solutions/articles/77000407101-wirepas-massive-remote-api-reference-manual)

(In the instructions below, the initial `$` character denotes your shell
prompt. Don't type that.)


### Cloning The Repository

First, run:

```
$ git clone https://github.com/wirepas/otap-remote-api-tool.git
$ cd otap-remote-api-tool/
```


### Installing Required Packages

On a Debian-based system (Ubuntu, Mint, …), run:

```
$ sudo apt install protobuf-compiler python3-venv sqlite3
```

On a Mac, install [Homebrew](https://brew.sh/) and run:

```
$ brew install protobuf
```

Macs come with _sqlite3_ and Python virtual environment support already
installed.


### Installing Python Packages in a Virtual Environment

It is a good idea to use a Python virtual environment for the required packages.
To create a virtual environment, make sure the _python3-venv_ package is
installed, as instructed above.

Then, create a virtual environment in the _otap-remote-api-tool/_ directory:

```
$ python -m venv venv
```

The virtual environment needs to be activated every time a new shell is opened:

```
$ . /venv/bin/activate
```

(Please note the leading dot and space.)


Once you have the virtual environment activated, run:

```
$ pip3 install wheel
$ pip3 install paho-mqtt protobuf
```


### Building Protocol Buffers Python Files

Run the following command in the _otap-remote-api-tool/_ directory to fetch the
Wirepas Messaging _.proto_ files and convert them to Python:

```
$ ./gen_proto_py.sh
```

You need to be online to access Github. The shell script will clone the
_wirepas/backend-apis.git_ repository and runs the Protocol Buffers Compiler to
create several Python files in a _proto-py/_ directory.


## Configuration

After all the prerequisites have been installed and before
_otap_remote_api_tool.py_ can be used, some details about the MQTT connection
and the network must be configured.

Connection settings and gateway details are read from an
[INI file](https://en.wikipedia.org/wiki/INI_file), which is then given as a
command line argument to _otap_remote_api_tool.py_.

An example INI file:

```
# Example Company MQTT broker and network configuration

[mqtt]
host     = mqtt-server.example.com
port     = 8883
username = mqttmasteruser
password = ExamplePasswordHere!
use_tls  = yes


# Example Feature Lock Key: all zeros
[remote_api:example]
feature_lock_key = 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00

# No Feature Lock Key
[remote_api:unlocked]
feature_lock_key =


# Gateways

[gateway:EXAMPLE-00001]
sinks = sink0,

[gateway:EXAMPLE-00002]
sinks = sink0,
```

### The “mqtt” Section

Connection details are set in the `mqtt` section of the INI file. Please note,
that the password in in clear text, so the INI file should not be left on any
unsupervised computer. Port is usually 8883 for TLS-protected connections and
1883 otherwise. The `use_tls` parameter (`yes` / `no`) selects, whether TLS
connections are in use, typically yes.


### The “remote_api:” Sections

Feature Lock Keys are not directly set on the command line of
_otap_remote_api_tool.py_. Instead, they are read from the INI file and can be
given descriptive names. Leaving the `feature_lock_key` parameter empty causes
the key to be unset, which is handled in various ways in
_otap_remote_api_tool.py_.


### The “gateway:” Sections

Gateways and their sinks are listed in the `gateway:` sections. For most uses
cases, there must be at least one gateway, with at least one sink listed. Name
of the gateway (the part after the colon `:`) must match the configured name of
the gateway. Multiple sinks can be listed in a comma-separated list. Usually the
first sink is called `sink0`.

To collect a list of available gateways and sinks, you can use the
`--any_source` command line argument combined with the `none` request, to see
incoming data packets from all gateways and sinks. The INI file can then be
populated with the correct gateways and sinks.


## Running _otap_remote_api_tool.py_

_otap_remote_api_tool.py_ is a command line program. It must be run from a
shell. Usually it is run inside a
[script](https://en.wikipedia.org/wiki/Script_(Unix)) session, or the output is
piped to a file using the [tee](https://en.wikipedia.org/wiki/Tee_(command))
command. That way, the output can be recorded and analyzed. For example, node
addresses can be collected in a text file for further commands to use.


### Command Line Arguments

_otap_remote_api_tool.py_ supports a number of command line arguments:

```
$ ./otap_remote_api_tool.py --help
usage: otap_remote_api_tool.py [-h] [-f file] [-b] [-k key] [-l key] [-p num]
                               [-i sec] [-d sec] [-u sec] [-s seq] [-a] [-v]
                               connection_ini request

A tool to send OTAP-related Remote API requests via MQTT

positional arguments:
  connection_ini        connection details as INI file
  request               request to perform

optional arguments:
  -h, --help            show this help message and exit
  -f file, --node_list file
                        file with a list of node addresses (mutually exclusive
                        to --broadcast)
  -b, --broadcast       broadcast request to all nodes (mutually exclusive to
                        --node_list)
  -k key, --feature_lock_key key
                        use feature lock key for request (default: do not use
                        feature lock key)
  -l key, --new_feature_lock_key key
                        feature lock key to set for requests set_bits_set_lock
                        and clear_bits_set_lock (default: clear feature lock
                        key)
  -p num, --batch_size num
                        batch size of parallel requests (when --broadcast not
                        used, default 10)
  -i sec, --initial_delay sec
                        initial request delay in seconds (default 5)
  -d sec, --repeat_delay sec
                        delay between repeated requests in seconds (no repeat
                        by default)
  -u sec, --update_delay sec
                        update delay in seconds for update_scratchpad request
                        (default: 10)
  -s seq, --sequence seq
                        sequence number for update_scratchpad request
  -a, --any_source      show received packets from all gateways and sinks,
                        even those not listed in connection_ini file
  -v, --verbose         verbosity, can use up to two

requests: none, ping, cancel, request_info, clear_bits, clear_bits_clear_lock,
set_bits_set_lock, clear_bits_set_lock, update_scratchpad, reset_node, agent
```

TODO: Add command line argument details


### Requests

The following requests are supported:

| Command | Purpose |
|---------|---------|
| none | No action, just display incoming data |
| ping | Send a Remote API Ping request to the chosen nodes |
| cancel | Send a Remote API Cancel request, to reset the Remote API state machine to a known state and stop any running update countdown |
| request_info | Ask nodes to send scratchpad status, current node role, feature lock bits and feature lock key |
| clear_bits | Clear feature lock bits to all 1’s (active low) on chosen nodes |
| clear_bits_clear_lock | Clear feature lock bits (all 1’s) and feature lock key on chosen nodes |
| set_bits_set_lock | Set feature lock bits to disable OTAP and set the feature lock key to the given value, or no key if not given |
| clear_bits_set_lock | Clear feature lock bits (all 1’s) set the feature lock key to the given value, or no key if not given |
| update_scratchpad | Send a Remote API OTAP update request, starting an update countdown with the given timeout |
| reset_node | Reset nodes |
| agent | Run an external agent to handle more complicated use cases |

TODO: Add request details
