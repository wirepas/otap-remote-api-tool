#!/usr/bin/env python3

# connection_ini_parser.py - A parser for connection settings INI files

import configparser


def parse_feature_lock_key(key):
    """Parse and verify a Feature Lock Key"""

    key = key.strip()
    if key == "":
        return None

    try:
        key_bytes = [int(n, 16) for n in key.split()]
        if len(key_bytes) != 16:
            raise ValueError
    except ValueError:
        raise ValueError('invalid key: "%s"' % key)

    return " ".join(["%02X" % n for n in key_bytes])


def read_connection_ini(file_):
    """Read connection settings INI file"""

    class MqttSettings:
        def __init__(
            self, host=None, port=None, username=None, password=None, use_tls=None
        ):
            self.host = host
            self.port = port
            self.username = username
            self.password = password
            self.use_tls = use_tls

        @staticmethod
        def _fields():
            return ("host", "port", "username", "password", "use_tls")

    class RemoteApi:
        def __init__(self, feature_lock_key=None):
            self.feature_lock_key = feature_lock_key

        @staticmethod
        def _fields():
            return ("feature_lock_key",)

    class Gateway:
        def __init__(self, sinks=[]):
            self.sinks = sinks

        @staticmethod
        def _fields():
            return ("sinks",)

    class Connection:
        def __init__(self, mqtt_settings=MqttSettings(), remote_api={}, gateways={}):
            self.mqtt_settings = mqtt_settings
            self.remote_api = remote_api
            self.gateways = gateways

    connection = Connection()

    ini = configparser.ConfigParser()
    ini.read_file(file_)

    if not ini.has_section("mqtt"):
        raise ValueError('missing "mqtt" section')

    for section in ini.sections():
        if section == "mqtt":
            for option in MqttSettings._fields():
                if not ini.has_option("mqtt", option):
                    raise ValueError('missing option "%s" in section "mqtt"' % option)
                if option == "port":
                    value = ini.getint("mqtt", option)
                elif option == "use_tls":
                    value = ini.getboolean("mqtt", option)
                else:
                    raw = (option == "password") and True or False
                    value = ini.get("mqtt", option, raw=raw)
                setattr(connection.mqtt_settings, option, value)
        elif section.startswith("remote_api:"):
            remote_api = section.split(":", 1)[1]

            for option in RemoteApi._fields():
                if not ini.has_option(section, option):
                    raise ValueError(
                        'missing option "%s" in section "%s"' % (option, section)
                    )

            feature_lock_key = parse_feature_lock_key(
                ini.get(section, "feature_lock_key")
            )

            connection.remote_api[remote_api] = RemoteApi(feature_lock_key)
        elif section.startswith("gateway:"):
            gateway = section.split(":", 1)[1]

            for option in Gateway._fields():
                if not ini.has_option(section, option):
                    raise ValueError(
                        'missing option "%s" in section "%s"' % (option, section)
                    )

            sinks = ini.get(section, "sinks").replace(",", " ").split()

            connection.gateways[gateway] = Gateway(sinks)
        else:
            raise ValueError('invalid section "%s"' % section)

    return connection
