# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

""" Module for MQTT Sparkplug B Python async plugin """
import json
import asyncio
import copy
import logging
from datetime import datetime, timezone

import async_ingest
import paho.mqtt.client as mqtt
from fledge.common import logger

try:
    from fledge.plugins.south.mqtt_sparkplug.sparkplug_b import sparkplug_b_pb2
except:
    # FIXME: Import sparkplug_b_pb2 in a better way for unit tests
    pass

__author__ = (
    "Jon Scott (OSIsoft), "
    "Ashish Jabble (Dianomic)"
)

__copyright__ = (
    "Copyright (c) 2018 OSIsoft, LLC, "
    "Copyright (c) 2024 Dianomic Systems, Inc."
)

__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__, level=logging.INFO)

_PLUGIN_NAME = 'MQTT Sparkplug'
_DEFAULT_CONFIG = {
    'plugin': {
        'description': _PLUGIN_NAME,
        'type': 'string',
        'default': 'mqtt_sparkplug',
        'readonly': 'true'
    },
    'assetName': {
        'description': 'Name of Asset',
        'type': 'string',
        'default': 'mqtt',
        'order': '1',
        'displayName': 'Asset Name',
        'mandatory': 'true'
    },
    'url': {
        'description': 'Hostname for MQTT Server',
        'type': 'string',
        'default': 'chariot.groov.com',
        'order': '2',
        'displayName': 'MQTT Host'
    },
    'port': {
        'description': 'Port for MQTT Server',
        'type': 'string',
        'default': '1883',
        'order': '3',
        'displayName': 'MQTT Port'
    },
    'user': {
        'description': 'Username for MQTT Server',
        'type': 'string',
        'default': 'opto',
        'order': '4',
        'displayName': 'Username'
    },
    'password': {
        'description': 'Password for MQTT Server',
        'type': 'password',
        'default': 'opto22',
        'order': '5',
        'displayName': 'Password'
    },
    'topic': {
        'description': 'Name of Topic',
        'type': 'string',
        'default': 'spBv1.0/Opto22/DDATA/groovEPIC_workshop/Strategy',
        'order': '6',
        'displayName': 'Topic'
    },
}

c_callback = None
c_ingest_ref = None
loop = None


def plugin_info():
    """ Returns information about the plugin.
    Args:
    Returns:
        dict: plugin information
    Raises:
    """

    return {
        'name': _PLUGIN_NAME,
        'version': '2.6.0',
        'mode': 'async',
        'type': 'south',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(config):
    """ Initialise the plugin.
    Args:
        config: JSON configuration document for the South plugin configuration category
    Returns:
        data: JSON object to be used in future calls to the plugin
    Raises:
    """
    handle = copy.deepcopy(config)
    handle['_mqtt'] = MqttSubscriberClient(handle)
    return handle


def plugin_start(handle):
    """ Extracts data from the sinusoid and returns it in a JSON document as a Python dict.
    Available for async mode only.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        None - If no reading is available
    Raises:
        TimeoutError
    """
    global loop
    loop = asyncio.new_event_loop()
    # Create a connection to the MQTT server
    try:
        _mqtt = handle["_mqtt"]
        _mqtt.loop = loop
        _mqtt.start()
    except Exception as ex:
        _LOGGER.error("Error establishing connection to MQTT server: {}".format(str(ex)))


def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    """
    _LOGGER.info("Old config for {} {} \n new config {}".format(_PLUGIN_NAME, handle, new_config))

    # plugin_shutdown
    plugin_shutdown(handle)

    # plugin_init
    new_handle = plugin_init(new_config)

    # plugin_start
    plugin_start(new_handle)

    return new_handle


def plugin_shutdown(handle):
    """ Shutdowns the plugin doing required cleanup, to be called prior to the South plugin service being shut down.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        plugin shutdown
    """
    global loop
    try:
        _mqtt = handle["_mqtt"]
        _mqtt.stop()

        loop.stop()
        loop = None
        _mqtt = None
    except Exception as ex:
        _LOGGER.error("Error shutting down connection to MQTT server: {}".format(str(ex)))
    else:
        _LOGGER.info('{} plugin shut down.'.format(_PLUGIN_NAME))


def plugin_register_ingest(handle, callback, ingest_ref):
    """Required plugin interface component to communicate to South C server

    Args:
        handle: handle returned by the plugin initialisation call
        callback: C opaque object required to passed back to C->ingest method
        ingest_ref: C opaque object required to passed back to C->ingest method
    """
    global c_callback, c_ingest_ref
    c_callback = callback
    c_ingest_ref = ingest_ref


class MqttSubscriberClient(object):
    """ mqtt subscriber """

    __slots__ = ['mqtt_client', 'broker_host', 'broker_port', 'username', 'password', 'topic',
                 'asset_name', 'loop']

    def __init__(self, config):
        self.mqtt_client = mqtt.Client()
        self.broker_host = config['url']['value']
        self.broker_port = int(config['port']['value'])
        self.username = config['user']['value']
        self.password = config['password']['value']
        self.topic = config['topic']['value']
        self.asset_name = config['assetName']['value']

    def on_connect(self, client, userdata, flags, rc):
        """ The callback for when the client receives a CONNACK response from the server """

        if self.topic.startswith('spBv1.0'):
            client.connected_flag = True
            # subscribe at given Topic on connect
            client.subscribe(self.topic)
            _LOGGER.info("MQTT connection established. Subscribed to topic: {}".format(self.topic))
        else:
            _LOGGER.error("The topic {} is NOT a Sparkplug B v1.0 topic.".format(self.topic))

    def on_disconnect(self, client, userdata, rc):
        pass

    def on_message(self, client, userdata, msg):
        """ The callback for when a PUBLISH message is received from the server """

        _LOGGER.debug("MQTT message received - Topic: {}, Payload: {}".format(
            str(msg.topic), str(msg.payload)))
        try:
            # Protobuf message structure
            sparkplug_payload = sparkplug_b_pb2.Payload()
            sparkplug_payload.ParseFromString(msg.payload)

            for metric in sparkplug_payload.metrics:
                value = "Unknown"
                if metric.HasField("boolean_value"):
                    """ bool value cast to int as internal. See FOGL-8067 """
                    value = metric.boolean_value
                elif metric.HasField("float_value"):
                    value = metric.float_value
                elif metric.HasField("int_value"):
                    value = metric.int_value
                elif metric.HasField("string_value"):
                    # value = metric.string_value
                    # NOTE: handle unescaping the value
                    value = json.loads(metric.string_value)
                # TODO: FOGL- 9198 - Handle other data types
                if value == "Unknown":
                    _LOGGER.warning("Ignoring metric '{}' due to unknown type. "
                                    "Only supported types are: float, integer, string, bool.".format(metric.name))
                    continue

                self.save(metric, value)
        except Exception as ex:
            msg = (
                "Message payload must comply with spBv1.0 standards. Please ensure that the format and structure of the "
                "payload adhere to the specified requirements.")
            _LOGGER.error(ex, msg)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        pass

    def on_unsubscribe(self, client, userdata, mid):
        pass

    def start(self):
        if self.username and len(self.username.strip()) and self.password and len(self.password):
            self.mqtt_client.username_pw_set(self.username, password=self.password)
        # event callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_subscribe = self.on_subscribe
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.connect(self.broker_host, self.broker_port)
        _LOGGER.info("Attempting to connect to MQTT broker at {}:{}...".format(self.broker_host, self.broker_port))

        self.mqtt_client.loop_start()

    def stop(self):
        self.mqtt_client.disconnect()
        self.mqtt_client.loop_stop()

    def save(self, metric, value):
        data = {
            'asset': self.asset_name,
            'timestamp': datetime.fromtimestamp(metric.timestamp, tz=timezone.utc
                                                ).strftime('%Y-%m-%d %H:%M:%S.%s'),
            'readings': {metric.name: value}
        }
        async_ingest.ingest_callback(c_callback, c_ingest_ref, data)
