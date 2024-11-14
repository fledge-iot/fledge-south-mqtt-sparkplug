# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

""" Module for MQTT Sparkplug B Python async plugin """
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
    'user': {
        'description': 'Username for MQTT Server',
        'type': 'string',
        'default': '',
        'order': '1',
        'displayName': 'Username',
        'group': 'Authentication'
    },
    'password': {
        'description': 'Password for MQTT Server',
        'type': 'password',
        'default': '',
        'order': '2',
        'displayName': 'Password',
        'group': 'Authentication'
    },
    'url': {
        'description': 'Hostname for MQTT Server',
        'type': 'string',
        'default': 'localhost',
        'order': '1',
        'displayName': 'MQTT Host',
        'mandatory': 'true',
        'group': 'Connection'
    },
    'port': {
        'description': 'Port for MQTT Server',
        'type': 'string',
        'default': '1883',
        'order': '2',
        'displayName': 'MQTT Port',
        'mandatory': 'true',
        'group': 'Connection'
    },
    'assetNaming': {
        'description': 'Asset naming',
        'type': 'enumeration',
        'options': ['Asset Name', 'Topic Fragments', 'Topic'],
        'default': 'Asset Name',
        'order': '1',
        'displayName': 'Asset Naming',
        'group': 'Readings Structure'
    },
    'assetName': {
        'description': 'Asset Name',
        'type': 'string',
        'default': '',
        'order': '2',
        'displayName': 'Asset Name',
        'group': 'Readings Structure',
        'validity': 'assetNaming == "Asset Name"'
    },
    'topicFragments': {
        'description': 'Values will be used from the subscribed topic',
        'type': 'string',
        'default': 'spBv1.0/{group_id}/{message_type}/{edge_node_id}/{device_id}',
        'order': '3',
        'displayName': 'Topic Fragments',
        'group': 'Readings Structure',
        'validity': 'assetNaming == "Topic Fragments"'
    },
    'datapoints': {
        'description': 'To construct reading datapoints from the received data attributes on topic',
        'type': 'enumeration',
        'options': ['Per metric', 'Per device'],
        'default': 'Per metric',
        'order': '4',
        'displayName': 'Datapoints',
        'group': 'Readings Structure'
    },
    'attachTopicDatapoint': {
        'description': 'Attach Topic as a Datapoint in Reading',
        'type': 'boolean',
        'default': 'false',
        'order': '5',
        'displayName': 'Attach Topic as a Datapoint',
        'group': 'Readings Structure'
    },
    'topic': {
        'description': 'Name of Topic',
        'type': 'string',
        'default': 'spBv1.0/group_id/message_type/edge_node_id/device_id',
        'order': '1',
        'displayName': 'Topic',
        'mandatory': 'true',
        'group': 'Topic'
    }
}

c_callback = None
c_ingest_ref = None
loop = None
namespace = "spBv1.0"
specification = "{}/{{group_id}}/{{message_type}}/{{edge_node_id}}/{{device_id}}".format(namespace)
placeholder_keys = ['group_id', 'message_type', 'edge_node_id', 'device_id']


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
                 'asset_name', 'asset_naming', 'topic_fragments', 'attach_topic_datapoint', 'loop']

    def __init__(self, config):
        self.mqtt_client = mqtt.Client()
        self.broker_host = config['url']['value']
        self.broker_port = int(config['port']['value'])
        self.username = config['user']['value']
        self.password = config['password']['value']
        self.asset_name = config['assetName']['value']
        self.asset_naming = config['assetNaming']['value']
        self.topic = config['topic']['value']
        self.topic_fragments = config['topicFragments']['value']
        self.attach_topic_datapoint = config['attachTopicDatapoint']['value']

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
                    value = metric.string_value
                # TODO: FOGL- 9198 - Handle other data types
                if value == "Unknown":
                    _LOGGER.warning("Ignoring metric '{}' due to unknown type. "
                                    "Only supported types are: float, integer, string, bool.".format(metric.name))
                    continue

                self.save(metric, value)
        except ValueError as err:
            _LOGGER.error(err)
        except Exception as ex:
            msg = ("Message payload must comply with spBv1.0 standards. Please ensure that the format and structure "
                   "of the payload adhere to the specified requirements.")
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
        _LOGGER.info("Attempting to connect to MQTT broker at {}:{}...".format(self.broker_host,
                                                                               self.broker_port))

        self.mqtt_client.loop_start()

    def stop(self):
        self.mqtt_client.disconnect()
        self.mqtt_client.loop_stop()

    def save(self, metric, value):
        if self.asset_naming == 'Topic Fragments':
            asset = self.validate_and_extract_topic_fragment()
        elif self.asset_naming == 'Topic':
            asset = self.topic
        else:
            asset = self.asset_name
        readings = {metric.name: value}
        if self.attach_topic_datapoint == "true":
            readings.update({"SparkPlugB:Topic": self.topic})
        data = {
            'asset': asset,
            'timestamp': datetime.fromtimestamp(metric.timestamp, tz=timezone.utc
                                                ).strftime('%Y-%m-%d %H:%M:%S.%s'),
            'readings': readings
        }
        async_ingest.ingest_callback(c_callback, c_ingest_ref, data)

    def validate_and_extract_topic_fragment(self):
        # Split the specification and fragment into parts
        spec_parts = specification.split('/')
        fragment_parts = self.topic_fragments.split('/')

        # Check if fragment contains valid placeholders
        valid_placeholders = set(placeholder_keys)
        fragment_placeholders = []

        # Check for multiple occurrences of namespace in the fragment
        if fragment_parts.count(namespace) > 1:
            raise ValueError("Invalid fragment: '{}' appears more than once.".format(namespace))

        # Validate placeholders and collect them
        for part in fragment_parts:
            if part.startswith("{") and part.endswith("}"):
                # Extract the placeholder key by removing curly braces
                key = part[1:-1]
                fragment_placeholders.append(key)
                # Check if it's a valid placeholder
                if key not in valid_placeholders:
                    raise ValueError("Invalid placeholder '{}' found in the topic fragment.".format(key))
            elif part != namespace:  # Ensure no extraneous parts except namespace or valid placeholders
                raise ValueError("Invalid part '{}' found in the topic fragment.".format(part))

        # Check for duplicate placeholders in the fragment
        duplicate_placeholders = [key for key in fragment_placeholders if fragment_placeholders.count(key) > 1]
        if duplicate_placeholders:
            raise ValueError("Invalid fragment: Duplicate placeholder(s) found: {}.".format(', '.join(
                set(duplicate_placeholders))))

        # Validate the order of placeholders
        placeholder_order = {key: spec_parts.index('{{{}}}'.format(key)) for key in placeholder_keys}

        # Check if placeholders are in the correct order
        last_index = -1
        for placeholder in fragment_placeholders:
            if placeholder not in placeholder_order:
                raise ValueError("Unknown placeholder '{}' in the fragment.".format(placeholder))
            current_index = placeholder_order[placeholder]
            if current_index < last_index:
                raise ValueError("Invalid order: '{}' must appear after its previous placeholders.".format(placeholder))
            last_index = current_index

        # Ensure version prefix namespace appears before any placeholders
        if namespace in fragment_parts:
            if fragment_parts[0] != namespace:
                raise ValueError("'{}' must appear before any placeholders.".format(namespace))
        else:
            # If no version prefix is provided, ensure the fragment still follows the placeholder order
            if len(fragment_parts) == 0 or fragment_parts[0] in placeholder_keys:
                raise ValueError("The fragment must start with '{}' if no placeholders are given.".format(namespace))

        # Extract the values from the topic based on placeholders
        topic_parts = self.topic.split('/')
        placeholder_values = {}

        for i, part in enumerate(spec_parts):
            if part.startswith("{") and part.endswith("}"):  # It's a placeholder
                key = part[1:-1]  # Remove the curly braces to get the key
                placeholder_values[key] = topic_parts[i]  # Map the key to the corresponding topic part

        # Replace the placeholders in the fragment with values from the topic
        result_parts = []
        for part in fragment_parts:
            if part.startswith("{") and part.endswith("}"):  # If it's a placeholder
                key = part[1:-1]  # Get the placeholder key
                result_parts.append(placeholder_values.get(key))  # Get the corresponding value
            else:
                result_parts.append(part)  # If it's not a placeholder, just add the part as is

        # Join the result parts to form the final output
        result = '/'.join(result_parts)
        return result

