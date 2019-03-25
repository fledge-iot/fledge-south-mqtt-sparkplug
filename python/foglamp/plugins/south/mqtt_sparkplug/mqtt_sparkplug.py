# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Module for MQTT Sparkplug Python async plugin """

import asyncio
import copy
import uuid
import logging

from foglamp.common import logger
from foglamp.plugins.common import utils
import async_ingest

import paho.mqtt.client as mqtt
from foglamp.plugins.south.mqtt_sparkplug.sparkplug_b import *


__author__ = "Jon Scott, Ashish Jabble"
__copyright__ = "Copyright (c) 2019 Dianomic Systems"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


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
        'order': '1'
    },
    'url': {
        'description': 'URL for MQTT Server',
        'type': 'string',
        'default': 'chariot.groov.com',
        'order': '2'
    },
    'port': {
        'description': 'Port for MQTT Server',
        'type': 'string',
        'default': '1883',
        'order': '3'
    },
    'user': {
        'description': 'Username for MQTT Server',
        'type': 'string',
        'default': 'opto',
        'order': '4'
    },
    'password': {
        'description': 'Password for MQTT Server',
        'type': 'string',
        'default': 'opto22',
        'order': '5'
    },
    'topic': {
        'description': 'Name of Topic',
        'type': 'string',
        'default': 'spBv1.0/Opto22/DDATA/groovEPIC_workshop/Strategy',
        'order': '6'
    },       
}

_LOGGER = logger.setup(__name__, level=logging.INFO)
_client = None
_callback_event_loop = None
_topic = None
_serverURL = None
_assetName = None

c_callback = None
c_ingest_ref = None
loop = None
t = None


def plugin_info():
    """ Returns information about the plugin.
    Args:
    Returns:
        dict: plugin information
    Raises:
    """

    return {
        'name': _PLUGIN_NAME,
        'version': '1.5.0',
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
    global _client
    global _topic
    global _serverURL
    global _assetName

    # Get connection parameters from configuration
    user = handle['user']['value']
    password = handle['password']['value']
    _serverURL = handle['url']['value']
    port = handle['port']['value']

    # Save the topic of interest for later - will be used in on_connect()
    _topic = handle['topic']['value']
    _assetName = handle['assetName']['value']

    # Create a connection to the MQTT server
    try:
        _client = mqtt.Client()
        _client.on_connect = on_connect
        _client.on_message = on_message
        _client.username_pw_set(user, password)
        _client.connect(_serverURL, int(port), 60)
    except Exception as e:
        _LOGGER.error("Error in establishing a connection to MQTT Server: " + str(e))

    _client.loop_start()


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
    global _client
    global _LOGGER
    global _callback_event_loop

    _client.loop_stop(force=False)
    if _callback_event_loop is not None:
        _callback_event_loop.close()
        _callback_event_loop = None
    _LOGGER.info('{} has shut down.'.format(_PLUGIN_NAME))


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


# MQTT Server Connection Callback
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):        
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    global _topic
    global _serverURL
    _LOGGER.info('{} plugin has connected to {}.'.format(_PLUGIN_NAME, str(_serverURL)))
    
    client.subscribe(_topic)
    _LOGGER.info('{} plugin has subscribed to {}.'.format(_PLUGIN_NAME, str(_topic)))

    
async def save_data(data):    
    async_ingest.ingest_callback(c_callback, c_ingest_ref, data)


# MQTT Message Received Callback
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global _callback_event_loop
    try:
        inbound_payload = sparkplug_b_pb2.Payload()
        inbound_payload.ParseFromString(msg.payload)
        time_stamp = utils.local_timestamp()

        if _callback_event_loop is None:
                _LOGGER.debug("Message processing event doesn't yet exist - creating new event loop.")
                asyncio.set_event_loop(asyncio.new_event_loop())
                _callback_event_loop = asyncio.get_event_loop()

        for metric in inbound_payload.metrics:
            key = str(uuid.uuid4())
            data = {
                'asset': metric.name,
                'timestamp': time_stamp,  # metric.timestamp
                'key': key,
                'readings': {
                    "value": metric.float_value,
                }
            }
            _callback_event_loop.run_until_complete(save_data(data))
    except Exception as e:
        _LOGGER.error(e)
