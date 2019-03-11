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
from foglamp.services.south import exceptions
from foglamp.services.south.ingest import Ingest

import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
from sparkplug_b import *


__author__ = "Jon Scott"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'MQTT Sparkplug Python Plugin',
        'type': 'string',
        'default': 'mqtt-sparkplug',
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
        'order': '1'
    },
    'port': {
        'description': 'Port for MQTT Server',
        'type': 'string',
        'default': '1883',
        'order': '2'
    },
    'user': {
        'description': 'Username for MQTT Server',
        'type': 'string',
        'default': 'opto',
        'order': '3'
    },
    'password': {
        'description': 'Password for MQTT Server',
        'type': 'string',
        'default': 'opto22',
        'order': '4'
    },
    'topic': {
        'description': 'Topic ',
        'type': 'string',
        'default': 'spBv1.0/Opto22/DDATA/groovEPIC_workshop/Strategy',
        'order': '5'
    },       
}

_LOGGER = logger.setup(__name__, level=logging.INFO)
_client = None
_callback_event_loop = None
_topic = None
_serverURL = None
_PLUGIN_NAME = 'MQTT Sparkplug Python Plugin'
_assetName = None

def plugin_info():
    """ Returns information about the plugin.
    Args:
    Returns:
        dict: plugin information
    Raises:
    """

    return {
        'name': _PLUGIN_NAME,
        'version': '1.0',
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
    global _client
    global _topic
    global _serverURL
    global _assetName
    data = copy.deepcopy(config)
    
    # Get connection parameters from configuration
    user = config['user']['value']
    passwd = config['password']['value']
    _serverURL = config['url']['value']
    port = config['port']['value']
    
    # Save the topic of interest for later - will be used in on_connect()
    _topic = config['topic']['value']
    _assetName = config['assetName']['value']
    
    # Create a connection to the MQTT server
    try: 
        _client = mqtt.Client()
        _client.on_connect = on_connect
        _client.on_message = on_message    
        _client.username_pw_set(user, passwd)        
        _client.connect(_serverURL, int(port), 60)
    except Exception as e:
        _LOGGER.error("Error in establishing a connection to MQTT Server: " + str(e))
        _LOGGER.error("Please validate connection parameters")
        
    return data


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
    _client.loop_start()
    _LOGGER.info('Plugin for {} has started.'.format(_assetName))    
    
def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    """
    global _LOGGER
    _LOGGER.info("Old config for {} {} \n new config {}".format(_assetName, handle, new_config))

    # Find diff between old config and new config
    diff = utils.get_diff(handle, new_config)

    # Plugin should re-initialize and restart if key configuration is changed
    if 'url' in diff or 'assetName' in diff or 'topic' in diff:
        plugin_shutdown(handle)
        new_handle = plugin_init(new_config)
        new_handle['restart'] = 'yes'
        _LOGGER.info("Restarting {} plugin due to change in configuration key [{}]".format(_assetName, ', '.join(diff)))
    else:
        new_handle = copy.deepcopy(new_config)
        new_handle['restart'] = 'no'

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

    
# MQTT Server Connection Callback
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):        
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    global _topic
    global _serverURL
    _LOGGER.info('{} plugin has connected to {}.'.format(_assetName, str(_serverURL)))
    
    client.subscribe(_topic)
    _LOGGER.info('{} plugin has subscribed to {}.'.format(_assetName, str(_topic)))

    
async def save_data(data):    
    await Ingest.add_readings(asset=data['asset'], timestamp=data['timestamp'], key=data['key'], readings=data['readings'])

    
# MQTT Message Received Callback
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global _callback_event_loop
    #_LOGGER.info('{} has a message.'.format(_PLUGIN_NAME))    
    try:
        if not Ingest.is_available():
            _LOGGER.error("Ingest is not availabe")
        else:
            inboundPayload = sparkplug_b_pb2.Payload()
            inboundPayload.ParseFromString(msg.payload)
            time_stamp = utils.local_timestamp()
            
            if _callback_event_loop is None:
                    _LOGGER.debug("Message processing event doesn't yet exist - creating new event loop.")
                    asyncio.set_event_loop(asyncio.new_event_loop())
                    _callback_event_loop = asyncio.get_event_loop()
                    
            for metric in inboundPayload.metrics:
                readingKey = str(uuid.uuid4())
                data = {
                    'asset' : metric.name,
                    'timestamp' : time_stamp, #metric.timestamp,
                    'key' : readingKey,
                    'readings' : {                        
                        "value": metric.float_value,
                    }                    
                }
                #_LOGGER.info("UUID: " + readingKey)
                #_LOGGER.info("Metric Name: " + str(metric.name))
                #_LOGGER.info(metric)
                
           
                _callback_event_loop.run_until_complete(save_data(data))              
            
            #_LOGGER.info('Exiting message callback.')
    except Exception as e:
        _LOGGER.error(e)
   
