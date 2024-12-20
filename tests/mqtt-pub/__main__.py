#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import random
import time
from . import sparkplug_b_pb2
import paho.mqtt.client as mqtt


__author__ = "Ashish Jabble (Dianomic)"
__copyright__ = "Copyright (c) 2024 Dianomic Systems, Inc."
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# MQTT config
MQTT_BROKER = "localhost"
MQTT_PORT = 1883 # Default port
KEEP_ALIVE_INTERVAL = 45

# Define your Sparkplug parameters
# Topic => namespace/group_id/message_type/edge_node_id/device_id
# Define your Sparkplug parameters
DBIRTH_TOPIC = "spBv1.0/Opto22/DBIRTH/groovEPIC_workshop/Strategy" # Indicates when device comes online
DDEATH_TOPIC = "spBv1.0/Opto22/DDEATH/groovEPIC_workshop/Strategy" # Indicates when device goes online
DDATA_TOPIC = "spBv1.0/Opto22/DDATA/groovEPIC_workshop/Strategy"   # Send Periodic data updates from device

# Define your device ID
device_id = "your_device_id"


# Sparkplug payload example for DBIRTH
def create_dbirth_payload():
    message = {
        "metrics": [
            {
                "name": "Temperature Sensor",
                "value": random.uniform(22.0, 32.0),  # Float
                "timestamp": int(time.time()),
                "type": "float"
            }
        ]
    }
    # Create a Payload instance
    payload = sparkplug_b_pb2.Payload()
    for metric in message["metrics"]:
        m = payload.metrics.add()  # Add a new Metric to the Payload
        m.name = metric["name"]
        m.timestamp = metric["timestamp"]
        m.float_value = metric["value"]
    # Now you can serialize the payload or use it as needed
    binary_data = payload.SerializeToString()
    return binary_data


# Create DDATA payload
def create_ddata_payload():
    bool_values = [True, False]
    message = {
        "metrics": [
            {
                "name": "Temperature Sensor",
                "value": random.uniform(-22.0, 32.0),  # Float
                "timestamp": int(time.time()),
                "type": "float"
            },
            {
                "name": "Double",
                "value": random.uniform(-100.0, 100.0),  # Double
                "timestamp": int(time.time()),
                "type": "double"
            },
            {
                "name": "Humidity Sensor",
                "value": random.randint(-45, 175),  # Integer
                "timestamp": int(time.time()),
                "type": "integer"
            },
            {
                "name": "Location",
                "value": "NCR",  # String
                "timestamp": int(time.time()),
                "type": "string"
            },
            {
                "name": "Status",
                "value": random.choice(bool_values),  # Boolean
                "timestamp": int(time.time()),
                "type": "boolean"
            }
        ]
    }

    # Create a Payload instance
    payload = sparkplug_b_pb2.Payload()
    # Populate the Payload with Metric instances
    for metric in message["metrics"]:
        m = payload.metrics.add()  # Add a new Metric to the Payload
        m.name = metric["name"]
        m.timestamp = metric["timestamp"]
        try:
            # Set the appropriate value based on type
            if metric["type"] == "float":
                m.float_value = metric["value"]
            elif metric["type"] == "double":
                m.double_value = metric["value"]
            elif metric["type"] == "integer":
                # Ensure that the value fits within the range of int32
                if -2147483648 <= metric["value"] <= 2147483647:
                    m.int_value = metric["value"]
                else:
                    # value within the range of uint64
                    m.long_value = metric["value"]
            elif metric["type"] == "string":
                m.string_value = metric["value"]
            elif metric["type"] == "boolean":
                m.boolean_value = metric["value"]
            else:
                print(f"Ignoring metric '{m.name}' due to unknown type.")
                continue
            print(f"Publishing metric with Name: {m.name}, Type: {metric['type']}, Value: {metric['value']}, "
                  f"Timestamp: {m.timestamp}")
        except Exception as ex:
            print(f"Error in metric name: '{m.name}' due to '{ex}'")

    # Now you can serialize the payload or use it as needed
    binary_data = payload.SerializeToString()
    return binary_data


# Create DDEATH payload
def create_ddeath_payload():
    message = {
        "metrics": [
            {
                "name": "Status",
                "value": False,  # Boolean
                "timestamp": int(time.time()),
                "type": "boolean"
            }
        ]
    }
    # Create a Payload instance
    payload = sparkplug_b_pb2.Payload()
    for metric in message["metrics"]:
        m = payload.metrics.add()  # Add a new Metric to the Payload
        m.name = metric["name"]
        m.timestamp = metric["timestamp"]
        m.boolean_value = metric["value"]
    # Now you can serialize the payload or use it as needed
    binary_data = payload.SerializeToString()
    return binary_data


# Callback for when the client connects
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Publish DBIRTH message
    dbirth = create_dbirth_payload()
    client.publish(DBIRTH_TOPIC, dbirth)
    print("Published DBIRTH message")


# Initialize MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect

# Connect to MQTT broker
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, KEEP_ALIVE_INTERVAL)

# Start the loop
mqtt_client.loop_start()

try:
    while True:
        # Publish DDATA message periodically
        ddata = create_ddata_payload()
        mqtt_client.publish(DDATA_TOPIC, ddata)
        print("Published DDATA message")
        time.sleep(5)  # Publish every 5 seconds
except KeyboardInterrupt:
    # Publish DDEATH message on exit
    ddeath = create_ddeath_payload()
    mqtt_client.publish(DDEATH_TOPIC, ddeath)
    print("Published DDEATH message")

finally:
    # Disconnect
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("Disconnected from broker")

