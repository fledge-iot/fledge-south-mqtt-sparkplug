===========================
fledge-south-mqtt-sparkplug
===========================

Fledge South MQTT SparkplugB (Subscriber) Plugin.

How to Test?
~~~~~~~~~~~~

Prerequisite
------------
The MQTT broker service should be running.

The example given here are tested using `mosquitto` http://test.mosquitto.org/

.. code-block:: console

    $ sudo apt install -y mosquitto
    $ sudo systemctl enable mosquitto.service


Install `paho-mqtt` pip package.

.. code-block:: console

    python3 -m pip install -r requirements-mqtt_sparkplug.txt


Run publisher script
--------------------

.. code-block:: console

    $ python3 -m mqtt-pub

    Publishing metric: Temperature Sensor, Type: float, Value: 30.175339239401513, Timestamp: 1729752898
    Publishing metric: Humidity Sensor, Type: integer, Value: 137, Timestamp: 1729752898
    Publishing metric: Location, Type: string, Value: NCR, Timestamp: 1729752898
    Publishing metric: Status, Type: boolean, Value: True, Timestamp: 1729752898

See Output with Plugin
----------------------

The Fledge plugin once configured to subscribe the topic `spBv1.0/Opto22/DDATA/groovEPIC_workshop/Strategy` which is by default and then it will start
ingesting the published messages.

Now, you can check Asset and Readings Tab either from GUI client or curl client; use below

.. code-block:: console

    $ curl -sX GET "http://localhost:8081/fledge/asset/mqtt?limit=4" | jq
    [
      {
        "reading": {
          "Temperature Sensor": 30.17533923
        },
        "timestamp": "2024-10-24 06:55:03.172973"
      },
      {
        "reading": {
          "Humidity Sensor": 137
        },
        "timestamp": "2024-10-24 06:55:03.172973"
      },
      {
        "reading": {
          "Location": "NCR"
        },
        "timestamp": "2024-10-24 06:55:03.172973"
      },
      {
        "reading": {
          "Status": 1
        },
        "timestamp": "2024-10-24 06:55:03.172973"
      }
    ]
