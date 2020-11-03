# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge.readthedocs.io/
# FLEDGE_END

from unittest.mock import patch
import pytest

from python.fledge.plugins.south.mqtt_sparkplug import mqtt_sparkplug

__author__ = "Ashish Jabble"
__copyright__ = "Copyright (c) 2019 Dianomic Systems"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

config = mqtt_sparkplug._DEFAULT_CONFIG
plugin_name = mqtt_sparkplug._PLUGIN_NAME


def test_plugin_contract():
    # Evaluates if the plugin has all the required methods
    assert callable(getattr(mqtt_sparkplug, 'plugin_info'))
    assert callable(getattr(mqtt_sparkplug, 'plugin_init'))
    assert callable(getattr(mqtt_sparkplug, 'plugin_start'))
    assert callable(getattr(mqtt_sparkplug, 'plugin_shutdown'))
    assert callable(getattr(mqtt_sparkplug, 'plugin_reconfigure'))


def test_plugin_info():
    assert mqtt_sparkplug.plugin_info() == {
        'name': plugin_name,
        'version': '1.8.2',
        'mode': 'async',
        'type': 'south',
        'interface': '1.0',
        'config': config
    }


def test_plugin_init():
    assert mqtt_sparkplug.plugin_init(config) == config


@pytest.mark.skip(reason="To be implemented")
def test_plugin_start():
    pass


@pytest.mark.skip(reason="To be implemented")
def test_plugin_reconfigure():
    pass


@pytest.mark.skip(reason="To be implemented")
def test_plugin_shutdown():
    pass
