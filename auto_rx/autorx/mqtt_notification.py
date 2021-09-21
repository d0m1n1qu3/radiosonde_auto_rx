#!/usr/bin/env python
#
#   radiosonde_auto_rx - MQTT Notification
#
#   Heavily inspired throu Email Notification from Philip Heron <phil@sanslogic.co.uk>
#
#   Copyright (C) 2021 Dominique Matz <d0m1n1qu3@l3x.de>
#   Released under GNU GPL v3 or later

import datetime
import logging
import time
import json
import paho.mqtt.publish as publish
from email.utils import formatdate
from threading import Thread
from .config import read_auto_rx_config
from .utils import position_info, strip_sonde_serial
from .geometry import GenericTrack

try:
    # Python 2
    from Queue import Queue

except ImportError:
    # Python 3
    from queue import Queue


class MqttNotification(object):
    """ MQTT Notification Class.

    Accepts telemetry dictionaries from a decoder, and sends a mqtt message on newly 
     detected sondes and on new Telemetry Infos.
    Incoming telemetry is processed via a queue, so this object should be thread safe.

    """

    # We require the following fields to be present in the input telemetry dict.
    REQUIRED_FIELDS = ["id", "lat", "lon", "alt", "type", "freq"]

    def __init__(
        self,
        mqtt_server="localhost",
        mqtt_port=1883,
        mqtt_authentication="None",
        mqtt_login="None",
        mqtt_password="None",
        mqtt_topic="radiosonde_auto_rx/",
        station_position=None,
        launch_notifications=True,
        landing_notifications=True,
        landing_range_threshold=50,
        landing_altitude_threshold=1000,
        landing_descent_trip=10,
    ):
        """ Init a new MQTT Notification Thread """
        self.mqtt_server = mqtt_server
        self.mqtt_port = mqtt_port
        self.mqtt_authentication = mqtt_authentication
        self.mqtt_login = mqtt_login
        self.mqtt_password = mqtt_password
        self.mqtt_topic = mqtt_topic
        self.station_position = station_position
        self.launch_notifications = launch_notifications
        self.landing_notifications = landing_notifications
        self.landing_range_threshold = landing_range_threshold
        self.landing_altitude_threshold = landing_altitude_threshold
        self.landing_descent_trip = landing_descent_trip

        # Dictionary to track sonde IDs
        self.sondes = {}

        self.max_age = 3600 * 2  # Only store telemetry for 2 hours

        # Input Queue.
        self.input_queue = Queue()

        # MQTT client
        #mqtt_client = paho.mqtt.client.Client()
        #self.log_info("Connecting to MQTT Server %s:%s" % (self.mqtt_server, self.mqtt_port))
        #mqtt_client.connect(self.mqtt_server, self.mqtt_port)
        #mqtt_client.loop_start()

        # Start queue processing thread.
        self.input_processing_running = True
        self.input_thread = Thread(target=self.process_queue)
        self.input_thread.start()

        self.log_info("Started MQTT Notifier Thread")

    def add(self, telemetry):
        """ Add a telemetery dictionary to the input queue. """
        # Check the telemetry dictionary contains the required fields.
        for _field in self.REQUIRED_FIELDS:
            if _field not in telemetry:
                self.log_error("JSON object missing required field %s" % _field)
                return

        # Add it to the queue if we are running.
        if self.input_processing_running:
            self.input_queue.put(telemetry)
        else:
            self.log_error("Processing not running, discarding.")

    def process_queue(self):
        """ Process packets from the input queue. """
        while self.input_processing_running:

            # Process everything in the queue.
            while self.input_queue.qsize() > 0:
                try:
                    _telem = self.input_queue.get_nowait()
                    self.process_telemetry(_telem)

                except Exception as e:
                    self.log_error("Error processing telemetry dict - %s" % str(e))

            # Sleep while waiting for some new data.
            time.sleep(2)

            self.clean_telemetry_store()

    def process_telemetry(self, telemetry):
        """ Process a new telemmetry dict, and send a mqtt msg if it is a new sonde. """
        _id = telemetry["id"]

        if _id not in self.sondes:
            self.sondes[_id] = {
                "last_time": time.time(),
                "descending_trip": 0,
                "descent_notified": False,
                "track": GenericTrack(max_elements=20),
            }

            # Add initial position to the track info.
            self.sondes[_id]["track"].add_telemetry(
                {
                    "time": telemetry["datetime_dt"],
                    "lat": telemetry["lat"],
                    "lon": telemetry["lon"],
                    "alt": telemetry["alt"],
                }
            )

            if self.launch_notifications:

                try:
                    # This is a new sonde. Send the MQTT msg.
                    ##########################################################
                    self.send_notification_mqtt(topic=self.mqtt_topic+"/new", message=json.dumps(telemetry))
                    ##########################################################
                
                except Exception as e:
                    self.log_error("Error sending MQTT msg - %s" % str(e))

        else:
            # Update track data.
            _sonde_state = self.sondes[_id]["track"].add_telemetry(
                {
                    "time": telemetry["datetime_dt"],
                    "lat": telemetry["lat"],
                    "lon": telemetry["lon"],
                    "alt": telemetry["alt"],
                }
            )
            # Update last seen time, so we know when to clean out this sondes data from memory.
            self.sondes[_id]["last_time"] = time.time()

            # We have seen this sonde recently. Let's check it's descending...

            if self.sondes[_id]["descent_notified"] == False and _sonde_state:
                # If the sonde is below our threshold altitude, *and* is descending at a reasonable rate, increment.
                if (telemetry["alt"] < self.landing_altitude_threshold) and (
                    _sonde_state["ascent_rate"] < -2.0
                ):
                    self.sondes[_id]["descending_trip"] += 1

                if self.sondes[_id]["descending_trip"] > self.landing_descent_trip:
                    # We've seen this sonde descending for enough time now.
                    # Note that we've passed the descent threshold, so we shouldn't analyze anything from this sonde anymore.
                    self.sondes[_id]["descent_notified"] = True

                    self.log_debug("Sonde %s triggered descent threshold." % _id)

                    # Let's check if it's within our notification zone.

                    if self.station_position != None:
                        _relative_position = position_info(
                            self.station_position,
                            (telemetry["lat"], telemetry["lon"], telemetry["alt"]),
                        )

                        _range = _relative_position["straight_distance"] / 1000.0
                        self.log_debug(
                            "Descending sonde is %.1f km away from station location"
                            % _range
                        )

                        if (
                            _range < self.landing_range_threshold
                            and self.landing_notifications
                        ):
                            self.log_info(
                                "Landing sonde %s triggered range threshold." % _id
                            )

                            # Nearby sonde landing detected
                            ##########################################################
                            self.send_notification_mqtt(topic=self.mqtt_topic+"/landing", message=json.dumps(telemetry))
                            ##########################################################

                    else:
                        # No station position to work with! Bomb out at this point
                        return

    def send_notification_mqtt(
        self, topic="radiosonde_auto_rx", message="Foobar"
    ):
        try:
            # TODO: auth and TLS
            publish.single(topic, message, hostname=self.mqtt_server, port=self.mqtt_port)

            self.log_info("MQTT notification sent.")
        except Exception as e:
            self.log_error("Error sending MQTT notification - %s" % str(e))

        pass

    def clean_telemetry_store(self):
        """ Remove any old data from the telemetry store """

        _now = time.time()
        _telem_ids = list(self.sondes.keys())
        for _id in _telem_ids:
            # If the most recently telemetry is older than self.max_age, remove all data for
            # that sonde from the local store.
            if (_now - self.sondes[_id]["last_time"]) > self.max_age:
                self.sondes.pop(_id)
                self.log_debug("Removed Sonde #%s from archive." % _id)

    def close(self):
        """ Close input processing thread. """
        self.log_debug("Waiting for processing thread to close...")
        self.input_processing_running = False

        if self.input_thread is not None:
            self.input_thread.join()

    def running(self):
        """ Check if the logging thread is running.

        Returns:
            bool: True if the logging thread is running.
        """
        return self.input_processing_running

    def log_debug(self, line):
        """ Helper function to log a debug message with a descriptive heading. 
        Args:
            line (str): Message to be logged.
        """
        logging.debug("MQTT - %s" % line)

    def log_info(self, line):
        """ Helper function to log an informational message with a descriptive heading. 
        Args:
            line (str): Message to be logged.
        """
        logging.info("MQTT - %s" % line)

    def log_error(self, line):
        """ Helper function to log an error message with a descriptive heading. 
        Args:
            line (str): Message to be logged.
        """
        logging.error("MQTT- %s" % line)


if __name__ == "__main__":
    # Test Script - Send an example mqtt msg using the settings in station.cfg
    import sys

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG
    )

    # Read in the station config, which contains the mqtt settings.
    config = read_auto_rx_config("station.cfg", no_sdr_test=True)

    # Start up an mqtt notifification object.
    _mqtt_notification = MqttNotification(
        mqtt_server=config["mqtt_server"],
        mqtt_port=config["mqtt_port"],
        mqtt_authentication=config["mqtt_authentication"],
        mqtt_login=config["mqtt_login"],
        mqtt_password=config["mqtt_password"],
        mqtt_topic=config["mqtt_topic"],
        station_position=(-10.0, 10.0, 0.0,),
        landing_notifications=True,
        launch_notifications=True,
    )

    # Wait a second..
    time.sleep(1)

    if len(sys.argv) > 1:
        _mqtt_notification.send_notification_mqtt(message="This is a test message")
        time.sleep(1)

    # Add in a packet of telemetry, which will cause the mqtt notifier to send a msg.
    print("Testing launch alert.")
    _mqtt_notification.add(
        {
            "id": "N1234557",
            "frame": 10,
            "lat": -10.0,
            "lon": 10.0,
            "alt": 10000,
            "temp": 1.0,
            "type": "RS41",
            "freq": "401.520 MHz",
            "freq_float": 401.52,
            "heading": 0.0,
            "vel_h": 5.1,
            "vel_v": -5.0,
            "datetime_dt": datetime.datetime.utcnow(),
        }
    )

    # Wait a little bit before shutting down.
    time.sleep(5)

    _test = {
        "id": "N1234557",
        "frame": 10,
        "lat": -10.01,
        "lon": 10.01,
        "alt": 800,
        "temp": 1.0,
        "type": "RS41",
        "freq": "401.520 MHz",
        "freq_float": 401.52,
        "heading": 0.0,
        "vel_h": 5.1,
        "vel_v": -5.0,
        "datetime_dt": datetime.datetime.utcnow(),
    }

    print("Testing landing alert.")
    for i in range(20):
        _mqtt_notification.add(_test)
        _test["alt"] = _test["alt"] - 5.0
        _test["datetime_dt"] = datetime.datetime.utcnow()
        time.sleep(2)

    time.sleep(60)

    _mqtt_notification.close()
