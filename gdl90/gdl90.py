import os
from time import sleep
import json
from datetime import datetime
from typing import Any, Dict
import socket
import threading
import schedule
import logging
from lib import decoder

from base_mqtt_pub_sub import BaseMQTTPubSub
from collections import namedtuple


class GDL90PubSub(BaseMQTTPubSub):
    """
    This class creates a connection to the MQTT broker and to the UDP port via
    the socket library to listen for GDL90 Unicast messages (both 1090 and 978) from a 
    Stratux device
    Args:
        BaseMQTTPubSub (BaseMQTTPubSub): parent class written in the EdgeTech Core module
    """

    def __init__(
        self: Any,
        gdl_receive_port: str,
        send_data_topic: str,
        debug: bool = True,
        **kwargs: Any,
    ):
        """
        The GDL90PubSub constructor takes a serial port address and after
        instantiating a connection to the MQTT broker also connects to the serial
        port specified.
        Args:
            send_data_topic (str): MQTT topic to publish the data from the port to.
            Specified via docker-compose.
            debug (bool, optional): If the debug mode is turned on, log statements print to stdout.
            Defaults to False.
        """
        super().__init__(**kwargs)
        # convert contructor parameters to class variables
        self.gdl_receive_port = gdl_receive_port
        self.send_data_topic = send_data_topic
        self.kill_listener = False
        self.gdl_buffer_length = 1024
        self.gdl_decoder = decoder.Decoder()
        self.gdl_decoder.addReturnHandler(self._GDL_return)

        self.current_gdl_timestamp = None

        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

        # connect to the MQTT client
        self.connect_client()
        sleep(1)
        # publish a message after successful connection to the MQTT broker
        self.publish_registration("GDL90 Sender Registration")

    def _GDL_return(self: Any, message: namedtuple):
        """Format GDL message for passing onto the bus and publish"""
        if message.MsgType == "Heartbeat":
            self.current_gdl_timestamp = message.Timestamp

        if message.MsgType == "TrafficReport":
            publish_dict = {}
            publish_dict["vertical_velocity"] = message.VVelocity
            #tslc
            publish_dict["time"] = self.current_gdl_timestamp 
            """ Per GDL 90 Data Interface Specification, 
            'The Time Stamp conveyed in the most recent Heartbeat message is the Time of Applicability 
            for all Traffic Reports output in that second. '"""
            publish_dict["altitude"] = message.Altitude
            publish_dict["icao_hex"] = message.Address
            publish_dict["horizontal_velocity"] = message.HVelocity
            publish_dict["track"] = message.TrackHeading
            publish_dict["lat"] = message.Latitude
            publish_dict["lon"] = message.Longitude
            publish_dict["flight"] = message.CallSign
            #squawk
            self._senddata(publish_dict)
        else:
            logging.info("Non-traffic message: " + str(message.MsgType))        

    def _handle_GDL_message(self: Any, message: bytes):
        """
        Handles received GDL messages.
        """

        self.gdl_decoder.addBytes(bytearray.fromhex(message))

    def _construct_listener(self: Any) -> None:
        """
        Sets up a socket connection using python's socket package to the port specified
        in the constructor.
        """
        self.listener_thread = threading.Thread(target=self._listen_port)
        self.listener_thread.start()

    def _listen_port(self: Any) -> None:
        "Continuously listens on a given port for UDP datagrams"
        while not self.kill_listener:
            try:
                self.soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.soc.bind(
                    "localhost",
                    self.gdl_receive_port
                )
                data, addr = self.soc.recvfrom(self.gdl_buffer_length)
                self.handle_GDL_message(data)
                logging.debug("Handling GDL message")

            except Exception as e:
                logging.error("Error in establishing connection to local socket")
        
        self.soc.close()

    def _disconnect_socket(self: Any) -> None:
        """Disconnects the serial connection using python's serial package."""
        self.serial.close()

    def _send_data(self: Any, data: Dict[str, str]) -> bool:
        """
        Leverages edgetech-core functionality to publish a JSON payload to the MQTT
        broker on the topic specified in the class constructor.
        Args:
            data (Dict[str, str]): Dictionary payload that maps keys to payload.
        Returns:
            bool: Returns True if successful publish else False.
        """
        out_json = self.generate_payload_json(
            push_timestamp=str(int(datetime.utcnow().timestamp())),
            device_type="SkyScan",
            id_="Deployment",
            deployment_id=f"SkyScan-Arlington-{'TEST'}",
            current_location="-90, -180",
            status="Debug",
            message_type="Event",
            model_version="null",
            firmware_version="v0.0.0",
            data_payload_type="",
            data_payload=json.dumps(data),
        )

        # publish the data as a JSON to the topic
        success = self.publish_to_topic(self.send_data_topic, out_json)

        if success:
            logging.debug(
                f"Successfully sent data on channel {self.send_data_topic}: {json.dumps(data)}"
            )
        else:
            logging.debug(
                f"Failed to send data on channel {self.send_data_topic}: {json.dumps(data)}"
            )
        # return True if successful else False
        return success

    def main(self: Any) -> None:
        """
        Main loop and function that setup the heartbeat to keep the TCP/IP
        connection alive and publishes the data to the MQTT broker and keeps the
        main thread alive.
        """
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="GDL Sender Heartbeat"
        )

        self._construct_listener()

        while True:
            try:
                # flush any scheduled processes that are waiting
                schedule.run_pending()
                # prevent the loop from running at CPU time
                sleep(0.001)

            except KeyboardInterrupt as exception:
                # if keyboard interrupt, fail gracefully
                self.kill_listener = True
                self.graceful_stop()
                if self.debug:
                    print(exception)


if __name__ == "__main__":
    sender = GDL90PubSub(
        gdl_receive_port=str(os.environ.get("GDL_RECEIVE_PORT")),
        send_data_topic=str(os.environ.get("SEND_DATA_TOPIC")),
        mqtt_ip=str(os.environ.get("MQTT_IP")),
    )
    sender.main()
