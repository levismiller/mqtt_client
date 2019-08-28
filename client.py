import logging
import signal
import random
import sys
import threading
from time import sleep

import paho.mqtt.client as mqtt
import msgpack

logging.basicConfig(level=logging.DEBUG)

mqtt_thread_run = True
mqtt_thread = None
tasks_thread_run = True
tasks_thread = None

client = None

TOPIC_PUB = 'fortem/skydome/device/register'
TOPIC_SUB = 'fortem/skydome/status'
# BROKER_ADDR = '127.0.0.1'
BROKER_ADDR = '10.10.12.241'
BROKER_PORT = 1883
BROKER_WAIT = 60

unique = random.uniform(0, 45)
client_id = 'AUJD6-OSKJD-S8JD7-KSUSN' + str(unique)
device_type = 'camera'


# device_type = 'hanger'

def on_message(client, userdata, msg):
    logging.info(msg.topic)


def on_connect(client, userdata, flags, rc):
    logging.info("MQTT: Client connected with result code " + str(rc))
    client.subscribe(TOPIC_SUB)


def mqtt_connection():
    global client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDR, BROKER_PORT, BROKER_WAIT)
    client.loop_forever()


def tasks():
    while tasks_thread_run:
        sleep(3)
        broadcastStat()
        sleep(3)
        broadcastStatus()


def broadcastStatus():
    status = {
        "clientId": client_id,
        "type": device_type,
        "ts": 1562615160123,
        "mqtt": {
            "host": "localhost",
            "port": 1883
        },
        "tracking": {
            "smoothing_alphas": [
                0.7,
                0.7,
                0.8
            ],
            "predict_ahead_time": 1.5
        },
        "throttle_ptz_speed": True,
        "record": {
            "record_video": True,
            "segment_time_s": "60",
            "recording_dir": "/mnt/seagate/recordings"
        },
        "broadcast_rate_hz": 5,
        "stream_url": "ws:10.10.12.111/8003",
        "recording_url": "http://10.10.12.111:9600",
        "lla": [40.34562, -111.79710, 1360.7],
        "orientation": {
            "roll": 0,
            "pitch": 21.0,
            "yaw": -175
        }
    }

    encoded = msgpack.packb(status, use_bin_type=False)
    status_topic = f'fortem/{device_type}/{client_id}/status'
    client.publish(status_topic, payload=encoded, qos=0, retain=False)


def broadcastStat():
    device = {
        'type': device_type,
        'clientId': client_id
    }

    encoded = msgpack.packb(device, use_bin_type=False)
    client.publish(TOPIC_PUB, payload=encoded, qos=0, retain=False)


def __service_shutdown(signum, frame):
    logging.info('Signal caught. Cleaning up and shutting down')
    global mqtt_thread_run
    mqtt_thread_run = False
    mqtt_thread.join(1)
    global tasks_thread_run
    tasks_thread_run = False
    tasks_thread.join(1)
    logging.info('APP: Server DOWN')
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, __service_shutdown)
    signal.signal(signal.SIGINT, __service_shutdown)

    try:
        mqtt_thread = threading.Thread(target=mqtt_connection)
        mqtt_thread.setName('mqtt')
        logging.info(' APP: Thread [' + mqtt_thread.getName() + '] is running')
        mqtt_thread.start()
    except Exception as e:
        logging.error(e)

    try:
        tasks_thread = threading.Thread(target=tasks)
        tasks_thread.setName('tasks')
        logging.info(
            ' APP: Thread [' + tasks_thread.getName() + '] is running')
        tasks_thread.start()
    except Exception as e:
        logging.error(e)
