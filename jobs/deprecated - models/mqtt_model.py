from sqlalchemy import text, create_engine
import json, os
from dotenv import load_dotenv

import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("rc: " + str(rc))
    print("userdata: " + str(userdata))
    print("flags:" + str(flags))


def on_publish(client, obj, mid):
    print("mid: " + str(mid))
    print("obj: " + str(obj))


def on_disconnect(client, userdata, rc):
    print("disconnected result code " + str(rc))


def generate_topic(charger_name):
    topic_root = os.getenv('MQTT_TOPIC_ROOT')
    return f"mqtt/{topic_root}/{charger_name}"
    # return f"{topic_root}/mw/{charger.name}"


def new_mqtt_client() -> mqtt.Client:
    mqtt_client = mqtt.Client()  # (, clean_session=False)
    mqtt_client.username_pw_set(os.getenv('MQTT_USERNAME'), os.getenv('MQTT_PASSWORD'))
    # mqtt_client.on_publish = on_publish  # assign function to callback

    mqtt_client.connect(os.getenv('MQTT_BROKER'), int(os.getenv('MQTT_PORT')))  # establish connection

    return mqtt_client


def publish(mqtt_client, topic, payload, qos=2, retain=True):
    mqtt_client.publish(topic, str(json.dumps(payload)))
