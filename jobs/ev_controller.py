import json
import os
import logging
import time

import paho.mqtt.client as mqtt
from sqlalchemy import text, create_engine

from dotenv import load_dotenv
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)


def on_connect(client, userdata, flags, rc):
    print("rc: " + str(rc))
    print("userdata: " + str(userdata))
    print("flags:" + str(flags))


def on_publish(client, obj, mid):
    print("mid: " + str(mid))
    print("obj: " + str(obj))


def generate_topic(charger_name):
    topic_root = os.getenv('MQTT_TOPIC_ROOT')
    return f"mqtt/{topic_root}/{charger_name}"


if __name__ == '__main__':
    load_dotenv()

    os.environ['TZ'] = 'Asia/Hong_Kong'
    time.tzset()
    logging.info(f"Timezone: {time.tzname}")

    mqtt_client = mqtt.Client()  # (, clean_session=False)
    mqtt_client.username_pw_set(os.getenv('MQTT_USERNAME'), os.getenv('MQTT_PASSWORD'))

    db_uri = os.getenv('DB_URI')
    engine = create_engine(db_uri)  # echo=True for debugging
    # engine = create_engine(db_uri, echo=True)
    db = engine.connect()

    now = datetime.now()

    print("Starting ev controller...")

    # get all transactions in progress
    sql = text("""
        SELECT id, charger_id, payment_id, id_tag, start_time, end_time, actual_start_time, target_end_time, 
        actual_end_time, actual_duration, duration, meter_start, meter_stop, status, date_created, date_modified
        FROM ev_transaction
        WHERE status <> 'remote_stop' AND status <> 'finished' AND (target_end_time <= NOW() OR (end_time <= NOW() AND target_end_time IS NULL))
    """)

    results = db.execute(sql).fetchall()

    transactions_ended = 0

    try:
        if results:
            print(f"Found {len(results)} EVSE transactions in progress...")

            # mqtt_client = mqtt_model.new_mqtt_client()  # (, clean_session=False)
            mqtt_client.connect(os.getenv('MQTT_BROKER'), int(os.getenv('MQTT_PORT')))  # establish connection

            for r in results:
                # if actual_end_time is null, then the transaction is still in progress and we need to stop it
                if r.actual_end_time is None:
                    sql = text(f"""
                            SELECT ev_charger.id, ev_charger.name, uuid, location_id, ev_location.pricing_id as location_pricing_id, 
                            ev_charger.pricing_id AS charger_pricing_id, vendor, token_id, ev_location.url_name as location_url_name,
                            ev_charger.ocpp_version, ev_charger.firmware, ev_charger.serial_no, ev_charger.model, ev_charger.zone, 
                            ev_charger.date_created, ev_charger.date_modified
                            FROM ev_charger
                            INNER JOIN ev_location ON ev_location.id = ev_charger.location_id 
                             WHERE ev_charger.id = :id
                        """).bindparams(id=r.charger_id)

                    charger = db.execute(sql).fetchone()

                    topic = generate_topic(charger.name)
                    payload = {"action": "RemoteStopTransaction",
                               "args": {"transaction_id": r.id}}

                    # mqtt_model.publish(mqtt_client, topic, payload, qos=2, retain=True)
                    mqtt_client.publish(topic, str(json.dumps(payload)), qos=0, retain=False)

                    logging.debug(f"Publish: {topic}, {str(json.dumps(payload))}")

                    transactions_ended += 1

        else:
            print("No EVSE transactions in progress...")

        print(f"{transactions_ended} EVSE transactions ended...")

    except Exception as e:
        logging.error(f"Exception: {e}")

    # get all ev charger status that are "finishing"
    # sql = text("""
    #         SELECT id, charger_id, status, error_code, date_modified
    #         FROM ev_status
    #         WHERE status = 'Finishing'
    #     """)
    # # OR status = 'Faulted'
    #
    # with engine.connect() as db:
    #     chargers = db.execute(sql).fetchall()
    #
    # try:
    #     if chargers:
    #         if mqtt_client is None:
    #             mqtt_client = mqtt_model.new_mqtt_client()  # (, clean_session=False)
    #
    #         for c in chargers:
    #             topic = mqtt_model.generate_topic(charger)
    #             payload = {"action": "TriggerMessage",
    #                        "args": {"requested_message": "StatusNotification"}}
    #
    #             mqtt_model.publish(mqtt_client, topic, payload, qos=0, retain=False)
    #             logging.debug(f"Trigger StatusNotification: {topic}, {str(json.dumps(payload))}")
    #
    # except Exception as e:
    #     logging.error(f"Exception: {e}")
