import connexion
import yaml
import logging
import logging.config
import json # Addition for Lab 6
import threading # Addition for Lab 6
import time
from pykafka import KafkaClient # Addition for Lab 6
from pykafka.common import OffsetType # Addition for Lab 6
from datetime import datetime
from connexion import NoContent
from db import make_session
from models import Lap, Sector


# .\venv\Scripts\Activate.ps1


with open("/config/storage_config.yaml", "r") as file:
    app_config = yaml.safe_load(file.read())

with open("/config/storage_log_config.yaml", "r") as file:
    LOG_CONFIG = yaml.safe_load(file.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

# paths handled by connexion via the yaml file
def add_lap(body):
    session = make_session()

    batch_timestamp_parsed = datetime.strptime(body["batch_timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")

    event = Lap(
        car_id = body["car_id"],
        batch_id = body["batch_id"],
        stint_id = body["stint_id"],
        batch_timestamp = batch_timestamp_parsed,
        lap_number = body["lap_number"],


        lap_time_seconds = body["lap_time_seconds"],
        fuel_consumed_kg = body["fuel_consumed_kg"],
        max_speed_kph = body["max_speed_kph"],
        # date_created automatically created by db

        trace_id = body["trace_id"]
    )


    session.add(event)
    session.commit()


    session.close()


    logger.debug(f"Stored event add_lap with a trace id of {body['trace_id']}")


    return NoContent, 201



def add_sector(body):
    session = make_session()

    batch_timestamp_parsed = datetime.strptime(body["batch_timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")

    event = Sector(
        car_id = body["car_id"],
        batch_id = body["batch_id"],
        stint_id = body["stint_id"],
        batch_timestamp = batch_timestamp_parsed,
        lap_number = body["lap_number"],


        sector_id = body["sector_id"],
        sector_time_seconds = body["sector_time_seconds"],
        entry_speed_kph = body["entry_speed_kph"],
        exit_speed_kph = body["exit_speed_kph"],
        # date_created automatically created by db

        trace_id = body["trace_id"]
    )


    session.add(event)
    session.commit()


    session.close()


    logger.debug(f"Stored event add_sector with a trace id of {body['trace_id']}")


    return NoContent, 201


def get_lap_readings(start_timestamp, end_timestamp):
    session = make_session()

    # convert timestamp strings into datetime obj
    try:
        # This handles various lengths of milliseconds/microseconds
        start_ts_dt = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        end_ts_dt = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        # Fallback if the timestamp is missing milliseconds entirely (e.g. 10:00:00Z)
        start_ts_dt = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
        end_ts_dt = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")

    readings = session.query(Lap).filter(
        (Lap.date_created >= start_ts_dt) & (Lap.date_created < end_ts_dt) 
    )

    results = []
    for item in readings:
        results.append(item.to_dict())
    
    session.close()

    logger.debug("Found %d lap readings (start: %s, end: %s)", len(results), start_ts_dt, end_ts_dt)

    return results, 200


def get_sector_readings(start_timestamp, end_timestamp):
    session = make_session()

    # convert timestamp strings into datetime obj
    try:
        # This handles various lengths of milliseconds/microseconds
        start_ts_dt = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        end_ts_dt = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        # Fallback if the timestamp is missing milliseconds entirely (e.g. 10:00:00Z)
        start_ts_dt = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
        end_ts_dt = datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")

    readings = session.query(Sector).filter(
        (Sector.date_created >= start_ts_dt) & (Sector.date_created < end_ts_dt) 
    )

    results = []
    for item in readings:
        results.append(item.to_dict())
    
    session.close()

    logger.debug("Found %d sector readings (start: %s, end: %s)", len(results), start_ts_dt, end_ts_dt)

    return results, 200

# lab6
def process_messages():
    """ Process event messages with a retry loop """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    
    # We loop forever until we successfully connect to Kafka
    client = None
    while client is None:
        try:
            logger.info("Connecting to Kafka at %s...", hostname)
            client = KafkaClient(hosts=hostname)
            # Try to grab the topic
            topic = client.topics[str.encode(app_config['events']['topic'])]
        except Exception as e:
            logger.error("Kafka not ready yet... waiting 5 seconds. Error: %s", e)
            time.sleep(5)

    # Change auto_offset_reset to EARLIEST so you don't miss messages 
    # that were sent while the storage service was offline!
    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.EARLIEST)
    
    logger.info("Successfully connected to Kafka. Starting message loop...")

    for msg in consumer:
        try:
            msg_str = msg.value.decode('utf-8')
            msg_obj = json.loads(msg_str)
            logger.info("Message received: %s" % msg_obj)
            
            payload = msg_obj["payload"]

            if msg_obj["type"] == "lap":
                add_lap(payload)
            elif msg_obj["type"] == "sector":
                add_sector(payload)
                
            consumer.commit_offsets()
        except Exception as e:
            logger.error(f"Failed to process a specific message: {e}")

# lab 6
def setup_kafka_thread():
    t1 = threading.Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()



app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api( "openapi.yaml", base_path="/storage", strict_validation=True, validate_responses=True)



if __name__ == "__main__":
    setup_kafka_thread()
    app.run(host="0.0.0.0", port=8090)