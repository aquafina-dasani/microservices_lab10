import connexion
import httpx
import uuid
import yaml
import logging
import logging.config
import json # Addition for Lab 6
from datetime import datetime # Addition for Lab 6
from pykafka import KafkaClient # Addition for Lab 6
from connexion import NoContent


# .\venv\Scripts\Activate.ps1


HEADERS = {
    "Content-Type": "application/json"
        }


with open("/config/receiver_config.yaml", "r") as file:
    app_config = yaml.safe_load(file.read())


with open("/config/receiver_log_config.yaml", "r") as file:
    LOG_CONFIG = yaml.safe_load(file.read())
    logging.config.dictConfig(LOG_CONFIG)


logger = logging.getLogger("basicLogger")


client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
topic = client.topics[str.encode(app_config['events']['topic'])]
producer = topic.get_sync_producer()


# paths handled by connexion via the yaml file
def report_lap_batch(body):
    # this is just common stuff that will appear on aech lap
    context = {
        "car_id": body["car_id"],
        "batch_id": body["batch_id"],
        "stint_id": body["stint_id"],
        "batch_timestamp": body["batch_timestamp"],
        "trace_id": str(uuid.uuid4())
    }

    logger.info(f"Received event add_lap with a trace id of {context['trace_id']}")

    for lap in body["laps"]:
        # combine the common stuff with lap item key-pair stuff
        payload = {**context, **lap}

        msg = { 
            "type": "lap", 
            "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": payload 
        }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info(f"Response for event add_lap (id: {context['batch_id']}) has status 201 (via Kafka)")


    return NoContent, 201


def report_sector_batch(body):
    # this is just common stuff that will appear on aech sector item
    context = {
        "car_id": body["car_id"],
        "batch_id": body["batch_id"],
        "stint_id": body["stint_id"],
        "batch_timestamp": body["batch_timestamp"],
        "trace_id": str(uuid.uuid4())
    }

    logger.info(f"Received event add_sector with a trace id of {context['trace_id']}")

    for sector in body["sectors"]:
        # combine the common stuff with sector item key-pair stuff
        payload = {**context, **sector}


        msg = { 
            "type": "sector", 
            "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": payload 
        }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info(f"Response for event add_sector (id: {context['batch_id']}) has status 201 (via Kafka)")


    return NoContent, 201



app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api( "openapi.yaml", base_path="/receiver", strict_validation=True, validate_responses=True)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)