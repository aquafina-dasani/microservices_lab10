import os
import connexion
import json
import yaml
import logging
import logging.config
from pykafka import KafkaClient
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


# Load application configuration
with open("/config/analyzer_config.yaml", "r") as file:
    app_config = yaml.safe_load(file.read())

# Load logging configuration
with open("/config/analyzer_log_config.yaml", "r") as file:
    LOG_CONFIG = yaml.safe_load(file.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

def get_lap_reading(index):
    """ Get a lap reading from the history in Kafka """
    logger.info(f"Retrieving lap event at index {index}")
    
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    topic = client.topics[str.encode(app_config['events']['topic'])]
    
    # reset_offset_on_start=True ensures we read from the beginning of the queue
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    counter = 0
    for msg in consumer:
        message = msg.value.decode("utf-8")
        data = json.loads(message)

        # We only care about 'lap' events for this endpoint
        if data["type"] == "lap":
            if counter == index:
                logger.info(f"Found lap event at index {index}")
                return data["payload"], 200
            counter += 1

    logger.error(f"Could not find lap event at index {index}")
    return {"message": f"Not Found: No lap event at index {index}!"}, 404


def get_sector_reading(index):
    """ Get a sector reading from the history in Kafka """
    logger.info(f"Retrieving sector event at index {index}")
    
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    topic = client.topics[str.encode(app_config['events']['topic'])]
    
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    counter = 0
    for msg in consumer:
        message = msg.value.decode("utf-8")
        data = json.loads(message)

        # We only care about 'sector' events for this endpoint
        if data["type"] == "sector":
            if counter == index:
                logger.info(f"Found sector event at index {index}")
                return data["payload"], 200
            counter += 1

    logger.error(f"Could not find sector event at index {index}")
    return {"message": f"Not Found: No sector event at index {index}!"}, 404


def get_stats():
    """ Get the total count of each event type in the Kafka queue """
    logger.info("Retrieving event statistics from Kafka")
    
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    topic = client.topics[str.encode(app_config['events']['topic'])]
    
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    num_laps = 0
    num_sectors = 0

    for msg in consumer:
        message = msg.value.decode("utf-8")
        data = json.loads(message)

        if data["type"] == "lap":
            num_laps += 1
        elif data["type"] == "sector":
            num_sectors += 1

    stats = {
        "num_laps": num_laps,
        "num_sectors": num_sectors
    }

    logger.info(f"Calculated statistics: {stats}")
    return stats, 200


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", base_path="/analyzer", strict_validation=True, validate_responses=True)

if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
    app.add_middleware(
        CORSMiddleware,
        position=MiddlewarePosition.BEFORE_EXCEPTION,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

if __name__ == "__main__":
    # Running on 8110 so it doesn't conflict with receiver(8080), storage(8090), or processing(8100)
    app.run(host="0.0.0.0",port=8110)