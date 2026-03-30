import connexion
import yaml
import httpx
import os
import json
import logging
import logging.config
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


# .\venv\Scripts\Activate.ps1


with open("/config/processing_config.yaml", "r") as file:
    app_config = yaml.safe_load(file.read())


with open("/config/processing_log_config.yaml", "r") as file:
    LOG_CONFIG = yaml.safe_load(file.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


def populate_stats():
    logger.info("Start periodic processing")


    # load data.json
    filename = app_config['datastore']['filename']


    if os.path.isfile(filename):
        with open(filename, 'r') as file:
            stats = json.load(file)
    else:
        stats = {
            "num_laps": 0,
            "num_sectors": 0,
            "max_lap_speed": 0.00,
            "min_sector_time": 999.99,
            "last_updated": "2026-01-01T00:00:00.000Z"
        }


    # define timestamps for storage service call
    current_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    params = {
        "start_timestamp": stats['last_updated'],
        "end_timestamp": current_ts
    }


    # get data for both lap and sector
    lap_url = app_config["eventstores"]["lap_event"]["url"]
    sector_url = app_config["eventstores"]["sector_event"]["url"]

    lap_res = httpx.get(lap_url, params=params)
    sector_res = httpx.get(sector_url, params=params)


    # lap_res processing
    if lap_res.status_code != 200:
        logger.error(f"Lap resolution received status code:  {lap_res.status_code}")
    
    else:
        new_laps = lap_res.json()

        lap_count = len(new_laps)
        logger.info(f"Received {lap_count} new lap events")

        highest_speed = stats['max_lap_speed']

        for lap in new_laps:
            if lap["max_speed_kph"] > highest_speed:
                highest_speed = lap["max_speed_kph"]


        stats["num_laps"] = stats["num_laps"] + lap_count
        stats["max_lap_speed"] = highest_speed
    

    # sector_res processing
    if sector_res.status_code != 200:
        logger.error(f"Sector resolution received status code:  {sector_res.status_code}")
    else:
        new_sectors = sector_res.json()

        sector_count = len(new_sectors)
        logger.info(f"Received {sector_count} new sector events")

        fastest_sector_time = stats['min_sector_time']

        for sector in new_sectors:
            if sector["sector_time_seconds"] < fastest_sector_time:
                fastest_sector_time = sector["sector_time_seconds"]

        
        stats["num_sectors"] = stats["num_sectors"] + sector_count
        stats["min_sector_time"] = fastest_sector_time


    stats["last_updated"] = current_ts


    with open(filename, 'w') as file:
        json.dump(stats, file, indent=4)

    
    logger.debug(f"Updated stats values: {stats}")
    logger.info("End periodic processing")


def get_stats():
    logger.info("Request for statistics started")
    filename = app_config['datastore']['filename']


    if os.path.isfile(filename):
        with open(filename, 'r') as file:
            stats = json.load(file)
    else:
        logger.error("data.json file not found")
        return {"message": "Statistics do not exist"}, 404
    

    logger.debug(f"data.json contents: {stats}")


    logger.info("Request for statistics completed")
    return stats, 200


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)

    sched.add_job(populate_stats, 
        'interval', 
        seconds=app_config['scheduler']['interval'])
    
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api( "openapi.yaml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    init_scheduler()
    app.run(host="0.0.0.0",port=8100)

