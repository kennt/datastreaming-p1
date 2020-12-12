"""Defines trends calculations for stations"""
import logging
import logging.config
from pathlib import Path

import faust

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

ORIGINAL_STATION_TOPIC="com.udacity.connect-stations"
TRANSFORMED_STATION_TOPIC="com.udacity.stations.transformed"

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic(ORIGINAL_STATION_TOPIC, value_type=Station)

out_topic = app.topic(TRANSFORMED_STATION_TOPIC, partitions=1, value_type=TransformedStation)

table = app.Table(
    "transformed-stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def station_event(stations):
    async for station in stations:
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        else:
            line = "green"
        transformed_station = TransformedStation(
                                station.station_id,
                                station.station_name,
                                station.order,
                                line
                                )
        table[station.station_id] = transformed_station

if __name__ == "__main__":
    app.main()
