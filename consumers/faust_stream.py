"""Defines trends calculations for stations"""
import logging
import faust


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


app = faust.App("stations-stream", broker="kafka://kafka:9092", store="memory://")

topic = app.topic("org.chicago.cta.trainstation.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.trainstation.stations-table", partitions=1)

transformed_station_table = app.Table(
   "TransformedStation",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic
)

def transform(station):
    if station is None:
        return None
    
    if isinstance(station, TransformedStation):
        return station
    
    line = "N/A"
    
    if station.red == True:
        line = "red"
    elif station.blue == True:
        line = "blue"
    elif station.green == True:
        line = "green"

    return TransformedStation(
        station_id=station.station_id,
        station_name=station.station_name,
        order=station.order,
        line=line)

@app.agent(topic)
async def process(stations):
    
    stations.add_processor(transform)

    async for event in stations:

        transformed_station_table[event.station_id] = event


if __name__ == "__main__":
    app.main()
