"""Defines a Tornado Server that consumes Kafka Event data for display"""
import logging
import logging.config
from pathlib import Path
import tornado.ioloop
import tornado.template
import tornado.web


# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")


from consumer import KafkaConsumer
from models import Lines, Weather
from topic_check import topic_exists


logger = logging.getLogger(__name__)

topic_names = {
    "weather" : "org.chicago.cta.weather",
    "arrivals" : "org.chicago.cta.trainstation.arrivals",
    "stations" : "org.chicago.cta.trainstation.stations",
    "riders" : "org.chicago.cta.trainstation.riders",
    "stations_table" : "org.chicago.cta.trainstation.stations-table",
    "riders_summary_table" : "org.chicago.cta.trainstation.riders-summary-table"
}

class MainHandler(tornado.web.RequestHandler):
    """Defines a web request handler class"""

    template_dir = tornado.template.Loader(f"{Path(__file__).parents[0]}/templates")
    template = template_dir.load("status.html")

    def initialize(self, weather, lines):
        """Initializes the handler with required configuration"""
        self.weather = weather
        self.lines = lines

    def get(self):
        """Responds to get requests"""
        logging.debug("rendering and writing handler template")
        self.write(
            MainHandler.template.generate(weather=self.weather, lines=self.lines)
        )


def run_server():
    """Runs the Tornado Server and begins Kafka consumption"""
    if topic_exists(topic_names["riders_summary_table"]) is False:
        logger.fatal(
            "Ensure that the KSQL Command has run successfully before running the web server!"
        )
        exit(1)
    if topic_exists(topic_names["stations_table"]) is False:
        logger.fatal(
            "Ensure that Faust Streaming is running successfully before running the web server!"
        )
        exit(1)

    weather_model = Weather()
    lines = Lines()

    application = tornado.web.Application(
        [(r"/", MainHandler, {"weather": weather_model, "lines": lines})]
    )
    application.listen(5000)

    # Build kafka consumers
    consumers = [
        KafkaConsumer(
            topic_names["weather"],
            weather_model.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
            topic_names["stations_table"],
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        ),
        KafkaConsumer(
            topic_names["arrivals"],
            lines.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
            topic_names["riders_summary_table"],
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        ),
    ]

    try:
        logger.info(
            "Open a web browser to http://localhost:8888 to see the Transit Status Page"
        )
        for consumer in consumers:
            tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)

        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        for consumer in consumers:
            consumer.close()


if __name__ == "__main__":
    run_server()
