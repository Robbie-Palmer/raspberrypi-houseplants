import logging
import time

import busio
from adafruit_seesaw.seesaw import Seesaw
from board import SCL, SDA
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.error import SerializationError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from houseplants import CONFIGS
from houseplants.avro_helper import Reading, Mapping

# set up logging
logger = logging.getLogger('soil_monitor')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('soil_monitor.log')
fh.setLevel(logging.WARN)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.addHandler(fh)

TOUCH_HI = 1200
TOUCH_LO = 600

READINGS_TOPIC = 'houseplant-readings'
MAPPINGS_TOPIC = 'houseplant-sensor-mapping'

# set up schema registry
SR_CONF = {
    'url': CONFIGS['schema-registry']['schema.registry.url'],
    'basic.auth.user.info': CONFIGS['schema-registry']['basic.auth.user.info']
}
SR_CLIENT = SchemaRegistryClient(SR_CONF)

# set up Kafka producer
READINGS_SERIALIZER = AvroSerializer(
    schema_registry_client=SR_CLIENT,
    schema_str=Reading.schema,
    to_dict=Reading.reading_to_dict
)

PRODUCER_CONF = CONFIGS['kafka'].copy()
PRODUCER_CONF['value.serializer'] = READINGS_SERIALIZER
PRODUCER = SerializingProducer(PRODUCER_CONF)

# set up Kafka Consumer details
MAPPINGS_DESERIALIZER = AvroDeserializer(
    schema_registry_client=SR_CLIENT,
    schema_str=Mapping.schema,
    from_dict=Mapping.dict_to_mapping
)

CONSUMER_CONF = CONFIGS['kafka'].copy()
CONSUMER_CONF['value.deserializer'] = MAPPINGS_DESERIALIZER
CONSUMER_CONF['group.id'] = 'sensor-mapping-consumer'
CONSUMER_CONF['auto.offset.reset'] = 'earliest'
CONSUMER_CONF['enable.auto.commit'] = 'false'
CONSUMER = DeserializingConsumer(CONSUMER_CONF)
CONSUMER.subscribe([MAPPINGS_TOPIC])

PLANT_ADDRESSES = {}


def consume_sensor_mappings():
    # loop until there aren't any messages
    while True:
        try:
            msg = CONSUMER.poll(1.0)

            # if no more messages in mapping topic, then move on
            if msg is None and len(PLANT_ADDRESSES) != 0:
                return
            elif msg is not None:
                sensor_id = msg.value().sensor_id
                plant_id = msg.value().plant_id

                PLANT_ADDRESSES[sensor_id] = plant_id
        except SerializationError as e:
            # Report malformed record, discard results, continue polling
            logger.error("Message deserialization failed {}".format(e))
            pass
        except Exception as e:
            logger.error("Other exception {}".format(str(e)))


def produce_sensor_readings():
    i2c_bus = busio.I2C(SCL, SDA)
    for address, plant_id in PLANT_ADDRESSES.items():
        try:
            ss = Seesaw(i2c_bus, addr=int(address, 16))

            # read moisture 
            touch = ss.moisture_read()
            if touch < TOUCH_LO:
                touch = TOUCH_LO
            elif touch > TOUCH_HI:
                touch = TOUCH_HI

            touch_percent = (touch - TOUCH_LO) / (TOUCH_HI - TOUCH_LO) * 100

            # read temperature
            temp = ss.get_temp()

            # send data to Kafka
            ts = int(time.time())
            reading = Reading(int(plant_id), round(touch_percent, 3), round(temp, 3))

            logger.info("Publishing message: key, value: ({},{})".format(str(plant_id), reading))
            PRODUCER.produce(READINGS_TOPIC, key=plant_id, value=reading, timestamp=ts)
            PRODUCER.poll()

        except Exception as e:
            logger.error("Other exception {}".format(str(e)))


while True:
    consume_sensor_mappings()
    produce_sensor_readings()

    time.sleep(30)
