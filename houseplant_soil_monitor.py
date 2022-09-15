import time
import json
import logging

from board import SCL, SDA
import busio
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from adafruit_seesaw.seesaw import Seesaw

import avro_helper

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

# fetches the configs from the available file
CONFIGS_FILE = './configs/configs.yaml'
CONFIGS = {}
with open(CONFIGS_FILE, 'r') as config_file:
    CONFIGS = yaml.load(config_file, Loader=yaml.CLoader)

# set up schema registry
SR_CONF = {
        'url': CONFIGS['schema-registry']['schema.registry.url'],
        'basic.auth.user.info': CONFIGS['schema-registry']['basic.auth.user.info']
}
SR_CLIENT = SchemaRegistryClient(SR_CONF)

# set up Kafka producer
READINGS_SERIALIZER = AvroSerializer(
        schema_registry_client = SR_CLIENT,
        schema_str = avro_helper.reading_schema,
        to_dict = avro_helper.Reading.reading_to_dict
)

PRODUCER_CONF = CONFIGS['kafka']
PRODUCER_CONF['value.serializer'] = READINGS_SERIALIZER
PRODUCER = SerializingProducer(PRODUCER_CONF)

# set up Kafka Consumer details
MAPPINGS_DESERIALZIER = AvroDeserializer(
        schema_registry_client = SR_CLIENT,
        schema_str = avro_helper.mapping_schema,
        to_dict = avro_helper.Mapping.mapping_to_dict
)

CONSUMER_CONF = CONFIGS['kafka']
CONSUMER_CONF['value.deserializer'] = MAPPINGS_DESERIALZIER
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
            if msg is None:
                break
            else:
                sensor_id = msg.key()
                plant_id = msg.value().get('plant_id')

                PLANT_ADDRESSES[sensor_id] = plant_id
        except SerializerError as e:
            # Report malformed record, discard results, continue polling
            logger.error("Message deserialization failed {}".format(e))
            pass
        except Exception as e:
            logger.error("Other exception {}".format(str(e)))


def produce_sensor_readings():
    i2c_bus = busio.I2C(SCL, SDA)
    for k,v in PLANT_ADDRESSES.items():
        try:
            ss = Seesaw(i2c_bus, addr=v)

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
            reading = avro_helper.Reading(int(k), ts, round(touch_percent, 3), round(temp, 3))

            logger.info("Publishing message: key, value: ({},{})".format(str(k), reading))
            PRODUCER.produce(READINGS_TOPIC, key=k, value=reading) 
            PRODUCER.poll()

        except Exception as e:
            logger.error("Other exception ".format(str(e)))


while True:
    consume_sensor_mappings()
    produce_sensor_readings()

    time.sleep(15)
