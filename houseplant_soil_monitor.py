import time
import logging

from board import SCL, SDA
import busio
from adafruit_seesaw.seesaw import Seesaw

from confluent_kafka.error import SerializationError

from helpers import avro,clients,logging

TOUCH_HI = 1200
TOUCH_LO = 600

logger = logging.set_logging('soil_monitor')
config = clients.config()


def consume_sensor_mappings(consumer, plant_addresses):
    while True:
        try:
            msg = consumer.poll(1.0)

            if msg is None and len(plant_addresses) != 0:
                # implies no more messages in mapping topic, return current mapping dict
                return plant_addresses
            elif msg is not None:
                # received good record, updating mapping
                sensor_id = msg.value().sensor_id
                plant_id = msg.value().plant_id
                plant_addresses[sensor_id] = plant_id
                continue
            else:
                # empty poll, might be at the beginning of the consumer's lifecycle
                continue
        except SerializationError as e:
            # report malformed record, discard results, continue polling 
            logger.error("Message deserialization failed %s", e)
            continue



def produce_sensor_readings(producer, plant_addresses):
    i2c_bus = busio.I2C(SCL, SDA)
    for address,plant_id in plant_addresses.items():
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
            reading = avro_helper.Reading(int(plant_id), round(touch_percent, 3), round(temp, 3))

            logger.info("Publishing message: key, value: ({},{})".format(str(plant_id), reading))
            producer.produce(config['topics']['readings'], key=plant_id, value=reading, timestamp=ts) 
        except Exception as e:
            logger.error("Got exception %s", e)
        finally:
            producer.poll()
            producer.flush()


if __name__ == '__main__':
    # set up Kafka Producer for Readings
    producer = clients.producer(clients.readings_serializer())

    # set up Kafka Consumer for Mappings
    consumer = clients.consumer(clients.mappings_deserializer(), 'sensor-mapping-consumer', [config['topics']['mappings']])

    plant_addresses = {}

    # start readings capture loop
    try:
        while True:
            # attempt to fetch new plant-sensor mappings
            plant_addresses = consume_sensor_mappings(consumer, plant_addresses)

            # capture readings from sensors
            #produce_sensor_readings(producer, plant_addresses)

            time.sleep(30)
    finally:
        consumer.close()
        producer.flush()
