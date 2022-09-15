import argparse, sys
from confluent_kafka import avro, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4

houseplant_schema = """
{ 
    "name": "metadata",
    "namespace": "com.houseplants",
    "type": "record",
    "doc": "Houseplant metadata.",
    "fields": [
        {
            "doc": "Unique plant identification number.",
            "name": "plant_id",
            "type": "int"
        },
        {
            "doc": "Scientific name of the plant.",
            "name": "scientific_name",
            "type": "string"
        },
        {
            "doc": "The common name of the plant.",
            "name": "common_name",
            "type": "string"
        },
        {
            "doc": "The given name of the plant.",
            "name": "given_name",
            "type": "string"
        },
        {
            "doc": "Lowest temperature of the plant.",
            "name": "temperature_low",
            "type": "float"
        },
        {
            "doc": "Highest temperature of the plant.",
            "name": "temperature_high",
            "type": "float"
        },
        {
            "doc": "Lowest moisture of the plant.",
            "name": "moisture_low",
            "type": "float"
        },
        {
            "doc": "Highest moisture of the plant.",
            "name": "moisture_high",
            "type": "float"
        }
    ]
}
"""

class Houseplant(object):
    """Houseplant stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "plant_id", 
        "scientific_name",
        "common_name",
        "given_name",
        "temperature_low",
        "temperature_high",
        "moisture_low",
        "moisture_high"
    ]
    
    def __init__(self, plant_id, scientific_name, common_name, given_name, 
                       temperature_low, temperature_high, moisture_low, moisture_high):
        self.plant_id         = plant_id
        self.scientific_name  = scientific_name
        self.common_name      = common_name
        self.given_name       = given_name
        self.temperature_low  = temperature_low
        self.temperature_high = temperature_high
        self.moisture_low     = moisture_low
        self.moisture_high    = moisture_high

    @staticmethod
    def dict_to_houseplant(obj):
        return Houseplant(
                obj['plant_id'],
                obj['scientific_name'],
                obj['common_name'],    
                obj['given_name'],    
                obj['temperature_low'],    
                obj['temperature_high'],    
                obj['moisture_low'],    
                obj['moisture_high'],    
            )

    @staticmethod
    def houseplant_to_dict(houseplant, ctx):
        return Houseplant.to_dict(houseplant)

    def to_dict(self):
        return dict(
                    plant_id         = self.plant_id, 
                    scientific_name  = self.scientific_name,
                    common_name      = self.common_name,
                    given_name       = self.given_name,
                    temperature_low  = self.temperature_low,
                    temperature_high = self.temperature_high,
                    moisture_low     = self.moisture_low,
                    moisture_high    = self.moisture_high
                )

reading_schema = """
{
    "name": "reading",
    "namespace": "com.houseplants",
    "type": "record",
    "doc": "Houseplant reading taken from meters.",
    "fields": [
        {
            "doc": "Unique plant identification number.",
            "name": "plant_id",
            "type": "int"
        },
        {
            "doc": "Soil moisture as a percentage.",
            "name": "moisture",
            "type": "float"
        },
        {
            "doc": "Temperature in degrees C of the soil of this plant.",
            "name": "temperature",
            "type": "float"
        }
    ]
}
"""

class Reading(object):
    """Reading stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "plant_id", 
        "moisture",
        "temperature"
    ]
    
    def __init__(self, plant_id, moisture, temperature):
        self.plant_id    = plant_id
        self.moisture    = moisture
        self.temperature = temperature

    @staticmethod
    def dict_to_reading(obj):
        return Reading(
                obj['plant_id'],
                obj['moisture'],    
                obj['temperature'],    
            )

    @staticmethod
    def reading_to_dict(reading, ctx):
        return Reading.to_dict(reading)

    def to_dict(self):
        return dict(
                    plant_id    = self.plant_id,
                    moisture    = self.moisture,
                    temperature = self.temperature
                )


mapping_schema = """
{
    "name": "mapping",
    "namespace": "com.houseplants",
    "type": "record",
    "doc": "Sensor-houseplant mapping.",
    "fields": [
        {
            "doc": "Hardcoded ID of the physical soil sensor.",
            "name": "sensor_id",
            "type": "string"
        },
        {
            "doc": "Plant identification number.",
            "name": "plant_id",
            "type": "int"
        }
    ]
}
"""


class Mapping(object):
    """Mapping stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "sensor_id", 
        "plant_id"
    ]
    
    def __init__(self, sensor_id, plant_id):
        self.sensor_id = sensor_id
        self.plant_id  = plant_id

    @staticmethod
    def dict_to_mapping(obj):
        return Mapping(
                obj['sensor_id'],
                obj['plant_id']   
            )

    @staticmethod
    def mapping_to_dict(mapping, ctx):
        return Mapping.to_dict(mapping)

    def to_dict(self):
        return dict(
                    sensor_id = self.sensor_id,
                    plant_id  = self.plant_id
                )



def read_ccloud_config(config_file):
    """Read Confluent Cloud configuration for librdkafka clients"""
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


def pop_schema_registry_params_from_config(conf):
    """Remove potential Schema Registry related configurations from dictionary"""
    conf.pop('schema.registry.url', None)
    conf.pop('basic.auth.user.info', None)
    conf.pop('basic.auth.credentials.source', None)
    
    return conf