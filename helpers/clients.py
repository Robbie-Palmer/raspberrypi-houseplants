from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.error import SerializationError
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from helpers import avro

import yaml

READINGS_TOPIC = 'houseplant-readings'
METADATA_TOPIC = 'houseplant-metadata'
MAPPING_TOPIC  = 'houseplant-sensor-mapping'


def config():
	# fetches the configs from the available file
	with open('./config/config.yaml', 'r') as config_file:
		config = yaml.load(config_file, Loader=yaml.CLoader)

		return config


def sr_client():
	# set up schema registry
	sr_conf = config()['schema-registry']
	sr_client = SchemaRegistryClient(sr_conf)

	return sr_client


def readings_serializer():
	return AvroSerializer(
		schema_registry_client = sr_client(),
		schema_str = avro.reading_schema,
		to_dict = avro.Reading.reading_to_dict
		)


def mappings_serializer():
	return AvroSerializer(
		schema_registry_client = sr_client(),
		schema_str = avro.mapping_schema,
		to_dict = avro.Mapping.mapping_to_dict
		)


def mappings_deserializer():
	return AvroDeserializer(
		schema_registry_client = sr_client(),
		schema_str = avro.mapping_schema,
		from_dict = avro.Mapping.dict_to_mapping
		)


def houseplant_serializer():
	return AvroSerializer(
		schema_registry_client = schema_registry_client,
		schema_str = avro.houseplant_schema,
		to_dict = avro.Houseplant.houseplant_to_dict
		)


def producer(value_serializer):
	producer_conf = config()['kafka'] | { 'value.serializer': value_serializer }
	return SerializingProducer(producer_conf)


def consumer(value_deserializer, group_id, topics):
	consumer_conf = config()['kafka'] | {'value.deserializer': value_deserializer,
										  'group.id': group_id,
										  'auto.offset.reset': 'earliest',
										  'enable.auto.commit': 'false'
										  }

	consumer = DeserializingConsumer(consumer_conf)
	consumer.subscribe(topics)

	return consumer