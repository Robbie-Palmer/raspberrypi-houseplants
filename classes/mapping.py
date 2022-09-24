from dataclasses import dataclass

@dataclass
class Mapping:
	sensor_id: int
	plant_id: int

	@staticmethod
	def get_schema():
		with open('./avro/mapping.avsc', 'r') as handle:
			return handle.read()


	@staticmethod
	def dict_to_mapping(obj, ctx=None):
		return Mapping(
			obj['sensor_id'],
			obj['plant_id']
			)


	@staticmethod
	def mapping_to_dict(mapping, ctx=None):
		return mapping.to_dict()


	def to_dict(self):
		return dict(
			sensor_id = self.sensor_id,
			plant_id  = self.plant_id
			)
