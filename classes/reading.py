from dataclasses import dataclass

@dataclass
class Reading:
	plant_id: int
	moisture: float
	temperature: float

	@staticmethod
	def get_schema():
		with open('./avro/reading.avsc', 'r') as handle:
			return handle.read()


	@staticmethod
	def dict_to_reading(obj, ctx=None):
		return Reading(
			obj['plant_id'],
			obj['moisture'],
			obj['temperature']
			)


	@staticmethod
	def reading_to_dict(reading, ctx=None):
		return reading.to_dict()


	def to_dict(self):
		return dict(
			plant_id    = self.plant_id,
			moisture    = self.moisture,
			temperature = self.temperature
			)
