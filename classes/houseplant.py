from dataclasses import dataclass

@dataclass
class Houseplant:
	plant_id: int
	scientific_name: str
	common_name: str
	given_name: str
	temperature_low: float
	temperature_high: float
	moisture_low: float
	moisture_high: float

	@staticmethod
	def get_schema():
		with open('./avro/houseplant.avsc', 'r') as handle:
			return handle.read()


	@staticmethod
	def dict_to_houseplant(obj, ctx=None):
		return Houseplant(
			obj['plant_id'],
			obj['scientific_name'],
			obj['common_name'],
			obj['given_name'],
			obj['temperature_low'],
			obj['temperature_high'],
			obj['moisture_low'],
			obj['moisture_high']
			)


	@staticmethod
	def houseplant_to_dict(houseplant, ctx=None):
		return houseplant.to_dict()


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

