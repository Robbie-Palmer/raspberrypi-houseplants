def get_schema(file):
    with open(file, 'r') as handle:
        return handle.read()


class Houseplant:
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

    schema = get_schema('avro/houseplant.avsc')

    def __init__(self, plant_id, scientific_name, common_name, given_name,
                 temperature_low, temperature_high, moisture_low, moisture_high):
        self.plant_id = plant_id
        self.scientific_name = scientific_name
        self.common_name = common_name
        self.given_name = given_name
        self.temperature_low = temperature_low
        self.temperature_high = temperature_high
        self.moisture_low = moisture_low
        self.moisture_high = moisture_high

    def to_dict(self):
        return dict(
            plant_id=self.plant_id,
            scientific_name=self.scientific_name,
            common_name=self.common_name,
            given_name=self.given_name,
            temperature_low=self.temperature_low,
            temperature_high=self.temperature_high,
            moisture_low=self.moisture_low,
            moisture_high=self.moisture_high
        )


class Reading:
    """Reading stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "plant_id",
        "moisture",
        "temperature"
    ]

    schema = get_schema('avro/reading.avsc')

    def __init__(self, plant_id, moisture, temperature):
        self.plant_id = plant_id
        self.moisture = moisture
        self.temperature = temperature

    @staticmethod
    def dict_to_reading(obj, ctx=None):
        return Reading(
            obj['plant_id'],
            obj['moisture'],
            obj['temperature'],
        )

    def to_dict(self):
        return dict(
            plant_id=self.plant_id,
            moisture=self.moisture,
            temperature=self.temperature
        )


class Mapping:
    """Mapping stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "sensor_id",
        "plant_id"
    ]

    schema = get_schema('avro/mapping.avsc')

    def __init__(self, sensor_id, plant_id):
        self.sensor_id = sensor_id
        self.plant_id = plant_id

    @staticmethod
    def dict_to_mapping(obj, ctx=None):
        return Mapping(
            obj['sensor_id'],
            obj['plant_id']
        )

    def to_dict(self):
        return dict(
            sensor_id=self.sensor_id,
            plant_id=self.plant_id
        )
