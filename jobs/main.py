import os 
import random
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
import time
from datetime import datetime, timedelta


London_coordinates = {"latitude": 51.5074, "longitude": -0.1278}
Birmingham_coordinates = {"latitude": 52.4862, "longitude": -1.8904}

LATITUDE_INCREMENT = (Birmingham_coordinates["latitude"] - London_coordinates["latitude"]) / 100
LONGITUDE_INCREMENT = (Birmingham_coordinates["longitude"] - London_coordinates["longitude"]) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle-data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps-data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic-data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather-data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency-data")

start_time = datetime.now()
start_location = London_coordinates.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': round(random.uniform(0, 40), 2), 
        'direction': 'North-East',
        'vehicleType': vehicle_type,
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'camera_id': camera_id,
        'location': f"{location[0]},{location[1]}",
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedImageString',
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': f"{location[0]},{location[1]}",
        'temperature': round(random.uniform(-5, 26), 2),
        'weather_condition': random.choice(['Sunny', 'Rainy', 'Cloudy', 'Windy']),
        'precipitation': round(random.uniform(0, 25), 2),
        'wind_speed': round(random.uniform(0, 100), 2),
        'humidity': round(random.uniform(0, 100), 2),
        'air_quality_index': round(random.uniform(0, 500), 2),
    }

def generate_emergency_data(device_id, timestamp, location):
    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'incident_id': str(uuid.uuid4()),
        'timestamp': timestamp,
        'location': f"{location[0]},{location[1]}",
        'emergency_type': random.choice(['Accident', 'Medical', 'Fire', 'Police', 'None']),
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Details about the emergency incident.',
    }  

def simulate_vehicle_movement():
    global start_location   
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)
    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': f"{location['latitude']},{location['longitude']}",
        'speed': round(random.uniform(10, 40), 2),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fueltype': 'Hybrid'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.uuid4):
        return str(obj)
    raise TypeError(f"Type of {obj.__class__.__name__} not json serializable")

def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed: {err}")
    else:
        if msg.offset() % 5 == 0:
            print(f" {msg.topic()}: offset {msg.offset()}")

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']), 
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

def simulate_journey(producer, device_id):
    print(" Starting vehicle journey from London to Birmingham...")
   
    message_count = 0
    
    try:
        while True:
            vehicle_data = generate_vehicle_data(device_id)
            
            loc_parts = vehicle_data['location'].split(',')
            location_tuple = (float(loc_parts[0]), float(loc_parts[1]))
            
            gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
            traffic_camera_data = generate_traffic_camera_data(
                device_id, vehicle_data['timestamp'], location_tuple, camera_id='camera-hook'
            )
            weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], location_tuple)
            emergency_data = generate_emergency_data(device_id, vehicle_data['timestamp'], location_tuple)

            current_lat = location_tuple[0]
            current_lon = location_tuple[1]
            
            if (current_lat >= Birmingham_coordinates["latitude"] and
                current_lon >= Birmingham_coordinates["longitude"]):
                print(f"\n Vehicle reached Birmingham! Total messages: {message_count}")
                break
            
            produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
            produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
            produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
            produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
            produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)
            
            message_count += 5
            
            if message_count % 50 == 0:
                print(f" Location: {vehicle_data['location']} | Messages: {message_count}")

            time.sleep(5)
            
    except KeyboardInterrupt:
        print(f"\n Stopped. Total messages: {message_count}")


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f" Kafka Error: {err}"),
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-SmartCity-001')
    except Exception as e:
        print(f" Error: {e}")