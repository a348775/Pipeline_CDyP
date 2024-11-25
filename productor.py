from confluent_kafka import Producer
import requests
import json
from datetime import datetime, timedelta
import random

# Configuración del productor
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Conecta con el servidor Kafka
}
producer = Producer(producer_config)

# Función para enviar mensajes a Kafka
def send_to_kafka(topic, message):
    try:
        producer.produce(topic, value=json.dumps(message))
        producer.flush()
        print(f"Mensaje enviado al topic {topic}: {message}")
    except Exception as e:
        print(f"Error al enviar mensaje: {e}")

# Generar una fecha aleatoria dentro del rango permitido para la API de la NASA
def get_random_date():
    start_date = datetime(1995, 6, 16)  # Inicio del archivo APOD
    end_date = datetime.now()  # Fecha actual
    random_days = random.randint(0, (end_date - start_date).days)  # Días aleatorios entre inicio y hoy
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime('%Y-%m-%d')  # Formato YYYY-MM-DD

# Obtener datos de la API de la NASA con fecha aleatoria
def get_nasa_data_random_date(api_key):
    random_date = get_random_date()
    url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}&date={random_date}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error en la API de NASA para la fecha {random_date}: {response.status_code}")
        return None

# Generar coordenadas aleatorias para la API de OpenWeather
def get_random_coords():
    lat = random.uniform(-90, 90)  # Latitud aleatoria
    lon = random.uniform(-180, 180)  # Longitud aleatoria
    return lat, lon

# Obtener datos de la API de OpenWeather con coordenadas aleatorias
def get_weather_data_random_coords(api_key):
    lat, lon = get_random_coords()
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        weather_data = response.json()
        # Verificar que la respuesta contenga una ciudad válida (nombre no vacío)
        if weather_data.get('name'):  # 'name' contiene el nombre de la ciudad
            return weather_data
        else:
            print(f"Sin ciudad válida para lat: {lat}, lon: {lon}. Intentando nuevamente...")
            return None
    else:
        print(f"Error en la API de OpenWeather para lat: {lat}, lon: {lon}: {response.status_code}")
        return None

# Main
if __name__ == "__main__":
    # Claves de las APIs
    NASA_API_KEY = "efaP4C9crgZnnMJ9yaaQWxfGAJ0ff97awerf2KY4"
    WEATHER_API_KEY = "720157430c802ffdaad915f79d0bfc42"

    # Enviar 10 datos de la NASA con fecha aleatoria
    for _ in range(10):
        nasa_data = get_nasa_data_random_date(NASA_API_KEY)
        if nasa_data:
            send_to_kafka("nasa-data", nasa_data)

    # Enviar 10 datos del clima con coordenadas aleatorias
    valid_city_count = 0  # Contador de ciudades válidas
    while valid_city_count < 10:
        weather_data = get_weather_data_random_coords(WEATHER_API_KEY)
        if weather_data:
            send_to_kafka("weather-data", weather_data)
            valid_city_count += 1  # Incrementar solo cuando se encuentra una ciudad válida
