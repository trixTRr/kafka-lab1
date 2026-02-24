import os
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
import time
import random

class KafkaProducerConfig:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092,localhost:29093,localhost:29094')
        self.topic = 'sensor-data'
        self.client_id = 'sensor-producer-1'
        
        # Настройки продюсера для демонстрации разных конфигураций
        self.producer_configs = {
            'config_1_acks_0': {  # Максимальная скорость, риск потери
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': self.client_id,
                'acks': 0,
                'batch.num.messages': 1000,
                'linger.ms': 5,
                'compression.type': 'snappy',
                'max.in.flight.requests.per.connection': 5
            },
            'config_2_acks_1': {  # Баланс скорости и надежности
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': self.client_id,
                'acks': 1,
                'batch.num.messages': 500,
                'linger.ms': 10,
                'compression.type': 'lz4',
                'max.in.flight.requests.per.connection': 3
            },
            'config_3_acks_all': {  # Максимальная надежность
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': self.client_id,
                'acks': 'all',
                'batch.num.messages': 100,
                'linger.ms': 20,
                'compression.type': 'zstd',
                'max.in.flight.requests.per.connection': 1,
                'enable.idempotence': True
            }
        }
        
        # Текущая активная конфигурация
        active_config = os.getenv('PRODUCER_CONFIG', 'config_3_acks_all')
        self.current_config = self.producer_configs[active_config]
        
        self.schema = {
            "type": "record",
            "name": "SensorData",
            "fields": [
                {"name": "timestamp", "type": "string"},
                {"name": "sensor_id", "type": "string"},
                {"name": "temperature", "type": "float"},
                {"name": "humidity", "type": "float"},
                {"name": "pressure", "type": "float"},
                {"name": "vibration", "type": "float"},
                {"name": "air_quality_co2", "type": "float"},
                {"name": "battery_level", "type": "float"},
                {"name": "signal_strength", "type": "float"},
                {"name": "error_code", "type": "int"},
                {"name": "failure_probability", "type": "float"},
                {"name": "is_anomaly", "type": "int"}
            ]
        }