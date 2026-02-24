import os
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
import json

class KafkaConsumerConfig:
    def __init__(self, group_id='ml-consumer-group'):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092,localhost:29093,localhost:29094')
        self.topic = 'sensor-data'
        self.group_id = group_id
        
        # Разные стратегии auto.offset.reset для демонстрации
        self.offset_strategies = {
            'latest': 'Начинаем с самых новых сообщений',
            'earliest': 'Начинаем с самых старых сообщений',
            'none': 'Ошибка, если нет смещения'
        }
        
        self.current_strategy = os.getenv('AUTO_OFFSET_RESET', 'earliest')
        
        # Настройки consumer
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.current_strategy,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 3000,
            'max.partition.fetch.bytes': 1048576  # 1MB
        }
        
        # Конфигурации для демонстрации разных replication factor
        self.topic_configs = {
            'rf1_minisr1': {  # RF=1, minISR=1 - низкая надежность
                'replication_factor': 1,
                'min_insync_replicas': 1,
                'description': 'Одна реплика, нет отказоустойчивости'
            },
            'rf2_minisr1': {  # RF=2, minISR=1
                'replication_factor': 2,
                'min_insync_replicas': 1,
                'description': '2 реплики, подтверждение от 1'
            },
            'rf3_minisr2': {  # RF=3, minISR=2 - баланс
                'replication_factor': 3,
                'min_insync_replicas': 2,
                'description': '3 реплики, подтверждение от 2'
            },
            'rf3_minisr3': {  # RF=3, minISR=3 - макс надежность
                'replication_factor': 3,
                'min_insync_replicas': 3,
                'description': '3 реплики, подтверждение от всех'
            }
        }