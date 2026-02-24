import pandas as pd
import json
import time
import random
import logging
from datetime import datetime
from confluent_kafka import Producer
from config import KafkaProducerConfig
import os
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SensorDataProducer:
    def __init__(self, config_name='config_3_acks_all'):
        self.config = KafkaProducerConfig()
        self.producer_config = self.config.producer_configs[config_name]
        self.producer = Producer(self.producer_config)
        self.topic = self.config.topic
        self.delivered_records = 0
        
        logger.info(f"Producer инициализирован с конфигурацией: {config_name}")
        logger.info(f"Настройки: acks={self.producer_config.get('acks')}, "
                   f"batch.size={self.producer_config.get('batch.num.messages')}, "
                   f"linger.ms={self.producer_config.get('linger.ms')}")
    
    def delivery_report(self, err, msg):
        # Callback для подтверждения доставки
        if err is not None:
            logger.error(f'Ошибка доставки сообщения: {err}')
        else:
            self.delivered_records += 1
            if self.delivered_records % 1000 == 0:
                logger.info(f'Доставлено {self.delivered_records} сообщений')
    
    def send_message(self, message, key=None):
        # Отправка одного сообщения
        try:
            # Сериализуем сообщение в JSON
            message_json = json.dumps(message, default=str)
            
            # Отправляем
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=message_json,
                callback=self.delivery_report
            )
            
            # Триггерим отправку
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {e}")
    
    def send_batch(self, messages, batch_size=100):
        # Отправка батча сообщений
        for i, message in enumerate(messages):
            # Используем sensor_id как ключ для гарантии порядка
            key = message.get('sensor_id', str(i))
            self.send_message(message, key)
            
            # Флашим каждые batch_size сообщений
            if i > 0 and i % batch_size == 0:
                self.producer.flush()
                logger.info(f"Отправлен батч из {batch_size} сообщений")
    
    def flush(self):
        # Ожидание доставки всех сообщений
        self.producer.flush()
        logger.info(f"Всего доставлено сообщений: {self.delivered_records}")
    
    def simulate_stream(self, data_path, mode='random', fixed_delay=0.5):
        # Симуляция потока данных
        # mode: 'random' - случайная задержка, 'fixed' - фиксированная
        logger.info(f"Запуск симуляции потока в режиме: {mode}")
        
        # Загружаем данные
        if data_path.endswith('.parquet'):
            df = pd.read_parquet(data_path)
        else:
            df = pd.read_csv(data_path)
        
        logger.info(f"Загружено {len(df)} записей из {data_path}")
        
        # Конвертируем в список словарей
        records = df.to_dict('records')
        
        # Перемешиваем для большей реалистичности
        random.shuffle(records)
        
        message_count = 0
        start_time = time.time()
        
        try:
            for record in records:
                # Добавляем время отправки
                record['produced_at'] = datetime.now().isoformat()
                
                # Отправляем сообщение
                self.send_message(record, key=record['sensor_id'])
                message_count += 1
                
                # Задержка
                if mode == 'random':
                    delay = random.uniform(0.1, 2.0)
                else:
                    delay = fixed_delay
                
                time.sleep(delay)
                
                # Логируем прогресс
                if message_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = message_count / elapsed
                    logger.info(f"Отправлено {message_count} сообщений. "
                              f"Скорость: {rate:.2f} msg/sec")
                    
        except KeyboardInterrupt:
            logger.info("Прерывание пользователем")
        finally:
            self.flush()
            
        elapsed = time.time() - start_time
        logger.info(f"Симуляция завершена. Отправлено {message_count} сообщений за {elapsed:.2f} сек")

def demonstrate_different_configs():
    # Демонстрация работы с разными конфигурациями
    
    logger.info("=" * 60)
    logger.info("Демонстрация различных конфигураций Producer")
    logger.info("=" * 60)
    
    configs = ['config_1_acks_0', 'config_2_acks_1', 'config_3_acks_all']
    
    for config_name in configs:
        logger.info(f"\n--- Тестирование: {config_name} ---")
        producer = SensorDataProducer(config_name)
        
        # Отправляем тестовые сообщения
        test_messages = [
            {"sensor_id": "TEST1", "value": i, "timestamp": datetime.now().isoformat()}
            for i in range(100)
        ]
        
        start = time.time()
        producer.send_batch(test_messages, batch_size=10)
        producer.flush()
        elapsed = time.time() - start
        
        logger.info(f"Время отправки 100 сообщений: {elapsed:.4f} сек")
        logger.info(f"Доставлено: {producer.delivered_records}")

def main():
    # Проверяем аргументы командной строки
    config = sys.argv[1] if len(sys.argv) > 1 else 'config_3_acks_all'
    mode = sys.argv[2] if len(sys.argv) > 2 else 'random'
    
    # Создаем продюсера
    producer = SensorDataProducer(config)
    
    # Путь к данным
    data_path = '../kafka-lab1/data/datasets/sensor_data.parquet'
    
    if not os.path.exists(data_path):
        logger.error(f"Файл данных не найден: {data_path}")
        # Генерируем данные если файла нет
        from scripts.generate_data import main as generate
        generate()
    
    # Запускаем симуляцию
    producer.simulate_stream(data_path, mode=mode)

if __name__ == "__main__":
    main()