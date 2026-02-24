from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import sys
from config import KafkaConsumerConfig
from preprocessing import DataPreprocessor
import threading
import signal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SensorDataConsumer:
    def __init__(self, config_strategy='earliest', group_id='ml-consumer-group'):
        self.config = KafkaConsumerConfig(group_id)
        self.config.current_strategy = config_strategy
        self.consumer = Consumer(self.config.consumer_config)
        self.topic = self.config.topic
        self.running = True
        self.preprocessor = DataPreprocessor()
        
        # Статистика
        self.messages_processed = 0
        self.batches_processed = 0
        self.start_time = None
        
        logger.info(f"Consumer инициализирован с group.id={group_id}, "
                   f"auto.offset.reset={config_strategy}")
        
    def subscribe(self):
        # Подписка на топик
        self.consumer.subscribe([self.topic])
        logger.info(f"Подписан на топик: {self.topic}")
        self.start_time = datetime.now()
        
    def process_message(self, msg):
        # Обработка одного сообщения
        try:
            # Декодируем сообщение
            value = msg.value()
            if value is None:
                return None
            
            data = json.loads(value.decode('utf-8'))
            
            # Добавляем метаданные
            data['partition'] = msg.partition()
            data['offset'] = msg.offset()
            data['consumed_at'] = datetime.now().isoformat()
            
            return data
            
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
            return None
    
    def process_batch(self, messages, batch_size=1000):
        # Обработка батча сообщений
        if not messages:
            return None
        
        # Конвертируем в DataFrame
        df = pd.DataFrame(messages)
        
        # Применяем предобработку
        df_processed = self.preprocessor.preprocess(df)
        
        # Сохраняем обработанные данные
        self.save_batch(df_processed)
        
        self.batches_processed += 1
        self.messages_processed += len(messages)
        
        # Логируем прогресс
        if self.batches_processed % 10 == 0:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            rate = self.messages_processed / elapsed if elapsed > 0 else 0
            logger.info(f"Обработано батчей: {self.batches_processed}, "
                       f"сообщений: {self.messages_processed}, "
                       f"скорость: {rate:.2f} msg/sec")
        
        return df_processed
    
    def save_batch(self, df):
        # Сохранение батча данных
        
        # Имя файла с временной меткой
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'../kafka-lab1/data/processed/batch_{timestamp}_{self.batches_processed}.parquet'
        
        # Сохраняем
        df.to_parquet(filename, index=False)
        logger.info(f"Сохранен батч {self.batches_processed} в {filename}")
    
    def consume(self, max_messages=None, batch_size=1000):
        # Основной цикл потребления сообщений
        self.subscribe()
        
        batch_messages = []
        
        try:
            while self.running:
                # Получаем сообщение
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Достигнут конец партиции {msg.partition()}")
                    else:
                        logger.error(f"Ошибка consumer: {msg.error()}")
                    continue
                
                # Обрабатываем сообщение
                processed = self.process_message(msg)
                if processed:
                    batch_messages.append(processed)
                
                # Если набрали батч - обрабатываем
                if len(batch_messages) >= batch_size:
                    self.process_batch(batch_messages)
                    batch_messages = []
                
                # Проверяем лимит сообщений
                if max_messages and self.messages_processed >= max_messages:
                    logger.info(f"Достигнут лимит сообщений: {max_messages}")
                    break
                    
        except KeyboardInterrupt:
            logger.info("Прерывание пользователем")
        finally:
            # Обрабатываем остаток
            if batch_messages:
                self.process_batch(batch_messages)
            
            # Закрываем consumer
            self.consumer.close()
            logger.info(f"Consumer закрыт. Всего обработано: {self.messages_processed}")
    
    def stop(self):
        # Остановка consumer
        self.running = False

def demonstrate_offset_strategies():
    # Демонстрация разных стратегий auto.offset.reset
    
    strategies = ['latest', 'earliest', 'none']
    
    for strategy in strategies:
        logger.info(f"\n=== Тестирование стратегии: {strategy} ===")
        
        # Создаем consumer с новой группой для чистой демонстрации
        group_id = f"demo-group-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        consumer = SensorDataConsumer(strategy, group_id)
        
        try:
            # Потребляем несколько сообщений
            consumer.consume(max_messages=10)
        except Exception as e:
            logger.error(f"Ошибка при стратегии {strategy}: {e}")
        
        logger.info(f"Стратегия {strategy}: {consumer.config.offset_strategies[strategy]}")

def main():
    # Получаем стратегию из аргументов
    strategy = sys.argv[1] if len(sys.argv) > 1 else 'earliest'
    max_messages = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    # Создаем и запускаем consumer
    consumer = SensorDataConsumer(strategy)
    
    # Обработка сигнала для graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Получен сигнал остановки")
        consumer.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Запускаем потребление
    consumer.consume(max_messages=max_messages)

if __name__ == "__main__":
    main()