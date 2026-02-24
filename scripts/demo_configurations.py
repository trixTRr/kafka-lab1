import subprocess
import time
import logging
import json
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConfigDemo:
    def __init__(self, bootstrap_servers='localhost:29092,localhost:29093,localhost:29094'):
        self.bootstrap_servers = bootstrap_servers
        self.admin = AdminClient({'bootstrap.servers': bootstrap_servers})
        
    def create_topic(self, name, num_partitions=3, replication_factor=3, min_isr=2):
        # Создание топика с заданными параметрами
        topic = NewTopic(
            name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config={
                'min.insync.replicas': str(min_isr)
            }
        )
        
        fs = self.admin.create_topics([topic])
        
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Топик {topic} создан: RF={replication_factor}, minISR={min_isr}")
            except Exception as e:
                logger.error(f"Ошибка создания топика {topic}: {e}")
    
    def test_producer_config(self, topic, acks='all', messages=1000):
        # Тестирование продюсера с разными acks
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': acks,
            'batch.num.messages': 100,
            'linger.ms': 5
        }
        
        producer = Producer(producer_config)
        
        start = time.time()
        success = 0
        errors = 0
        
        for i in range(messages):
            try:
                producer.produce(
                    topic,
                    key=str(i),
                    value=json.dumps({'id': i, 'timestamp': time.time()})
                )
                producer.poll(0)
                success += 1
            except Exception as e:
                errors += 1
                logger.error(f"Ошибка: {e}")
        
        producer.flush()
        elapsed = time.time() - start
        
        logger.info(f"Конфигурация acks={acks}:")
        logger.info(f"  Отправлено: {success}, ошибок: {errors}")
        logger.info(f"  Время: {elapsed:.3f} сек, скорость: {success/elapsed:.0f} msg/sec")
        
        return {'success': success, 'errors': errors, 'elapsed': elapsed}
    
    def demonstrate_all(self):
        # Демонстрация всех конфигураций
        
        logger.info("=" * 60)
        logger.info("ДЕМОНСТРАЦИЯ КОНФИГУРАЦИЙ KAFKA")
        logger.info("=" * 60)
        
        # 1. Разные replication factor
        logger.info("\n1. ТЕСТИРОВАНИЕ REPLICATION FACTOR")
        
        topics = [
            ('rf1-test', 1, 1),  # RF=1, minISR=1
            ('rf2-test', 2, 1),  # RF=2, minISR=1
            ('rf3-min2-test', 3, 2),  # RF=3, minISR=2
            ('rf3-min3-test', 3, 3),  # RF=3, minISR=3
        ]
        
        for topic_name, rf, min_isr in topics:
            self.create_topic(topic_name, replication_factor=rf, min_isr=min_isr)
            time.sleep(2)  # Даем время на создание
            
            # Тестируем запись с acks=all
            self.test_producer_config(topic_name, acks='all')
        
        # 2. Разные acks
        logger.info("\n2. ТЕСТИРОВАНИЕ РАЗНЫХ ACKS")
        
        # Создаем топик с RF=3, minISR=2
        self.create_topic('acks-test', replication_factor=3, min_isr=2)
        time.sleep(2)
        
        acks_configs = ['0', '1', 'all']
        results = []
        
        for acks in acks_configs:
            result = self.test_producer_config('acks-test', acks=acks)
            results.append(result)
        
        # 3. Анализ результатов
        logger.info("\n3. АНАЛИЗ РЕЗУЛЬТАТОВ")
        
        df = pd.DataFrame(results, index=acks_configs)
        logger.info("\nСравнение acks:")
        logger.info(df.to_string())
        
        logger.info("\nВЫВОДЫ:")
        logger.info("- acks=0: максимальная скорость, но риск потери данных")
        logger.info("- acks=1: баланс скорости и надежности")
        logger.info("- acks=all: максимальная надежность, минимальная скорость")
        logger.info("- Больший RF увеличивает надежность, но снижает скорость")
        logger.info("- minISR влияет на доступность при отказе брокеров")
        
        return df

def main():
    demo = KafkaConfigDemo()
    demo.demonstrate_all()

if __name__ == "__main__":
    main()