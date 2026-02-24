docker-compose down -v
sudo rm -rf data/kafka1/*
sudo rm -rf data/kafka2/*
sudo rm -rf data/kafka3/*
docker-compose up -d kafka1
docker-compose up -d kafka2
docker-compose up -d kafka3
docker-compose up -d kafka-ui

echo "========================================="
echo "Запуск лабораторной работы Kafka"
echo "========================================="

# 1. Создание необходимых директорий
mkdir -p data/{raw,processed,datasets,kafka1,kafka2,kafka3}
mkdir -p models

# 2. Генерация данных
echo "Генерация тестовых данных..."
python3 scripts/generate_data.py

# 3. Запуск Kafka кластера
echo "Запуск Kafka кластера..."
docker-compose up -d

# 4. Ожидание готовности Kafka
echo "Ожидание готовности Kafka (30 секунд)..."
sleep 30
docker-compose exec kafka1 kafka-topics --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
# 5. Демонстрация конфигураций
echo "Демонстрация различных конфигураций..."
python3 scripts/demo_configurations.py

# 6. Запуск producer в разных режимах
echo "Запуск producer в режиме random..."
docker-compose up -d producer-app

# 7. Запуск consumer
echo "Запуск consumer..."
docker-compose up -d consumer-app

cp data/datasets/sensor_data.parquet data/processed/batch_001.parquet

# 8. Обучение модели
echo "Обучение ML модели..."
docker-compose up ml-trainer

# 9. Запуск dashboard
echo "Запуск dashboard..."
docker-compose up -d dashboard

echo "========================================="
echo "Лабораторная работа запущена!"
echo "Kafka UI: http://localhost:8080"
echo "Dashboard: http://localhost:8501"
echo "========================================="

# 10. Просмотр логов
echo "Хотите посмотреть логи? (y/n)"
read -n 1 answer
if [ "$answer" = "y" ]; then
    docker-compose logs -f
fi