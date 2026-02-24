import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import os

class SensorDataGenerator:
    def __init__(self, n_samples=500000):
        self.n_samples = n_samples
        self.sensor_ids = [f"SENSOR_{i:04d}" for i in range(1, 51)]  # 50 сенсоров
        
    def generate_temperature(self, base_temp=25.0, season_factor=None):
        # Генерация температуры с суточными и сезонными колебаниями
        if season_factor is None:
            season_factor = np.sin(np.linspace(0, 4*np.pi, self.n_samples))
        
        # Дневные колебания
        daily_pattern = np.sin(np.linspace(0, 100*np.pi, self.n_samples))
        
        # Шум измерений
        noise = np.random.normal(0, 0.5, self.n_samples)
        
        # Аномалии (редкие выбросы)
        anomalies = np.random.choice([0, 5, -5], self.n_samples, p=[0.98, 0.01, 0.01])
        
        temperature = base_temp + 10 * season_factor + 3 * daily_pattern + noise + anomalies
        return np.clip(temperature, -10, 50)  # Физически возможные значения
    
    def generate_humidity(self, base_humidity=60.0):
        # Генерация влажности
        # Влажность обратно пропорциональна температуре с шумом
        temp_factor = np.random.normal(0, 5, self.n_samples)
        humidity = base_humidity - 0.3 * temp_factor + np.random.normal(0, 3, self.n_samples)
        return np.clip(humidity, 10, 100)
    
    def generate_pressure(self, base_pressure=1013.25):
        # Атмосферное давление
        pressure = base_pressure + np.random.normal(0, 5, self.n_samples)
        return pressure
    
    def generate_vibration(self):
        # Вибрация оборудования
        # Большую часть времени вибрация низкая, иногда всплески
        base_vibration = np.random.exponential(0.1, self.n_samples)
        spikes = np.random.choice([0, 2, 5], self.n_samples, p=[0.95, 0.04, 0.01])
        return base_vibration + spikes
    
    def generate_air_quality(self):
        # Качество воздуха (CO2, VOC)
        co2_base = 400 + np.random.poisson(50, self.n_samples)
        # Пики при активности
        activity_peaks = np.random.choice([0, 200, 500], self.n_samples, p=[0.9, 0.08, 0.02])
        return co2_base + activity_peaks
    
    def generate_dataset(self):
        # Генерация полного датасета
        print(f"Генерация {self.n_samples} сэмплов...")
        
        # Временные метки
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        timestamps = [start_time + timedelta(seconds=i*2.6) for i in range(self.n_samples)]
        
        # Основные признаки
        season_factor = np.sin(np.linspace(0, 4*np.pi, self.n_samples))
        
        data = {
            'timestamp': timestamps,
            'sensor_id': [random.choice(self.sensor_ids) for _ in range(self.n_samples)],
            'temperature': self.generate_temperature(season_factor=season_factor),
            'humidity': self.generate_humidity(),
            'pressure': self.generate_pressure(),
            'vibration': self.generate_vibration(),
            'air_quality_co2': self.generate_air_quality(),
            'battery_level': np.random.uniform(0, 100, self.n_samples),
            'signal_strength': np.random.uniform(-120, -30, self.n_samples),
            'error_code': np.random.choice([0, 100, 200, 300, 404, 500], 
                                           self.n_samples, p=[0.95, 0.01, 0.01, 0.01, 0.01, 0.01])
        }
        
        df = pd.DataFrame(data)
        
        # Добавим целевую переменную: вероятность отказа оборудования
        # Чем выше температура, вибрация и CO2, тем выше риск
        risk_score = (
            0.3 * (df['temperature'] - 25) / 25 +
            0.4 * df['vibration'] / 5 +
            0.3 * (df['air_quality_co2'] - 400) / 500 +
            np.random.normal(0, 0.1, self.n_samples)
        )
        df['failure_probability'] = 1 / (1 + np.exp(-risk_score))
        df['is_anomaly'] = (df['failure_probability'] > 0.8).astype(int)
        
        return df

def main(): 
    # Генерируем датасет
    generator = SensorDataGenerator(n_samples=500000)
    df = generator.generate_dataset()
    
    # Сохраняем в разных форматах
    df.to_csv('../kafka-lab1/data/datasets/sensor_data.csv', index=False)
    df.to_parquet('../kafka-lab1/data/datasets/sensor_data.parquet', index=False)
    
    print(f"Датасет сохранен: {len(df)} записей")
    print("\nПервые 5 записей:")
    print(df.head())
    print("\nСтатистика:")
    print(df.describe())
    
    # Проверка размера
    print(f"\nРазмер датасета: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

if __name__ == "__main__":
    main()