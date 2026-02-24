import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib
import logging
import os
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetector:
    def __init__(self):
        self.model = None
        self.feature_cols = ['temperature', 'humidity', 'pressure', 'vibration',
                             'air_quality_co2', 'battery_level', 'signal_strength']
        
    def load_data(self, data_path='data/processed/'):
        all_files = glob.glob('data/processed/*.parquet')
        logger.info(f"Найдено {len(all_files)} файлов")
        
        if not all_files:
            logger.error("Нет файлов данных")
            return None
        
        dfs = []
        for file in all_files:
            df = pd.read_parquet(file)
            logger.info(f"Загружен {file}: {df.shape}")
            dfs.append(df)
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Всего загружено записей: {len(combined)}")
        return combined
    
    def prepare_features(self, df):
        # Создаем временные признаки если есть timestamp
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
            
            # Добавляем новые признаки
            extended_features = self.feature_cols + ['hour', 'day_of_week', 'is_weekend']
        else:
            extended_features = self.feature_cols
        
        # Выбираем доступные признаки
        available_features = [col for col in extended_features if col in df.columns]
        logger.info(f"Доступные признаки: {available_features}")
        
        X = df[available_features].fillna(0)
        return X, available_features
    
    def train(self, df, target_col='is_anomaly'):
        logger.info("Начало обучения модели...")
        
        X, features = self.prepare_features(df)
        
        if target_col in df.columns:
            y = df[target_col].values
        else:
            logger.warning(f"Колонка {target_col} не найдена, создаем случайные метки")
            y = np.random.choice([0, 1], len(df), p=[0.95, 0.05])
        
        logger.info(f"Размер X: {X.shape}, размер y: {y.shape}")
        
        # Проверка распределения классов
        unique_classes = np.unique(y)
        logger.info(f"Уникальные классы: {unique_classes}")
        logger.info(f"Распределение:\n{pd.Series(y).value_counts(normalize=True)}")
        
        # Если только один класс, создаем синтетические аномалии
        if len(unique_classes) == 1:
            logger.warning("Только один класс! Создаю синтетические аномалии...")
            y = y.copy()
            n_anomalies = int(len(y) * 0.05)  # 5% аномалий
            anomaly_indices = np.random.choice(len(y), n_anomalies, replace=False)
            y[anomaly_indices] = 1 - y[anomaly_indices]
            logger.info(f"Новое распределение:\n{pd.Series(y).value_counts(normalize=True)}")
        
        # Разделение на train/test
        stratify = y if len(np.unique(y)) > 1 else None
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=stratify
        )
        
        # Обучение
        self.model = RandomForestClassifier(
            n_estimators=50,
            max_depth=10,
            random_state=42,
            n_jobs=-1,
            class_weight='balanced'
        )
        
        self.model.fit(X_train, y_train)
        
        # Оценка
        train_pred = self.model.predict(X_train)
        test_pred = self.model.predict(X_test)
        
        train_acc = accuracy_score(y_train, train_pred)
        test_acc = accuracy_score(y_test, test_pred)
        
        logger.info(f"Train accuracy: {train_acc:.4f}")
        logger.info(f"Test accuracy: {test_acc:.4f}")
        
        # Classification report если есть два класса
        if len(np.unique(y)) > 1:
            logger.info("\n" + classification_report(y_test, test_pred))
        
        # Важность признаков
        importance = pd.DataFrame({
            'feature': features,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        logger.info(f"\nВажность признаков:\n{importance}")
        
        return self.model
    
    def save_model(self, path='models/'):
        os.makedirs(path, exist_ok=True)
        joblib.dump(self.model, os.path.join(path, 'anomaly_detector.pkl'))
        logger.info(f"Модель сохранена в {path}anomaly_detector.pkl")

def main():
    detector = AnomalyDetector()
    
    df = detector.load_data()
    if df is None:
        logger.info("Создание синтетических данных...")
        n_samples = 10000
        df = pd.DataFrame({
            'temperature': np.random.normal(25, 5, n_samples),
            'humidity': np.random.normal(60, 10, n_samples),
            'pressure': np.random.normal(1013, 10, n_samples),
            'vibration': np.random.exponential(0.1, n_samples),
            'air_quality_co2': np.random.normal(400, 50, n_samples),
            'battery_level': np.random.uniform(0, 100, n_samples),
            'signal_strength': np.random.uniform(-120, -30, n_samples),
            'timestamp': pd.date_range('2024-01-01', periods=n_samples, freq='1min'),
            'is_anomaly': np.random.choice([0, 1], n_samples, p=[0.95, 0.05])
        })
    
    detector.train(df)
    detector.save_model()
    
    logger.info("Обучение завершено успешно!")

if __name__ == "__main__":
    main()