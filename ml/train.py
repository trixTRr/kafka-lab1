# ml/train.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler
import joblib
import logging
import os
from datetime import datetime
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetector:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_cols = ['temperature', 'humidity', 'pressure', 'vibration',
                             'air_quality_co2', 'battery_level', 'signal_strength',
                             'hour', 'day_of_week', 'is_weekend']
        
    def load_data(self, data_path='data/processed/'):
        """Загрузка всех обработанных батчей"""
        all_files = glob.glob(os.path.join(data_path, 'batch_*.parquet'))
        
        if not all_files:
            logger.error(f"Нет файлов данных в {data_path}")
            return None
        
        logger.info(f"Найдено {len(all_files)} файлов")
        
        dfs = []
        for file in all_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
                logger.info(f"Загружен {file}: {df.shape}")
            except Exception as e:
                logger.error(f"Ошибка загрузки {file}: {e}")
        
        if not dfs:
            return None
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Всего загружено записей: {len(combined)}")
        
        return combined
    
    def prepare_features(self, df):
        """Подготовка признаков"""
        # Проверяем наличие всех признаков
        available_features = [col for col in self.feature_cols if col in df.columns]
        
        if len(available_features) < len(self.feature_cols):
            logger.warning(f"Отсутствуют признаки: "
                          f"{set(self.feature_cols) - set(available_features)}")
        
        X = df[available_features].copy()
        
        # Обработка пропусков
        X = X.fillna(X.median())
        
        return X, available_features
    
    def train(self, df, target_col='is_anomaly'):
        """Обучение модели"""
        logger.info("Начало обучения модели...")
        
        # Подготовка данных
        X, features = self.prepare_features(df)
        y = df[target_col].values
        
        logger.info(f"Размер X: {X.shape}, размер y: {y.shape}")
        logger.info(f"Распределение классов:\n{pd.Series(y).value_counts(normalize=True)}")
        
        # Разделение
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Масштабирование
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Обучение
        self.model = RandomForestClassifier(
            n_estimators=50,  # Меньше для скорости
            max_depth=10,
            random_state=42,
            n_jobs=-1,
            class_weight='balanced'
        )
        
        self.model.fit(X_train_scaled, y_train)
        
        # Оценка
        train_score = self.model.score(X_train_scaled, y_train)
        test_score = self.model.score(X_test_scaled, y_test)
        
        logger.info(f"Train accuracy: {train_score:.4f}")
        logger.info(f"Test accuracy: {test_score:.4f}")
        
        # ROC-AUC
        y_pred_proba = self.model.predict_proba(X_test_scaled)[:, 1]
        roc_auc = roc_auc_score(y_test, y_pred_proba)
        logger.info(f"ROC-AUC: {roc_auc:.4f}")
        
        # Cross-validation
        cv_scores = cross_val_score(self.model, X_train_scaled, y_train, cv=5)
        logger.info(f"CV scores: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
        
        # Важность признаков
        importance = pd.DataFrame({
            'feature': features,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        logger.info(f"\nВажность признаков:\n{importance}")
        
        return {
            'train_accuracy': train_score,
            'test_accuracy': test_score,
            'roc_auc': roc_auc,
            'cv_mean': cv_scores.mean(),
            'feature_importance': importance
        }
    
    def save_model(self, path='../models/'):
        """Сохранение модели"""
        os.makedirs(path, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        model_path = os.path.join(path, f'anomaly_detector_{timestamp}.pkl')
        scaler_path = os.path.join(path, f'scaler_{timestamp}.pkl')
        
        joblib.dump(self.model, model_path)
        joblib.dump(self.scaler, scaler_path)
        
        # Сохраняем как текущую версию
        joblib.dump(self.model, os.path.join(path, 'anomaly_detector.pkl'))
        joblib.dump(self.scaler, os.path.join(path, 'scaler.pkl'))
        
        logger.info(f"Модель сохранена: {model_path}")
        logger.info(f"Scaler сохранен: {scaler_path}")
        
        return model_path, scaler_path
    
    def load_model(self, model_path='../models/anomaly_detector.pkl', 
                   scaler_path='../models/scaler.pkl'):
        """Загрузка модели"""
        if os.path.exists(model_path):
            self.model = joblib.load(model_path)
            self.scaler = joblib.load(scaler_path)
            logger.info(f"Модель загружена из {model_path}")
            return True
        else:
            logger.error(f"Модель не найдена: {model_path}")
            return False
    
    def predict(self, X):
        """Предсказание"""
        if self.model is None:
            logger.error("Модель не загружена")
            return None
        
        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)

def main():
    detector = AnomalyDetector()
    
    # Загрузка данных
    df = detector.load_data()
    if df is None:
        logger.error("Нет данных для обучения")
        return
    
    # Обучение
    metrics = detector.train(df)
    
    # Сохранение
    detector.save_model()
    
    logger.info("Обучение завершено успешно!")

if __name__ == "__main__":
    main()