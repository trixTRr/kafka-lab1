import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
import logging

logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.is_fitted = False
        
    def preprocess(self, df):
        # Основной метод предобработки
        logger.info(f"Предобработка датафрейма: {df.shape}")
        
        # Копируем, чтобы не изменять оригинал
        df_processed = df.copy()
        
        # 1. Обработка временных меток
        df_processed = self._process_timestamps(df_processed)
        
        # 2. Обработка пропущенных значений
        df_processed = self._handle_missing(df_processed)
        
        # 3. Удаление дубликатов
        df_processed = self._remove_duplicates(df_processed)
        
        # 4. Обработка выбросов
        df_processed = self._handle_outliers(df_processed)
        
        # 5. Нормализация числовых признаков
        df_processed = self._normalize_features(df_processed)
        
        # 6. Кодирование категориальных признаков
        df_processed = self._encode_categorical(df_processed)
        
        # 7. Создание новых признаков
        df_processed = self._create_features(df_processed)
        
        logger.info(f"Предобработка завершена. Итоговый размер: {df_processed.shape}")
        
        return df_processed
    
    def _process_timestamps(self, df):
        # Обработка временных меток
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            df['day_of_month'] = df['timestamp'].dt.day
            df['month'] = df['timestamp'].dt.month
            df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        if 'produced_at' in df.columns:
            df['produced_at'] = pd.to_datetime(df['produced_at'])
            df['processing_delay'] = (df['consumed_at'] - df['produced_at']).dt.total_seconds()
        
        return df
    
    def _handle_missing(self, df):

        # Проверяем пропуски
        missing = df.isnull().sum()
        if missing.sum() > 0:
            logger.info(f"Найдены пропуски: {missing[missing > 0]}")
            
            # Для числовых признаков заполняем медианой
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if df[col].isnull().any():
                    df[col].fillna(df[col].median(), inplace=True)
            
            # Для категориальных - модой
            categorical_cols = df.select_dtypes(include=['object']).columns
            for col in categorical_cols:
                if df[col].isnull().any():
                    df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'UNKNOWN', inplace=True)
        
        return df
    
    def _remove_duplicates(self, df):
        # Удаление дубликатов
        initial_len = len(df)
        df = df.drop_duplicates(subset=['sensor_id', 'timestamp'], keep='last')
        
        if len(df) < initial_len:
            logger.info(f"Удалено дубликатов: {initial_len - len(df)}")
        
        return df
    
    def _handle_outliers(self, df):
        # Обработка выбросов методом IQR
        numeric_cols = ['temperature', 'humidity', 'pressure', 'vibration', 
                        'air_quality_co2', 'battery_level', 'signal_strength']
        
        for col in numeric_cols:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)].shape[0]
                if outliers > 0:
                    logger.info(f"В колонке {col} найдено выбросов: {outliers}")
                    
                    # Заменяем выбросы на границы
                    df[col] = df[col].clip(lower_bound, upper_bound)
        
        return df
    
    def _normalize_features(self, df):
        # Нормализация числовых признаков
        numeric_cols = ['temperature', 'humidity', 'pressure', 'vibration', 
                        'air_quality_co2', 'battery_level', 'signal_strength']
        
        existing_cols = [col for col in numeric_cols if col in df.columns]
        
        if existing_cols:
            if not self.is_fitted:
                df[existing_cols] = self.scaler.fit_transform(df[existing_cols])
                self.is_fitted = True
            else:
                df[existing_cols] = self.scaler.transform(df[existing_cols])
        
        return df
    
    def _encode_categorical(self, df):
        # Кодирование категориальных признаков
        if 'sensor_id' in df.columns:
            # Создаем one-hot encoding для sensor_id
            sensor_dummies = pd.get_dummies(df['sensor_id'], prefix='sensor', drop_first=True)
            df = pd.concat([df, sensor_dummies], axis=1)
        
        if 'error_code' in df.columns:
            df['has_error'] = (df['error_code'] != 0).astype(int)
            df['error_code_category'] = df['error_code'].apply(self._categorize_error)
            
            # One-hot для категорий ошибок
            error_dummies = pd.get_dummies(df['error_code_category'], prefix='error', drop_first=True)
            df = pd.concat([df, error_dummies], axis=1)
        
        return df
    
    def _categorize_error(self, code):
        # Категоризация кодов ошибок
        if code == 0:
            return 'no_error'
        elif code in [100, 101, 102]:
            return 'warning'
        elif code in [200, 201, 202]:
            return 'critical'
        elif code in [300, 301, 302]:
            return 'fatal'
        elif code in [404]:
            return 'not_found'
        elif code in [500]:
            return 'server_error'
        else:
            return 'unknown'
    
    def _create_features(self, df):
        
        # Создание новых признаков

        # 1. Комбинированные признаки
        if 'temperature' in df.columns and 'humidity' in df.columns:
            df['temp_humidity_ratio'] = df['temperature'] / (df['humidity'] + 1)
        
        # 2. Агрегированные признаки
        if 'vibration' in df.columns:
            df['vibration_squared'] = df['vibration'] ** 2
            df['vibration_log'] = np.log1p(df['vibration'])
        
        # 3. Признаки взаимодействия с временем
        if 'hour' in df.columns and 'temperature' in df.columns:
            df['temp_by_hour'] = df['temperature'] * np.sin(2 * np.pi * df['hour'] / 24)
        
        return df