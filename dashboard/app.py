import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
import os
import joblib

st.set_page_config(page_title="Kafka Dashboard", layout="wide")
st.title("Kafka Data Dashboard")
st.markdown("---")

# Загружаем модель
@st.cache_resource
def load_model():
    try:
        model_path = 'models/anomaly_detector.pkl'
        if os.path.exists(model_path):
            model = joblib.load(model_path)
            st.success("Модель загружена")
            return model
        return None
    except:
        return None

model = load_model()

# Загружаем данные
@st.cache_data
def load_data():

    datasets_file = 'data/datasets/sensor_data.parquet'
    if os.path.exists(datasets_file):
        df = pd.read_parquet(datasets_file)
        st.info(f"Данные из datasets: {len(df)} записей")
        return df
    
    st.error("Нет данных ни в processed, ни в datasets")
    return None

df = load_data()

if df is not None:
    # Основные метрики
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Температура", f"{df['temperature'].mean():.1f}°C")
    with col2:
        st.metric("Влажность", f"{df['humidity'].mean():.1f}%")
    with col3:
        st.metric("Давление", f"{df['pressure'].mean():.1f} hPa")
    with col4:
        anomalies = df['is_anomaly'].sum() if 'is_anomaly' in df.columns else 0
        st.metric("Аномалии", f"{anomalies}")

    # Графики
    col1, col2 = st.columns(2)
    
    with col1:
        fig_temp = px.histogram(df, x='temperature', title='Распределение температуры')
        st.plotly_chart(fig_temp, use_container_width=True)
    
    with col2:
        fig_hum = px.histogram(df, x='humidity', title='Распределение влажности')
        st.plotly_chart(fig_hum, use_container_width=True)
    
    # Временной ряд
    if 'timestamp' in df.columns:
        # Берем случайные 1000 записей для производительности
        sample_df = df.sample(min(1000, len(df)))
        fig_time = px.scatter(sample_df, x='timestamp', y='temperature', 
                             color='is_anomaly' if 'is_anomaly' in df.columns else None,
                             title='Температура по времени')
        st.plotly_chart(fig_time, use_container_width=True)
    
    # Показываем данные
    with st.expander("Показать данные"):
        st.dataframe(df.head(100))
else:
    st.warning("Нет данных для отображения")
    
    # Создаем тестовые данные для демонстрации
    st.info("Создаю тестовые данные для демонстрации...")
    n_samples = 1000
    test_df = pd.DataFrame({
        'timestamp': pd.date_range(start=datetime.now(), periods=n_samples, freq='1min'),
        'temperature': np.random.normal(25, 5, n_samples),
        'humidity': np.random.normal(60, 10, n_samples),
        'pressure': np.random.normal(1013, 10, n_samples),
        'vibration': np.random.exponential(0.1, n_samples),
        'air_quality_co2': np.random.normal(400, 50, n_samples),
        'battery_level': np.random.uniform(0, 100, n_samples),
        'signal_strength': np.random.uniform(-120, -30, n_samples),
        'is_anomaly': np.random.choice([0, 1], n_samples, p=[0.95, 0.05])
    })
    st.dataframe(test_df.head(100))

st.markdown("---")
st.caption(f"Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")