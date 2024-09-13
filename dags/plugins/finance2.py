from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import os
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import pytz

# 기본 DAG 인자
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'stock_price_prediction',
    default_args=default_args,
    description='A DAG to collect and predict stock prices using yfinance and Linear Regression',
    schedule_interval='0 11 * * *',  # 매일 오전 11시에 실행
    start_date=datetime(2024, 8, 26),
    catchup=False,
)

def fetch_and_save_stock_price():
    stock_symbol = '005930.KS'
    file_path = './dags/stock_data.csv'
    
    # 한국 시간대 정의
    tz = pytz.timezone('Asia/Seoul')
    current_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    
    # 주식 데이터 다운로드
    try:
        stock_data = yf.download(stock_symbol, period='1d', interval='1h')
    except Exception as e:
        print(f"Error downloading stock data: {e}")
        return

    # 데이터프레임에 현재 시간 추가
    stock_data['Timestamp'] = current_time
    
    # 파일이 존재하는지 확인
    file_exists = os.path.isfile(file_path)
    
    # CSV 파일에 저장
    stock_data.to_csv(file_path, mode='a', header=not file_exists)
    
    print(f"Stock data saved to {file_path} at {current_time}")

def train_and_predict_stock_price():
    file_path = './dags/stock_data.csv'
    output_file_path = './dags/stock_predictions.csv'
    
    if not os.path.isfile(file_path):
        print("Data file does not exist.")
        return

    # CSV 파일에서 데이터 로드
    data = pd.read_csv(file_path, parse_dates=['Timestamp'], index_col='Timestamp')

    # 데이터 전처리
    data = data[['Close']]
    data['Hour'] = data.index.hour
    data['Day'] = data.index.dayofweek
    
    # Feature와 Target 변수 설정
    X = data[['Hour', 'Day']]
    y = data['Close']

    # Train/Test 데이터 분리
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # 데이터 정규화
    scaler_X = StandardScaler()
    X_train_scaled = scaler_X.fit_transform(X_train)
    X_test_scaled = scaler_X.transform(X_test)

    # 모델 학습
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)

    # 예측
    predictions = model.predict(X_test_scaled)
    
    # 마지막 데이터 포인트를 예측하기 위한 Feature 생성
    last_data = X.iloc[-1].values.reshape(1, -1)
    last_data_scaled = scaler_X.transform(last_data)
    predicted_price = model.predict(last_data_scaled)
    
    # 예측 결과를 DataFrame으로 변환
    tz = pytz.timezone('Asia/Seoul')
    prediction_time = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    prediction_df = pd.DataFrame({
        'Timestamp': [prediction_time],
        'Predicted_Price': [predicted_price[0]]
    })
    
    # 예측 결과를 CSV 파일에 저장
    if os.path.isfile(output_file_path):
        prediction_df.to_csv(output_file_path, mode='a', header=False, index=False)
    else:
        prediction_df.to_csv(output_file_path, mode='w', header=True, index=False)
    
    print(f"Predicted price saved to {output_file_path} at {prediction_time}")

# PythonOperator 정의
fetch_stock_price = PythonOperator(
    task_id='fetch_stock_price',
    python_callable=fetch_and_save_stock_price,
    dag=dag,
)

predict_stock_price = PythonOperator(
    task_id='predict_stock_price',
    python_callable=train_and_predict_stock_price,
    dag=dag,
)

# DAG 설정
fetch_stock_price >> predict_stock_price
