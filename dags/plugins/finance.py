from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import yfinance as yf
import os
import pytz

# 기본 DAG 인자
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'stock_price_collection',
    default_args=default_args,
    description='A DAG to collect stock prices using yfinance',
    schedule_interval='*/30 * * * *',  # 30분마다 실행
    start_date=datetime(2024, 8, 26),
    catchup=False,
)

def fetch_and_save_stock_price():
    stock_symbol = '005930.KS'
    file_path = './dags/stock_data.csv'  # CSV 파일의 경로를 수정하세요.
    
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

# PythonOperator 정의
fetch_stock_price = PythonOperator(
    task_id='fetch_stock_price',
    python_callable=fetch_and_save_stock_price,
    dag=dag,
)

# DAG 설정
fetch_stock_price
