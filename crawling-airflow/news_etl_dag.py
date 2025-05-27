"""
News ETL DAG for Airflow

이 DAG는 다음 작업을 수행합니다:
1. 뉴스 데이터 수집 (네이버 API, Firecrawl)
2. 데이터 처리 및 기사 본문 추출
3. 퀴즈 생성 및 저장
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG 기본 인수 설정
default_args = {
    'owner': 'weebee',
    'depends_on_past': False,
    'email': ['admin@weebee.io'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 27),
}

# DAG 정의
dag = DAG(
    'news_etl_pipeline',
    default_args=default_args,
    description='금융/경제 뉴스 수집, 처리, 퀴즈 생성 파이프라인',
    schedule_interval='0 */12 * * *',  # 12시간마다 실행 (0시, 12시)
    catchup=False,
    tags=['news', 'etl', 'finance'],
)

# news_etl.py 파일에서 함수 임포트
import sys
import os

# 현재 디렉토리 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 필요한 함수 임포트
from news_etl import (
    cleanup_database,
    fetch_naver_news,
    fetch_firecrawl_news,
    process_news_data,
    save_news_to_database,
    generate_and_save_quizzes
)

# 0. 데이터베이스 초기화 태스크
def cleanup_database_task(**kwargs):
    """기존 뉴스 및 퀴즈 데이터 삭제 (초기화)"""
    print("데이터베이스 초기화 시작...")
    cleanup_database()
    print("데이터베이스 초기화 완료")
    return True

# 1. 네이버 뉴스 수집 태스크
def collect_naver_news_task(**kwargs):
    """네이버 뉴스 API를 통해 금융/경제 관련 뉴스 수집"""
    print("네이버 뉴스 수집 시작...")
    naver_df = fetch_naver_news()
    kwargs['ti'].xcom_push(key='naver_news_df', value=naver_df.to_json(orient='records'))
    print(f"네이버 뉴스 {len(naver_df)}개 수집 완료")
    return len(naver_df)

# 2. Firecrawl 뉴스 수집 태스크
def collect_firecrawl_news_task(**kwargs):
    """Firecrawl API를 통해 금융/경제 관련 글로벌 뉴스 수집"""
    print("Firecrawl 뉴스 수집 시작...")
    firecrawl_df = fetch_firecrawl_news()
    kwargs['ti'].xcom_push(key='firecrawl_news_df', value=firecrawl_df.to_json(orient='records'))
    print(f"Firecrawl 뉴스 {len(firecrawl_df)}개 수집 완료")
    return len(firecrawl_df)

# 3. 뉴스 데이터 처리 태스크
def process_news_task(**kwargs):
    """수집된 뉴스 데이터 처리 및 기사 본문 추출"""
    import pandas as pd
    
    ti = kwargs['ti']
    naver_news_json = ti.xcom_pull(task_ids='collect_naver_news', key='naver_news_df')
    firecrawl_news_json = ti.xcom_pull(task_ids='collect_firecrawl_news', key='firecrawl_news_df')
    
    # JSON 문자열을 DataFrame으로 변환
    naver_df = pd.read_json(naver_news_json, orient='records') if naver_news_json else pd.DataFrame()
    firecrawl_df = pd.read_json(firecrawl_news_json, orient='records') if firecrawl_news_json else pd.DataFrame()
    
    # 데이터 합치기
    combined_df = pd.concat([naver_df, firecrawl_df], ignore_index=True).drop_duplicates(subset=['link'])
    print(f"총 {len(combined_df)}개 뉴스 처리 시작")
    
    if combined_df.empty:
        print("처리할 뉴스가 없습니다.")
        return 0
    
    # 뉴스 데이터 처리
    processed_df = process_news_data(combined_df)
    kwargs['ti'].xcom_push(key='processed_news_df', value=processed_df.to_json(orient='records'))
    print(f"뉴스 데이터 처리 완료: {len(processed_df)}개")
    return len(processed_df)

# 4. 데이터베이스 저장 태스크
def save_to_database_task(**kwargs):
    """처리된 뉴스 데이터를 데이터베이스에 저장"""
    import pandas as pd
    
    ti = kwargs['ti']
    processed_news_json = ti.xcom_pull(task_ids='process_news', key='processed_news_df')
    
    if not processed_news_json:
        print("저장할 뉴스 데이터가 없습니다.")
        return 0
    
    # JSON 문자열을 DataFrame으로 변환
    processed_df = pd.read_json(processed_news_json, orient='records')
    
    # 데이터베이스에 뉴스 저장
    news_ids = save_news_to_database(processed_df)
    kwargs['ti'].xcom_push(key='news_ids', value=news_ids)
    print(f"뉴스 {len(news_ids)}개 데이터베이스 저장 완료")
    return len(news_ids)

# 5. 퀴즈 생성 및 저장 태스크
def generate_quizzes_task(**kwargs):
    """저장된 뉴스 기사를 기반으로 퀴즈 생성 및 저장"""
    ti = kwargs['ti']
    news_ids = ti.xcom_pull(task_ids='save_to_database', key='news_ids')
    
    if not news_ids or len(news_ids) == 0:
        print("퀴즈를 생성할 뉴스가 없습니다.")
        return 0
    
    # 퀴즈 생성 및 저장
    print(f"{len(news_ids)}개 뉴스에 대한 퀴즈 생성 시작")
    generate_and_save_quizzes(news_ids)
    print("퀴즈 생성 및 저장 완료")
    return len(news_ids)

# DAG에 태스크 추가
cleanup_db = PythonOperator(
    task_id='cleanup_database',
    python_callable=cleanup_database_task,
    provide_context=True,
    dag=dag,
)

collect_naver_news = PythonOperator(
    task_id='collect_naver_news',
    python_callable=collect_naver_news_task,
    provide_context=True,
    dag=dag,
)

collect_firecrawl_news = PythonOperator(
    task_id='collect_firecrawl_news',
    python_callable=collect_firecrawl_news_task,
    provide_context=True,
    dag=dag,
)

process_news = PythonOperator(
    task_id='process_news',
    python_callable=process_news_task,
    provide_context=True,
    dag=dag,
)

save_to_database = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database_task,
    provide_context=True,
    dag=dag,
)

generate_quizzes = PythonOperator(
    task_id='generate_quizzes',
    python_callable=generate_quizzes_task,
    provide_context=True,
    dag=dag,
)

# 태스크 의존성 설정
cleanup_db >> [collect_naver_news, collect_firecrawl_news] >> process_news >> save_to_database >> generate_quizzes
