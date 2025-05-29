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
import pandas as pd
# 로깅 대신 print 사용

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
    'news_etl_pipeline_yeonji',
    default_args=default_args,
    description='금융/경제 뉴스 수집, 처리, 퀴즈 생성 파이프라인',
    schedule_interval='0 */6 * * *',  # 6시간마다 실행 (0시, 6시, 12시, 18시)
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
from news_etl_yeonji import (
    fetch_deepsearch_news,
    fetch_firecrawl_news,
    save_news_to_database,
    generate_and_save_quizzes
)

# 1. deepsearch 뉴스 수집 태스크
def collect_naver_news_task(**kwargs):
    """deepsearch News RSS 피드를 통해 금융/경제 관련 뉴스 수집"""
    print("deepsearch 뉴스 수집 시작...")
    try:
        deepsearch_df = fetch_deepsearch_news()  
        if deepsearch_df.empty:
            print("수집된 deepsearch 뉴스가 없습니다.")
            kwargs['ti'].xcom_push(key='naver_news_df', value='[]')
            return pd.DataFrame()
        kwargs['ti'].xcom_push(key='naver_news_df', value=deepsearch_df.to_json(orient='records'))
        print(deepsearch_df.head(1))
        print(f"deepsearch 뉴스 {len(deepsearch_df)}개 수집 완료")
        return deepsearch_df
    except Exception as e:
        print(f"deepsearch 뉴스 수집 중 오류: {e}")
        kwargs['ti'].xcom_push(key='naver_news_df', value='[]')
        return pd.DataFrame()

# 2. Firecrawl 뉴스 수집 태스크
def collect_firecrawl_news_task(**kwargs):
    """Firecrawl API를 통해 금융/경제 관련 글로벌 뉴스 수집"""
    print("Firecrawl 뉴스 수집 시작...")
    try:
        firecrawl_df = fetch_firecrawl_news()
        if not firecrawl_df.empty:
            kwargs['ti'].xcom_push(key='firecrawl_news_df', value=firecrawl_df.to_json(orient='records'))
            print(f"Firecrawl 뉴스 {len(firecrawl_df)}개 수집 완료")
            return firecrawl_df
        else:
            print("수집된 Firecrawl 뉴스가 없습니다.")
            # 빈 JSON 문자열 전달 (빈 DataFrame 대신)
            kwargs['ti'].xcom_push(key='firecrawl_news_df', value='[]')
            return pd.DataFrame()
    except Exception as e:
        print(f"Firecrawl 뉴스 수집 중 오류: {e}")
        # 오류 발생 시 빈 JSON 문자열 전달
        kwargs['ti'].xcom_push(key='firecrawl_news_df', value='[]')
        return pd.DataFrame()

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
    
    # 필드 이름 통일 (link가 없으면 url 사용, url이 없으면 link 사용)
    if not naver_df.empty:
        if 'link' not in naver_df.columns and 'url' in naver_df.columns:
            naver_df['link'] = naver_df['url']
        elif 'url' not in naver_df.columns and 'link' in naver_df.columns:
            naver_df['url'] = naver_df['link']
            
    if not firecrawl_df.empty:
        if 'link' not in firecrawl_df.columns and 'url' in firecrawl_df.columns:
            firecrawl_df['link'] = firecrawl_df['url']
        elif 'url' not in firecrawl_df.columns and 'link' in firecrawl_df.columns:
            firecrawl_df['url'] = firecrawl_df['link']
    
    # 데이터 합치기
    # URL 중복 제거를 위한 컬럼 결정
    dedup_col = 'link' if ('link' in naver_df.columns or 'link' in firecrawl_df.columns) else 'url'
    
    combined_df = pd.concat([naver_df, firecrawl_df], ignore_index=True)
    if not combined_df.empty and dedup_col in combined_df.columns:
        combined_df = combined_df.drop_duplicates(subset=[dedup_col])
    print(f"총 {len(combined_df)}개 뉴스 처리 시작")
    
    if combined_df.empty:
        print("처리할 뉴스가 없습니다.")
        return 0
    
    # 뉴스 데이터 처리
    # processed_df = process_news_data(combined_df)
    # kwargs['ti'].xcom_push(key='processed_news_df', value=processed_df.to_json(orient='records'))

    # 뉴스 데이터 처리
    processed_df = combined_df

    # 중복 컬럼 제거 (안전하게)
    processed_df = processed_df.loc[:, ~processed_df.columns.duplicated()]

    # 디버깅용 출력
    print(f"데이터프레임 컬럼: {processed_df.columns.tolist()}")
    print(f"뉴스 데이터 처리 완료: {len(processed_df)}개")

    # JSON 문자열로 변환해 XCom에 저장
    kwargs['ti'].xcom_push(key='processed_news_df', value=processed_df.to_json(orient='records'))
    print(f"뉴스 데이터 처리 완료: {len(processed_df)}개")
    return processed_df

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
    return news_ids

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
    return news_ids

# DAG에 태스크 추가 (데이터베이스 초기화 태스크 제거)

collect_naver_news = PythonOperator(
    task_id='collect_naver_news',
    python_callable=collect_naver_news_task,
    dag=dag,
)

collect_firecrawl_news = PythonOperator(
    task_id='collect_firecrawl_news',
    python_callable=collect_firecrawl_news_task,
    dag=dag,
)

process_news = PythonOperator(
    task_id='process_news',
    python_callable=process_news_task,
    dag=dag,
)

save_to_database = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database_task,
    dag=dag,
)

generate_quizzes = PythonOperator(
    task_id='generate_quizzes',
    python_callable=generate_quizzes_task,
    dag=dag,
)

# 태스크 의존성 설정 (데이터베이스 초기화 태스크 제거)
[collect_naver_news, collect_firecrawl_news] >> process_news >> save_to_database >> generate_quizzes
