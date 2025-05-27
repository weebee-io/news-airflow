#!/bin/bash

# 가상환경 활성화
source ~/weebee/crawling_venv/bin/activate

# Airflow 환경 설정
export AIRFLOW_HOME=~/airflow

# Airflow 초기화
airflow db init

# Airflow 관리자 계정 생성 (이미 생성했다면 건너뛰어도 됨)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@weebee.io \
    --password admin

# DAG 파일 복사
mkdir -p ~/airflow/dags
cp ~/weebee/crawling-airflow/news_etl.py ~/airflow/dags/
cp ~/weebee/crawling-airflow/news_etl_dag.py ~/airflow/dags/
cp ~/weebee/.env ~/airflow/dags/ # .env 파일도 복사

# 기존 프로세스 종료
pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true

# Airflow 웹서버 및 스케줄러 시작
nohup airflow webserver --port 8080 --hostname 0.0.0.0 > ~/airflow/webserver.log 2>&1 &
nohup airflow scheduler > ~/airflow/scheduler.log 2>&1 &

# 내 EC2 인스턴스의 퍼블릭 IP 가져오기 (EC2 메타데이터 서비스 이용)
EC2_PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)

echo "Airflow 웹서버와 스케줄러가 시작되었습니다."
echo "웹 UI는 http://${EC2_PUBLIC_IP}:8080 에서 접속할 수 있습니다."
echo "로그인 정보: username=admin, password=admin"
