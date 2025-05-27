#!/bin/bash

# 스크립트 실행 시간 기록
echo "===== 뉴스 ETL 작업 시작: $(date) ====="

# 가상환경 활성화 (EC2 환경에 맞게 경로 수정 필요)
if [ -d "crawling_venv" ]; then
    source crawling_venv/bin/activate
    echo "가상환경 활성화: crawling_venv"
elif [ -d "crawling" ]; then
    source crawling/bin/activate
    echo "가상환경 활성화: crawling"
else
    echo "경고: 가상환경을 찾을 수 없습니다. 시스템 Python으로 실행합니다."
fi

# 필요한 패키지 설치 확인
echo "필요한 패키지 확인 중..."
pip install -r requirements.txt

# Python 스크립트 실행
echo "뉴스 ETL 스크립트 실행 중..."
python news_etl.py

# 실행 완료 시간 기록
echo "===== 뉴스 ETL 작업 완료: $(date) ====="
