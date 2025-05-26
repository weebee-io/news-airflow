#!/bin/bash

# 스크립트 실행 시간 기록
echo "===== DeepSearch ETL 작업 시작: $(date) ====="

# Python 스크립트 실행
# Assumes python3 is in PATH and deepsearch_api.py is in the current working directory (set by Dockerfile's WORKDIR)
# Environment variables are expected to be set by Docker Compose
python3 deepsearch_api.py

# 실행 완료 시간 기록
echo "===== DeepSearch ETL 작업 완료: $(date) ====="