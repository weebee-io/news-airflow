#!/usr/bin/env python3
"""
News ETL Pipeline

이 스크립트는 다음 작업을 수행합니다:
1. 네이버 API와 Firecrawl을 통해 금융/경제 관련 뉴스 데이터 수집
2. newspaper 라이브러리를 사용하여 기사 본문 추출
3. Claude API를 사용하여 뉴스 전처리 및 퀴즈 생성
4. AWS RDS에 데이터 저장
"""

import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
import pymysql
from datetime import datetime, timedelta
import time
import random
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import anthropic
from newspaper import Article
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 환경 변수 로드
logger.info(f"Current working directory: {os.getcwd()}")
load_dotenv()
logger.info("환경 변수 로드 완료")

# API 키 및 환경 변수 확인
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
FIRECRAWL_API_KEY = os.getenv('FIRECRAWL_API_KEY')
NAVER_CLIENT_ID = os.getenv('NAVER_CLIENT_ID')
NAVER_CLIENT_SECRET = os.getenv('NAVER_CLIENT_SECRET')
DB_IP = os.getenv('DB_IP')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def fetch_naver_news():
    """네이버 API를 통해 금융/경제 관련 뉴스 수집"""
    logger.info("네이버 뉴스 API 데이터 수집 시작")
    
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET
    }
    
    # 검색어 리스트
    search_terms = [
        "금융", "경제", "주식", "투자", "은행", 
        "금리", "채권", "외환", "글로벌 경제", "국제 금융"
    ]
    
    news_items = []
    
    for term in search_terms:
        # 검색어 인코딩
        encText = urllib.parse.quote(term)
        params = {
            "query": encText,
            "display": 20,  # 한 번에 가져올 결과 수
            "start": 1,     # 시작 위치
            "sort": "date"  # 날짜순 정렬
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "items" in data and data["items"]:
                logger.info(f"'{term}' 검색어로 {len(data['items'])}개 뉴스 항목 수집")
                news_items.extend(data["items"])
            else:
                logger.warning(f"'{term}' 검색어로 뉴스를 찾을 수 없습니다.")
                
            # API 호출 간 딜레이
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"네이버 API 호출 중 오류 발생: {e}")
    
    # 결과를 DataFrame으로 변환
    if news_items:
        df = pd.DataFrame(news_items)
        # HTML 태그 제거
        df['title'] = df['title'].str.replace('<[^<]+?>', '', regex=True)
        df['description'] = df['description'].str.replace('<[^<]+?>', '', regex=True)
        # 중복 제거
        df = df.drop_duplicates(subset=['link'])
        logger.info(f"총 {len(df)}개 네이버 뉴스 수집 완료")
        return df
    else:
        logger.warning("수집된 뉴스가 없습니다.")
        return pd.DataFrame()

def fetch_firecrawl_news():
    """Firecrawl API를 통해 금융/경제 관련 글로벌 뉴스 수집"""
    logger.info("Firecrawl API 데이터 수집 시작")
    
    if not FIRECRAWL_API_KEY:
        logger.error("Firecrawl API 키가 설정되지 않았습니다.")
        return pd.DataFrame()
    
    url = "https://api.firecrawl.dev/search"
    headers = {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # 검색어 리스트 (영어)
    search_terms = [
        "finance news", "economic news", "stock market", 
        "investment news", "banking news", "global economy"
    ]
    
    news_items = []
    
    for term in search_terms:
        payload = {
            "query": term,
            "num_results": 20,
            "time_period": "day",  # 최근 24시간 내 뉴스
            "search_type": "news"
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if "results" in data and data["results"]:
                logger.info(f"'{term}' 검색어로 {len(data['results'])}개 뉴스 항목 수집")
                
                for item in data["results"]:
                    news_items.append({
                        "title": item.get("title", ""),
                        "description": item.get("snippet", ""),
                        "link": item.get("url", ""),
                        "pubDate": item.get("published_date", ""),
                        "source": item.get("source", "")
                    })
            else:
                logger.warning(f"'{term}' 검색어로 뉴스를 찾을 수 없습니다.")
                
            # API 호출 간 딜레이
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Firecrawl API 호출 중 오류 발생: {e}")
    
    # 결과를 DataFrame으로 변환
    if news_items:
        df = pd.DataFrame(news_items)
        # 중복 제거
        df = df.drop_duplicates(subset=['link'])
        logger.info(f"총 {len(df)}개 Firecrawl 뉴스 수집 완료")
        return df
    else:
        logger.warning("수집된 뉴스가 없습니다.")
        return pd.DataFrame()

def extract_article_with_newspaper(url):
    """newspaper 라이브러리를 사용하여 뉴스 기사 전문 추출"""
    if not url or not url.startswith('http'):
        return ""
    
    # 랜덤 User-Agent 설정
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
    ]
    
    try:
        # newspaper 라이브러리로 기사 다운로드 및 파싱
        article = Article(url)
        article.config.browser_user_agent = random.choice(user_agents)
        article.download()
        article.parse()
        
        # 기사 내용이 충분히 있는지 확인
        if article.text and len(article.text) > 100:
            return article.text
        
        # 내용이 부족하면 메타데이터도 확인
        if hasattr(article, 'meta_description') and article.meta_description:
            return article.meta_description
            
        # 그래도 내용이 없으면 빈 문자열 반환
        return ""
        
    except Exception as e:
        logger.error(f"기사 추출 중 오류 발생 ({url}): {e}")
        return ""

def process_news_data(df):
    """수집된 뉴스 데이터 처리 및 기사 본문 추출"""
    if df.empty:
        logger.warning("처리할 데이터가 없습니다.")
        return df
    
    # 데이터 전처리
    df = df.drop_duplicates(subset=['link', 'title'])
    
    # 날짜 형식 통일
    if 'pubDate' in df.columns:
        df['pubDate'] = pd.to_datetime(df['pubDate'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # 병렬로 기사 본문 추출
    urls = df['link'].tolist()
    logger.info(f"총 {len(urls)}개 기사 본문 추출 시작")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(extract_article_with_newspaper, url): i for i, url in enumerate(urls)}
        
        for future in as_completed(future_to_url):
            idx = future_to_url[future]
            try:
                content = future.result()
                if content:
                    df.at[idx, 'description'] = content
                    logger.info(f"기사 {idx+1}/{len(urls)} 본문 추출 완료 ({len(content)} 자)")
                else:
                    logger.warning(f"기사 {idx+1}/{len(urls)} 본문 추출 실패")
            except Exception as e:
                logger.error(f"기사 {idx+1}/{len(urls)} 처리 중 오류: {e}")
    
    # 데이터베이스 구조에 맞게 컬럼 이름 변경
    column_mapping = {
        'title': 'title',
        'description': 'description',
        'link': 'url',
        'pubDate': 'published_date',
        'source': 'source'
    }
    
    # 필요한 컬럼만 선택하고 이름 변경
    df = df.rename(columns=column_mapping)
    df = df[list(column_mapping.values())]
    
    # 현재 시간 추가
    df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"총 {len(df)}개 기사 처리 완료")
    return df

def preprocess_news_with_claude(news_id, title, description):
    """Claude API를 사용하여 뉴스 기사 요약 및 키워드 추출"""
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키가 설정되지 않았습니다.")
        return None
    
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 처리할 수 없습니다.")
        return None
    
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
        prompt = f"""
        다음은 금융/경제 관련 뉴스 기사입니다:
        
        제목: {title}
        
        본문:
        {description[:10000]}
        
        위 기사에 대해 다음 정보를 제공해주세요:
        1. 기사 내용을 3~4문장으로 객관적 요약
        2. 금융 개념 키워드 5개 (고유명사 제외, 예: 금리, 자산, 소비, 재정, 통화정책)
        
        다음 JSON 형식으로 응답해주세요:
        {{
            "summary": "기사 요약문",
            "keywords": "키워드1, 키워드2, 키워드3, 키워드4, 키워드5"
        }}
        """
        
        message = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            temperature=0.2,
            system="당신은 금융/경제 뉴스를 분석하고 요약하는 AI 전문가입니다. 항상 객관적이고 정확한 정보를 제공합니다.",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        
        # 응답에서 JSON 형식 추출
        response_text = message.content[0].text
        
        # JSON 부분만 추출
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start >= 0 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            processed_data = json.loads(json_str)
            
            if "summary" in processed_data and "keywords" in processed_data:
                logger.info(f"기사 ID {news_id} 전처리 완료")
                return processed_data
        
        logger.error(f"기사 ID {news_id}에 대한 전처리 응답이 유효한 JSON 형식이 아닙니다.")
        return None
        
    except Exception as e:
        logger.error(f"Claude API 호출 중 오류 발생: {e}")
        return None

def generate_quiz_with_claude(news_id, title, description):
    """Claude API를 사용하여 뉴스 기사 기반 퀴즈 생성"""
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키가 설정되지 않았습니다.")
        return None
    
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 퀴즈를 생성할 수 없습니다.")
        return None
    
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
        prompt = f"""
        당신은 경제·금융 교육을 위한 NIE 콘텐츠 제작자입니다. 아래 뉴스 기사를 분석하여, 금융 교육 콘텐츠로 변환하십시오.
        
        제목: {title}
        
        본문:
        {description[:10000]}
        
        위 기사 내용을 바탕으로 최소 3개의 퀴즈를 생성해주세요. 각 퀴즈는 다음 형식을 따라야 합니다:
        
        1. newsquiz_content: 키워드 중 하나에 대한 금융지식/이해력 관련 질문 (개념 응용 또는 이해 중심, 고유명사 사용 금지)
        2. newsquiz_choice_a~d: 객관식 보기 4개
        3. newsquiz_correct_ans: 정답 보기 (a, b, c, d 중 하나)
        4. newsquiz_score: EASY=30 / NORMAL=60 / HARD=100
        5. newsquiz_level: 난이도 (EASY / NORMAL / HARD)
        6. reason: 난이도 판정 근거
        
        난이도 분류 기준:
        - "EASY": 개념 정의 또는 직접적인 결과
        - "NORMAL": 개념 간 연관성 또는 원인-결과 이해
        - "HARD": 정책 변화, 구조적 추론, 간접적 응용 필요
        
        다음 JSON 형식으로 응답해주세요:
        {{
            "quizzes": [
                {{
                    "newsquiz_content": "질문 내용",
                    "newsquiz_choice_a": "보기 1",
                    "newsquiz_choice_b": "보기 2",
                    "newsquiz_choice_c": "보기 3",
                    "newsquiz_choice_d": "보기 4",
                    "newsquiz_correct_ans": "정답 보기(a, b, c, d 중 하나)",
                    "newsquiz_score": 난이도에 따른 점수,
                    "newsquiz_level": "난이도",
                    "reason": "난이도 판정 근거"
                }},
                ...
            ]
        }}
        """
        
        message = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=2000,
            temperature=0.7,
            system="당신은 금융/경제 뉴스를 분석하고 교육적인 퀴즈를 생성하는 AI 전문가입니다. 항상 난이도를 적절히 분류하고 교육적 가치가 있는 문제를 출제합니다.",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        
        # 응답에서 JSON 형식 추출
        response_text = message.content[0].text
        
        # JSON 부분만 추출
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start >= 0 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            quiz_data = json.loads(json_str)
            
            if "quizzes" in quiz_data and len(quiz_data["quizzes"]) > 0:
                logger.info(f"기사 ID {news_id}에 대한 {len(quiz_data['quizzes'])}개 퀴즈 생성 완료")
                return quiz_data
        
        logger.error(f"기사 ID {news_id}에 대한 퀴즈 생성 응답이 유효한 JSON 형식이 아닙니다.")
        return None
        
    except Exception as e:
        logger.error(f"Claude API 호출 중 오류 발생: {e}")
        return None

def save_news_to_database(df):
    """뉴스 데이터를 MySQL 데이터베이스에 저장"""
    if df.empty:
        logger.warning("저장할 데이터가 없습니다.")
        return []
    
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        news_ids = []
        
        for _, row in df.iterrows():
            # 뉴스 기사 저장
            cursor.execute('''
                INSERT INTO news 
                (title, description, url, published_date, source, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                description = VALUES(description),
                source = VALUES(source),
                published_date = VALUES(published_date)
            ''', (
                row['title'], 
                row['description'], 
                row['url'], 
                row['published_date'], 
                row['source'], 
                row['created_at']
            ))
            
            # 삽입된 기사의 ID 가져오기
            news_id = cursor.lastrowid
            if not news_id:
                # 이미 존재하는 기사의 경우 ID 조회
                cursor.execute('SELECT news_id FROM news WHERE url = %s', (row['url'],))
                result = cursor.fetchone()
                if result:
                    news_id = result[0]
            
            if news_id:
                news_ids.append(news_id)
        
        conn.commit()
        logger.info(f"{len(news_ids)}개 기사 데이터베이스 저장 완료")
        
        cursor.close()
        conn.close()
        
        return news_ids
        
    except Exception as e:
        logger.error(f"데이터베이스 저장 중 오류 발생: {e}")
        return []

def update_news_with_claude_data(news_id, processed_data):
    """뉴스 기사에 Claude가 생성한 요약문과 키워드 추가"""
    if not processed_data or "summary" not in processed_data or "keywords" not in processed_data:
        logger.warning(f"기사 ID {news_id}에 대한 전처리 데이터가 없습니다.")
        return False
    
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        # 뉴스 업데이트 - 요약문과 키워드 추가
        cursor.execute('''
            UPDATE news 
            SET summary = %s, keywords = %s
            WHERE news_id = %s
        ''', (
            processed_data["summary"],
            processed_data["keywords"],
            news_id
        ))
        
        conn.commit()
        logger.info(f"기사 ID {news_id} 요약문과 키워드 업데이트 완료")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"뉴스 업데이트 중 오류 발생: {e}")
        return False

def save_quiz_to_database(news_id, quiz_data):
    """생성된 퀴즈를 데이터베이스에 저장"""
    if not quiz_data or "quizzes" not in quiz_data:
        logger.warning(f"기사 ID {news_id}에 대한 퀴즈 데이터가 없습니다.")
        return False
    
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        for quiz in quiz_data["quizzes"]:
            # 퀴즈 저장
            cursor.execute('''
                INSERT INTO news_quiz 
                (news_id, newsquiz_content, newsquiz_choice_a, newsquiz_choice_b, 
                newsquiz_choice_c, newsquiz_choice_d, newsquiz_correct_ans, 
                newsquiz_score, newsquiz_level, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                news_id,
                quiz["newsquiz_content"],
                quiz["newsquiz_choice_a"],
                quiz["newsquiz_choice_b"],
                quiz["newsquiz_choice_c"],
                quiz["newsquiz_choice_d"],
                quiz["newsquiz_correct_ans"],
                quiz["newsquiz_score"],
                quiz["newsquiz_level"],
                quiz["reason"]
            ))
        
        conn.commit()
        logger.info(f"기사 ID {news_id}에 대한 {len(quiz_data['quizzes'])}개 퀴즈 저장 완료")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"퀴즈 데이터베이스 저장 중 오류 발생: {e}")
        return False

def generate_and_save_quizzes(news_ids):
    """기사 ID 목록에 대해 요약/키워드 추출 및 퀴즈 생성 처리"""
    if not news_ids:
        logger.warning("처리할 기사가 없습니다.")
        return
    
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        # 각 뉴스 기사에 대해 처리
        for news_id in news_ids:
            # 뉴스 데이터 가져오기
            cursor.execute('SELECT title, description FROM news WHERE news_id = %s', (news_id,))
            news_data = cursor.fetchone()
            
            if not news_data:
                logger.warning(f"기사 ID {news_id}의 데이터를 찾을 수 없습니다.")
                continue
                
            title, description = news_data
            
            # 1. 요약문과 키워드 추출
            logger.info(f"기사 ID {news_id} 요약 및 키워드 추출 시작...")
            processed_data = preprocess_news_with_claude(news_id, title, description)
            
            if processed_data:
                # 요약문과 키워드 저장
                update_news_with_claude_data(news_id, processed_data)
            else:
                logger.warning(f"기사 ID {news_id}에 대한 요약 및 키워드 추출 실패")
                continue
            
            # 2. 퀴즈 생성
            logger.info(f"기사 ID {news_id} 퀴즈 생성 시작...")
            quiz_data = generate_quiz_with_claude(news_id, title, description)
            
            if quiz_data:
                # 퀴즈 저장
                save_quiz_to_database(news_id, quiz_data)
            else:
                logger.warning(f"기사 ID {news_id}에 대한 퀴즈 생성 실패")
                
            # API 호출 제한 에 경출
            time.sleep(1)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"퀴즈 생성 및 저장 중 오류 발생: {e}")

def cleanup_database():
    """기존 데이터베이스 데이터 초기화"""
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        # 테이블 초기화
        cursor.execute('DELETE FROM news_quiz')
        cursor.execute('DELETE FROM news')
        
        conn.commit()
        logger.info("데이터베이스 테이블 초기화 완료")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"데이터베이스 초기화 중 오류 발생: {e}")

def main():
    """메인 ETL 프로세스"""
    logger.info("=== 뉴스 ETL 파이프라인 시작 ===")
    
    try:
        # 0. 데이터베이스 초기화 (사용자 요청에 따라)
        cleanup_database()
        
        # 1. 네이버 뉴스 수집
        naver_df = fetch_naver_news()
        logger.info(f"네이버 뉴스 {len(naver_df)}개 수집 완료")
        
        # 2. Firecrawl 뉴스 수집
        firecrawl_df = fetch_firecrawl_news()
        logger.info(f"Firecrawl 뉴스 {len(firecrawl_df)}개 수집 완료")
        
        # 3. 데이터 합치기
        combined_df = pd.concat([naver_df, firecrawl_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['url'])
        logger.info(f"총 {len(combined_df)}개 뉴스 수집")
        
        if combined_df.empty:
            logger.warning("수집된 뉴스가 없습니다.")
            return
        
        # 4. 뉴스 데이터 처리
        processed_df = process_news_data(combined_df)
        logger.info(f"데이터 처리 완료: {len(processed_df)}개")
        
        # 5. 데이터베이스에 저장
        news_ids = save_news_to_database(processed_df)
        logger.info(f"데이터베이스 저장 완료: {len(news_ids)}개")
        
        # 6. 요약/키워드 추출 및 퀴즈 생성 및 저장
        generate_and_save_quizzes(news_ids)
        
        logger.info("=== 뉴스 ETL 파이프라인 완료 ===")
        
    except Exception as e:
        logger.error(f"ETL 프로세스 중 오류 발생: {e}")

if __name__ == "__main__":
    main()
