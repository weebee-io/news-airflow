#!/usr/bin/env python3
"""
News ETL Pipeline

이 스크립트는 다음 작업을 수행합니다:
1. 네이버 API와 Firecrawl을 통해 금융/경제 관련 뉴스 데이터 수집
2. newspaper 라이브러리를 사용하여 기사 본문 추출
3. Claude API를 사용하여 뉴스 전처리 및 퀴즈 생성
4. AWS RDS에 데이터 저장
"""
import requests
import pandas as pd
from newspaper import Article  # newspaper 라이브러리 import 추가
import logging
from dotenv import load_dotenv 
import os
import anthropic
import json
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import wraps
import pymysql
import time
import hashlib
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
DB_IP = os.getenv('DB_IP')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DEEPSEARCH_API_KEY = os.getenv('DEEPSEARCH_API_KEY')
# 유틸리티 함수

@contextmanager
def get_db_connection():
    """데이터베이스 연결을 위한 컨텍스트 매니저"""
    conn = None
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        yield conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def error_handler(func):
    """함수의 오류를 처리하는 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"{func.__name__} 실행 중 오류 발생: {e}")
            return None
    return wrapper

def call_claude_api(prompt, max_tokens=1000, temperature=0.0):
    """Claude API 호출을 위한 공통 함수"""
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키가 설정되지 않았습니다.")
        return None
        
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
        message = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=max_tokens,
            temperature=temperature,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # 응답에서 JSON 추출
        response_text = message.content[0].text
        
        # JSON 형식 추출
        json_start = response_text.find('{')
        json_end = response_text.rfind('}')
        
        if json_start == -1 or json_end == -1:
            logger.error("JSON 형식을 찾을 수 없습니다.")
            logger.debug(f"Claude 응답: {response_text[:200]}...")
            return None
            
        json_str = response_text[json_start:json_end+1]
        
        try:
            result = json.loads(json_str)
            return result
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON 파싱 오류: {json_err}")
            logger.debug(f"JSON 데이터: {json_str[:200]}...")
            return None
            
    except Exception as e:
        logger.error(f"Claude API 호출 중 오류: {e}")
        return None

def fetch_news_from_api():
    logger.info("뉴스 데이터 수집 시작")
    
    # 최근 12시간 기준으로 시간 설정
    now = datetime.now()
    
    logger.info(f"뉴스 수집: {now.strftime('%Y-%m-%d %H:%M')}")
    
    # API 결과 저장
    results = []
    
    try:
        # deepsearch에서 뉴스 수집 (네이버 뉴스 대체)
        deepsearch_news = fetch_deepsearch_news()
        if not deepsearch_news.empty:
            logger.info(f"Google 뉴스: {len(deepsearch_news)}개 수집")
            results.append(deepsearch_news)
        
        # 추가 뉴스 소스도 같은 방식으로 수정
        firecrawl_news = fetch_firecrawl_news()
        if not firecrawl_news.empty:
            logger.info(f"Firecrawl 뉴스: {len(firecrawl_news)}개 수집")
            results.append(firecrawl_news)
        
    except Exception as e:
        logger.error(f"뉴스 수집 중 오류: {e}")
    
    if not results:
        logger.warning("수집된 뉴스가 없습니다.")
        return pd.DataFrame()
    
    # 수집된 결과 합치기
    combined_df = pd.concat(results, ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['link'])
    logger.info(f"총 {len(combined_df)}개의 뉴스 데이터 수집 완료")
    
    return combined_df

def fetch_firecrawl_news():
    """Firecrawl API를 통해 금융/경제 관련 글로벌 뉴스 수집 - 필터링 없이"""
    logger.info("Firecrawl API 데이터 수집 시작")
    
    if not FIRECRAWL_API_KEY:
        logger.error("Firecrawl API 키가 설정되지 않았습니다.")
        return pd.DataFrame()
    
    url = "https://api.firecrawl.dev/search"
    headers = {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # 검색어 리스트 (영어) - 간단한 금융 키워드로 변경
    search_terms = [
        "finance news", "economic news", "stock market", "investment news", "banking news", 
        "global economy", "financial markets", "trading", "bonds", "currency", "cryptocurrency",
        "financial education", "monetary policy", "inflation", "financial literacy", "market analysis"
    ]
    
    news_items = []
    
    for term in search_terms:
        payload = {
            "query": term,
            "num_results": 20,
            "time_period": "day",  # API 제한으로 12시간이 없어 day 사용 후 코드에서 필터링
            "search_type": "news"
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if "results" in data and data["results"]:
                logger.info(f"'{term}' 검색어로 {len(data['results'])}개 뉴스 항목 수집")
                
                for item in data["results"]:
                    # 발행일 파싱 및 최근 12시간 이내 뉴스인지 확인
                    published_date = item.get("published_date", "")
                    is_recent = True
                    
                    if published_date:
                        try:
                            # Firecrawl API의 일반적인 날짜 형식은 ISO 8601 (예: 2024-05-27T09:30:00Z)
                            pub_datetime = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
                            time_diff = datetime.now(tz=pub_datetime.tzinfo) - pub_datetime
                            is_recent = time_diff.total_seconds() <= 12 * 3600  # 12시간을 초로 변환
                            
                            if not is_recent:
                                logger.debug(f"12시간 이상 지난 Firecrawl 기사 제외: {item.get('title', '')}, 발행일: {published_date}")
                                continue
                        except Exception as e:
                            logger.warning(f"Firecrawl 발행일 파싱 오류: {e}. 현재 시간 사용")
                    
                    news_items.append({
                        "title": item.get("title", ""),
                        "description": item.get("snippet", ""),
                        "link": item.get("url", ""),
                        "pubDate": published_date,
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
        
        # 금융/경제 관련 필터링
        finance_keywords = [
            'finance', 'economic', 'stock', 'invest', 'bank', 'market', 'trading', 
            'bond', 'currency', 'crypto', 'financial', 'monetary', 'inflation', 
            'economy', 'fund', 'asset', 'wealth', 'money', 'tax'
        ]
        
        # 제목이나 설명에 금융/경제 키워드가 포함된 뉴스만 필터링
        pattern = '|'.join(finance_keywords)
        mask = (df['title'].str.contains(pattern, case=False, na=False) | 
                df['description'].str.contains(pattern, case=False, na=False))
        filtered_df = df[mask]
        
        if len(filtered_df) > 0:
            logger.info(f"총 {len(df)}개 중 {len(filtered_df)}개 금융/경제 관련 뉴스 필터링 완료")
            return filtered_df
        else:
            logger.warning("금융/경제 관련 뉴스가 필터링 후 없습니다. 전체 뉴스 반환")
            return df
    else:
        logger.warning("수집된 뉴스가 없습니다.")
        return pd.DataFrame()

def fetch_deepsearch_news():
        """deepsearch 피드를 통해 금융/경제 관련 뉴스 수집"""
        import requests
        import pandas as pd

        
        # 오늘 날짜
        today = datetime.now().strftime("%Y-%m-%d")

        # 내일 날짜
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        # API 요청 URL 및 헤더
        url = "https://api-v2.deepsearch.com/v1/articles/economy,society"
        params = {
            "date_from": today,
            "date_to": tomorrow,
            "api_key": DEEPSEARCH_API_KEY
        }

        # API 호출
        response = requests.get(url, params=params)

        # 응답이 성공했는지 확인
        if response.status_code == 200:
            data = response.json()
            
            # articles 키가 존재하는 경우 해당 데이터를 DataFrame으로 변환
            if "data" in data:
                df = pd.DataFrame(data["data"])
                # print(df.head())  # 결과 확인용
            else:
                print("응답에 'data' 키가 없습니다.")
        else:
            print(f"요청 실패: {response.status_code} - {response.text}")



        # 기사 본문 크롤링 함수 - 직접 originallink의 링크에 들어가서 본문을 가져오기
        def get_article_content(url):
            try:
                article = Article(url, language='ko')  # 한국어 설정
                article.download()
                article.parse()
                return article.text
            except Exception as e:
                print(f"본문 크롤링 오류 ({url}): {e}")
                return None

        # 각 뉴스 링크에서 본문을 추출하여 df의 'content' 열에 추가
        df = df[['id', 'sections', 'title', 'publisher', 'author', 'summary',
            'image_url', 'content_url', 'published_at']]
        df['description'] = df['content_url'].apply(get_article_content)
        df = df[df['description'].str.strip() != '']  # 빈 문자열이나 공백만 있는 행 제거
        df['created_at'] = datetime.now().isoformat()
        df.rename(columns={'publisher': 'source', "content_url":"url", "published_at":"published_date"}, inplace=True)

        return df

@error_handler
def preprocess_news_with_claude(news_id, title, description):
    """기사 내용을 기반으로 Claude API를 이용해 정확한 요약 및 키워드 추출"""
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 처리할 수 없습니다.")
        return None
        
    prompt = f"""
    당신은 금융/경제 뉴스 분석 전문가입니다. 다음 기사에서 가장 중요한 금융/경제 키워드를 추출하고, 요점을 정확하게 요약해주세요.
    
    기사 제목: {title}
    기사 내용: {description[:10000]}
    
    이 기사에서 핵심 금융/경제 키워드 5개와 내용을 2~3문장으로 정확하게 요약해주세요.
    
    1. 키워드는 기사의 핵심 금융/경제 용어나 개념을 담고 있어야 합니다.
    2. 요약은 기사의 주요 내용과 시장에 미칠 영향이나 경제적 의의를 포함해야 합니다.
    
    반드시 다음 JSON 형식으로 응답해주세요:
    {{
        "keywords": "키워드1, 키워드2, 키워드3, 키워드4, 키워드5",
        "summary": "기사의 내용을 2~3문장으로 요약한 내용"
    }}
    
    중요: 오직 JSON 형식만 제공해주세요. 다른 텍스트나 설명은 포함하지 마세요.
    """
    
    # 공통 Claude API 호출 함수 사용
    result = call_claude_api(prompt, max_tokens=1000, temperature=0.0)
    
    if result:
        # 기본값 확인 및 설정
        if "keywords" not in result or not result["keywords"]:
            result["keywords"] = ""
            logger.warning(f"기사 ID {news_id}: 키워드가 없어 빈 문자열로 설정합니다.")
            
        if "summary" not in result or not result["summary"]:
            result["summary"] = ""
            logger.warning(f"기사 ID {news_id}: 요약문이 없어 빈 문자열로 설정합니다.")
            
        logger.info(f"기사 ID {news_id}: 키워드 및 요약 생성 완료")
    
    return result

@error_handler
def generate_quiz_with_claude(news_id, title, description):
    """Claude API를 사용하여 뉴스 기사 기반 퀴즈 생성 - 기사당 최소 3개 문제 생성"""
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키가 설정되지 않았습니다.")
        return None
    
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 퀴즈를 생성할 수 없습니다.")
        return None
    
    prompt = f"""
    당신은 금융/경제 교육을 위한 퀴즈 전문가입니다. 다음 기사를 기반으로 학습자들이 금융 개념을 이해할 수 있는 교육용 퀴즈를 만들어주세요.
    
    기사 제목: {title}
    기사 내용: {description[:10000]}
    
    이 기사의 주요 금융/경제 개념을 이해하기 위한 객관식 퀴즈를 제작해주세요. 반드시 최소 3개 이상의 퀴즈를 제작하고, 다음 형식을 정확히 따라주세요.

    다음 JSON 형식으로 응답해주세요:
    {{
        "quizzes": [
            {{
                "newsquiz_content": "퀴즈 질문",
                "newsquiz_choice_a": "선택지 A",
                "newsquiz_choice_b": "선택지 B",
                "newsquiz_choice_c": "선택지 C",
                "newsquiz_choice_d": "선택지 D",
                "newsquiz_correct_ans": "A", # A, B, C, D 중 하나
                "newsquiz_score": 5, # 1~10사이 숫자
                "newsquiz_level": "NORMAL", # EASY, NORMAL, HARD 중 하나
                "reason": "정답 이유 설명"
            }},
            # 추가 퀴즈...
        ]
    }}
    
    중요: 반드시 최소 3개 이상의 퀴즈를 제작해야 합니다. 각 퀴즈는 다양한 난이도와 주제를 다루어야 합니다.
    """
    
    # 공통 Claude API 호출 함수 사용
    quiz_data = call_claude_api(prompt, max_tokens=4000, temperature=0.7)
    
    if not quiz_data:
        return None
        
    # 최소 3개 이상의 퀴즈가 있는지 확인
    if "quizzes" not in quiz_data or len(quiz_data["quizzes"]) < 3:
        logger.warning(f"기사 ID {news_id}: 퀴즈가 3개 미만입니다. (생성된 퀴즈: {len(quiz_data.get('quizzes', []))}개)")
    
    # 각 퀴즈의 필수 필드 확인
    required_fields = [
        "newsquiz_content", "newsquiz_choice_a", "newsquiz_choice_b", 
        "newsquiz_choice_c", "newsquiz_choice_d", "newsquiz_correct_ans", 
        "newsquiz_score", "newsquiz_level", "reason"
    ]
    
    valid_quizzes = []
    for idx, quiz in enumerate(quiz_data.get("quizzes", [])):
        missing_fields = [f for f in required_fields if f not in quiz]
        
        if missing_fields:
            logger.warning(f"기사 ID {news_id}: 퀴즈 #{idx+1}에 필수 필드 {missing_fields}가 없습니다.")
            # 필드 값 추가
            for field in missing_fields:
                if field == "newsquiz_score":
                    quiz[field] = 5  # 기본값 5
                elif field == "newsquiz_level":
                    quiz[field] = "NORMAL"  # 기본값 medium
                elif field == "newsquiz_correct_ans":
                    quiz[field] = "A"  # 기본값 A
                else:
                    quiz[field] = ""  # 다른 필드는 빈 문자열
        
        # 정답 형식 검사 (A, B, C, D 중 하나여야 함)
        if quiz["newsquiz_correct_ans"] not in ["A", "B", "C", "D"]:
            logger.warning(f"기사 ID {news_id}: 퀴즈 #{idx+1}의 정답이 잘못되었습니다: {quiz['newsquiz_correct_ans']}")
            # A로 바꾸기
            quiz["newsquiz_correct_ans"] = "A"
        
        valid_quizzes.append(quiz)
    
    # 유효한 퀴즈만 저장
    quiz_data["quizzes"] = valid_quizzes
    
    return quiz_data

@error_handler
def save_news_to_database(df):
    """뉴스 데이터프레임을 데이터베이스에 저장 - 중복 기사 확인 및 새 기사만 추가"""
    if df.empty:
        logger.warning("저장할 뉴스 데이터가 없습니다.")
        return []
    
    # 데이터베이스에 추가할 뉴스 ID를 저장할 목록
    inserted_news_ids = []
    total_articles = len(df)
    skipped_articles = 0
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # 기사 해시값 및 URL 저장을 위한 집합
        article_hashes = set()
        article_urls = set()
        
        # 기존 기사 해시 및 URL 로드
        cursor.execute("SELECT article_hash, url FROM news")
        existing_data = cursor.fetchall()
        for hash_val, url in existing_data:
            article_hashes.add(hash_val)
            article_urls.add(url)
            
        for _, row in df.iterrows():
            title = row['title'] if not pd.isna(row['title']) else ''
            description = row['description'] if not pd.isna(row['description']) else ''
            created_at = row['created_at'] if not pd.isna(row['created_at']) else None
            source = row['source'] if not pd.isna(row['source']) else ''
            url = row['url'] if not pd.isna(row['url']) else ''
            keywords = row['keywords'] if 'keywords' in row and not pd.isna(row['keywords']) else ''
            summary = row['summary'] if 'summary' in row and not pd.isna(row['summary']) else ''
            
            # 중복 여부 확인 (URL 및 해시 기반)
            article_hash = hashlib.md5(url.encode()).hexdigest()
            
            # 이미 저장된 기사인지 검사 (해시 또는 URL 기반)
            if article_hash in article_hashes or url in article_urls:
                logger.info(f"중복 기사 건너뜀: {title[:30]}...")
                skipped_articles += 1
                continue
                
            # 스키마 중 keywords, summary 컬럼이 아직 없는 경우에는 에러를 발생할 수 있음
            try:
                cursor.execute('''
                    INSERT INTO news 
                    (title, description, created_at, source, url, article_hash, keywords, summary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    title, description, created_at, source, url, article_hash, keywords, summary
                ))
                
                # 방금 삽입한 레코드의 ID 가져오기
                news_id = cursor.lastrowid
                inserted_news_ids.append(news_id)
                
                # 해시와 URL 등록
                article_hashes.add(article_hash)
                article_urls.add(url)
                
                logger.debug(f"기사 저장 성공 (ID: {news_id}): {title[:30]}...")
                
            except Exception as insert_error:
                logger.error(f"기사 저장 중 오류 발생: {insert_error}")
                # 개별 기사 저장 실패가 전체 처리를 막지 않도록 함
                continue
        
        # 변경사항 적용
        conn.commit()
        cursor.close()
    
    logger.info(f"총 {total_articles}개 중 {len(inserted_news_ids)}개의 새 기사가 데이터베이스에 저장되었습니다. (중복 건너뜀: {skipped_articles}개)")
    return inserted_news_ids

@error_handler
def update_news_with_claude_data(news_id, processed_data):
    """뉴스 기사에 Claude가 생성한 요약문과 키워드 추가"""
    if not processed_data or "summary" not in processed_data or "keywords" not in processed_data:
        logger.warning(f"기사 ID {news_id}에 대한 전처리 데이터가 없습니다.")
        return False
    
    with get_db_connection() as conn:
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
        
    return True

@error_handler
def save_quiz_to_database(news_id, quiz_data):
    """생성된 퀴즈를 데이터베이스에 저장하고 저장된 퀴즈 수 반환"""
    if not quiz_data or "quizzes" not in quiz_data:
        logger.warning(f"기사 ID {news_id}에 대한 퀴즈 데이터가 없습니다.")
        return 0
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        saved_count = 0
        
        for quiz in quiz_data["quizzes"]:
            try:
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
                saved_count += 1
            except Exception as quiz_err:
                logger.error(f"퀴즈 항목 저장 중 오류: {quiz_err}")
                # 하나의 퀴즈 저장 오류가 전체 처리를 중단하지 않도록 함
        
        conn.commit()
        logger.info(f"기사 ID {news_id}에 대한 {saved_count}개 퀴즈 저장 완료")
        cursor.close()
        
    return saved_count

@error_handler
def generate_and_save_quizzes(news_ids):
    """기사 ID 목록에 대해 요약/키워드 추출 및 퀴즈 생성 처리 - 기사당 최소 3개의 퀴즈 생성"""
    if not news_ids:
        logger.warning("처리할 기사가 없습니다.")
        return
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        processed_count = 0
        quiz_count = 0
        
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
                processed_count += 1
                logger.info(f"기사 ID {news_id} 요약 및 키워드 저장 완료")
            else:
                logger.warning(f"기사 ID {news_id}에 대한 요약 및 키워드 추출 실패")
                # 실패하더라도 계속 진행
                
            # 2. 퀴즈 생성 (최소 3개 이상)
            logger.info(f"기사 ID {news_id} 퀴즈 생성 시작...")
            quiz_data = generate_quiz_with_claude(news_id, title, description)
            
            # 최대 3회까지 재시도
            retry_count = 0
            while (not quiz_data or 
                   "quizzes" not in quiz_data or 
                   len(quiz_data["quizzes"]) < 3) and retry_count < 3:
                logger.warning(f"기사 ID {news_id}에 대한 퀴즈 생성 부족. 재시도 {retry_count+1}/3...")
                time.sleep(2)  # API 요청 중간에 짠시 대기
                quiz_data = generate_quiz_with_claude(news_id, title, description)
                retry_count += 1
            
            if quiz_data and "quizzes" in quiz_data and len(quiz_data["quizzes"]) > 0:
                # 퀴즈 저장
                saved_quizzes = save_quiz_to_database(news_id, quiz_data)
                quiz_count += saved_quizzes
                logger.info(f"기사 ID {news_id}에 대한 {saved_quizzes}개 퀴즈 저장 완료")
            else:
                logger.error(f"기사 ID {news_id}에 대한 퀴즈 생성 실패 (최대 재시도 후)")
                
            # API 호출 제한 때문에 짠시 시간 대기
            time.sleep(2)
        
        cursor.close()
        
    logger.info(f"총 {len(news_ids)}개 기사 중 {processed_count}개 요약/키워드 처리, {quiz_count}개 퀴즈 생성 완료")

def main():
    """메인 ETL 프로세스"""
    logger.info("=== 뉴스 ETL 파이프라인 시작 ===")
    
    try:
        # 통합된 API 호출 함수 사용
        combined_df = fetch_news_from_api()
        
        if combined_df.empty:
            logger.warning("수집된 뉴스가 없습니다.")
            return
        
        # 4. 뉴스 데이터 처리
        processed_df = combined_df
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
