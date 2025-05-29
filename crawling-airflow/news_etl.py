#!/usr/bin/env python3
"""
News ETL Pipeline - Firecrawl 버전

이 스크립트는 다음 작업을 수행합니다:
1. Firecrawl API를 통해 금융/경제 관련 뉴스 데이터 수집
2. newspaper 라이브러리를 사용하여 기사 본문 추출
3. Claude API를 사용하여 뉴스 전처리 및 퀴즈 생성
4. AWS RDS에 데이터 저장
"""

import os
import re
import json
import time
import hashlib
import logging
import requests
import html
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from newspaper import Article
from dotenv import load_dotenv
import pymysql
import random
import anthropic
import logging
from contextlib import contextmanager
from functools import wraps
from urllib.parse import urlparse

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

# news_etl.py 파일에 다음 함수를 추가
def get_last_run_time():
    """최근 실행 시간을 확인하고 없으면 새로 생성"""
    last_run_file = os.path.join(os.path.expanduser("~"), "airflow", "last_news_run.txt")
    current_time = datetime.now()
    
    # 최근 24시간 기준으로 설정 (더 많은 뉴스 수집을 위해 확장)
    last_run = current_time - timedelta(hours=24)
    
    try:
        # 현재 시간으로 파일 업데이트
        with open(last_run_file, "w") as f:
            f.write(current_time.strftime("%Y-%m-%d %H:%M:%S"))
            
        logger.info(f"뉴스 데이터 수집 시간 범위: {last_run.strftime('%Y-%m-%d %H:%M:%S')} ~ {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        return last_run
    except Exception as e:
        logger.error(f"최근 실행 시간 처리 중 오류: {e}")
        return last_run

def fetch_news_from_api():
    """금융/경제 관련 뉴스 데이터 수집 - Firecrawl API만 사용"""
    logger.info("뉴스 데이터 수집 시작")
    
    # 최근 24시간 기준으로 시간 설정
    now = datetime.now()
    last_run = get_last_run_time()
    
    logger.info(f"최근 24시간 뉴스 수집: {last_run.strftime('%Y-%m-%d %H:%M')} ~ {now.strftime('%Y-%m-%d %H:%M')}")
    
    try:
        # Firecrawl API를 통해 금융/경제 관련 뉴스 수집
        firecrawl_news = fetch_firecrawl_news()
        
        if firecrawl_news.empty:
            logger.warning("수집된 뉴스가 없습니다.")
            return pd.DataFrame()
            
        logger.info(f"Firecrawl 뉴스: {len(firecrawl_news)}개 수집 완료")
        
        # 중복 제거
        firecrawl_news = firecrawl_news.drop_duplicates(subset=['url'])
        logger.info(f"중복 제거 후 {len(firecrawl_news)}개의 뉴스 데이터 수집 완료")
        
        return firecrawl_news
        
    except Exception as e:
        logger.error(f"뉴스 수집 중 오류: {e}")
        return pd.DataFrame()


# 네이버 API 관련 코드 제거 - Firecrawl API만 사용

def fetch_firecrawl_news():
    """개선된 Firecrawl API를 통해 금융/경제 관련 글로벌 뉴스 수집"""
    logger.info("Firecrawl API 데이터 수집 시작")
    
    if not FIRECRAWL_API_KEY:
        logger.error("Firecrawl API 키가 설정되지 않았습니다.")
        return pd.DataFrame()
    
    url = "https://api.firecrawl.dev/search"
    headers = {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # 금융/경제 관련 검색어 확장 - 더 많은 관련 뉴스를 가져오기 위해
    search_terms = [
        # 기본 금융/경제 용어
        "finance news", "economic news", "stock market", "investment news", "banking news", 
        "global economy", "financial markets", "trading", "bonds", "currency", "cryptocurrency",
        "financial education", "monetary policy", "inflation", "financial literacy", "market analysis",
        # 추가 금융 용어
        "central bank", "interest rates", "federal reserve", "economic growth", "financial regulation",
        "market trends", "economic indicators", "financial technology", "fintech", "banking sector",
        "investment strategy", "economic forecast", "financial crisis", "economic policy", "treasury bonds"
    ]
    
    # 제외할 키워드 - 금융/경제와 관련 없는 뉴스 필터링
    exclude_keywords = [
        # 패션/라이프스타일
        "fashion", "beauty", "clothing", "style", "runway", "designer", "luxury brand", "collection",
        "celebrity", "shopping", "accessories", "trends", "seasonal", "lifestyle", "outfit",
        # 연예/스포츠
        "celebrity", "entertainment", "movie", "music", "sports", "game", "football", "basketball",
        "baseball", "tennis", "athlete", "tournament", "concert", "festival", "award"
    ]
    
    # 제외할 도메인 목록 - 금융/경제와 관련 없는 사이트
    exclude_domains = [
        # 패션/라이프스타일 사이트
        "vogue.com", "elle.com", "harpersbazaar.com", "gq.com", "instyle.com",
        "cosmopolitan.com", "fashionista.com", "wwd.com", "refinery29.com",
        # 연예/스포츠 사이트
        "tmz.com", "espn.com", "variety.com", "hollywoodreporter.com", "eonline.com",
        "people.com", "etonline.com", "sports.yahoo.com", "goal.com", "bleacherreport.com"
    ]
    
    news_items = []
    excluded_count = 0
    
    for term in search_terms:
        payload = {
            "query": term,
            "num_results": 30,  # 더 많은 결과를 가져오기 위해 증가
            "time_period": "day",  # API 제한으로 24시간이 없어 day 사용
            "search_type": "news"
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if "results" in data and data["results"]:
                logger.info(f"'{term}' 검색어로 {len(data['results'])}개 뉴스 항목 수집")
                
                for item in data["results"]:
                    # 기본 정보 추출
                    title = item.get("title", "")
                    snippet = item.get("snippet", "")
                    url_link = item.get("url", "")
                    source = item.get("source", "")
                    published_date = item.get("published_date", "")
                    
                    # 제목과 스니펙을 소문자로 변환하여 필터링에 사용
                    title_lower = title.lower() if title else ""
                    snippet_lower = snippet.lower() if snippet else ""
                    
                    # 1. 제외 키워드 필터링
                    exclude_this = False
                    for exclude_word in exclude_keywords:
                        if exclude_word in title_lower or exclude_word in snippet_lower:
                            exclude_this = True
                            excluded_count += 1
                            logger.debug(f"제외된 기사: '{title[:30]}...' - 제외 키워드: {exclude_word}")
                            break
                    
                    if exclude_this:
                        continue
                    
                    # 2. 도메인 기반 필터링
                    if url_link:
                        try:
                            domain = urlparse(url_link).netloc
                            if domain.startswith('www.'):
                                domain = domain[4:]
                                
                            for exclude_domain in exclude_domains:
                                if exclude_domain in domain:
                                    logger.debug(f"제외된 도메인: {domain} - 기사: {title[:30]}...")
                                    excluded_count += 1
                                    exclude_this = True
                                    break
                        except Exception as e:
                            logger.warning(f"URL 파싱 오류: {e}")
                    
                    if exclude_this:
                        continue
                    
                    # 3. 발행일 파싱 및 형식화
                    formatted_date = ""
                    if published_date:
                        try:
                            # ISO 8601 형식 날짜 파싱 (2024-05-27T09:30:00Z)
                            pub_datetime = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
                            
                            # 데이터베이스에 저장할 형식으로 변환
                            formatted_date = pub_datetime.strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 최근 24시간 내 뉴스만 필터링
                            time_diff = datetime.now(tz=pub_datetime.tzinfo) - pub_datetime
                            if time_diff.total_seconds() > 24 * 3600:  # 24시간 초과
                                logger.debug(f"24시간 이상 지난 기사 제외: {title[:30]}..., 발행일: {formatted_date}")
                                continue
                                
                        except Exception as e:
                            logger.warning(f"발행일 파싱 오류: {e}. 현재 시간 사용")
                            formatted_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # 발행일이 없는 경우 현재 시간 사용
                        formatted_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 4. 뉴스 아이템 추가
                    news_items.append({
                        "title": title,
                        "description": snippet,  # 스니펙은 나중에 본문으로 대체됨
                        "url": url_link,
                        "published_date": formatted_date,  # 형식화된 날짜 저장
                        "source": source,
                        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
        df = df.drop_duplicates(subset=['url'])
        
        # 금융/경제 관련 키워드 확인
        finance_keywords = [
            'finance', 'economic', 'stock', 'invest', 'bank', 'market', 'trading', 
            'bond', 'currency', 'crypto', 'financial', 'monetary', 'inflation', 
            'economy', 'fund', 'asset', 'wealth', 'money', 'tax', 'capital',
            'interest rate', 'fed', 'recession', 'growth', 'gdp', 'earnings',
            'revenue', 'profit', 'debt', 'credit', 'loan', 'mortgage', 'portfolio'
        ]
        
        # 제목이나 설명에 금융/경제 키워드가 포함된 뉴스만 필터링
        pattern = '|'.join(finance_keywords)
        mask = (df['title'].str.contains(pattern, case=False, na=False) | 
                df['description'].str.contains(pattern, case=False, na=False))
        filtered_df = df[mask]
        
        if len(filtered_df) > 0:
            logger.info(f"총 {len(df)}개 중 {len(filtered_df)}개 금융/경제 관련 뉴스 필터링 완료 (제외된 기사: {excluded_count}개)")
            return filtered_df
        else:
            logger.warning("금융/경제 관련 뉴스가 필터링 후 없습니다. 전체 뉴스 반환")
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
    """개선된 뉴스 데이터 처리 - 기사 본문 추출 및 데이터 형식 통일"""
    logger.info(f"총 {len(df)}개 기사 처리 시작")
    
    # 1. 필드 이름 확인 및 통일
    logger.info(f"원본 데이터프레임 컴럼: {df.columns.tolist()}")
    
    # URL 필드 통일 (url 우선 사용)
    if 'url' not in df.columns and 'link' in df.columns:
        df['url'] = df['link']
        logger.info("'link' 필드를 'url' 필드로 변환")
    
    # 발행일 필드 통일 (published_date 우선 사용)
    if 'published_date' not in df.columns and 'pubDate' in df.columns:
        df['published_date'] = df['pubDate']
        logger.info("'pubDate' 필드를 'published_date' 필드로 변환")
    
    # 2. 원문 링크가 없는 기사 제외
    original_len = len(df)
    df = df[df['url'].notna() & (df['url'] != '')]
    excluded = original_len - len(df)
    if excluded > 0:
        logger.warning(f"원문 링크가 없는 {excluded}개 기사 제외됨. 남은 기사: {len(df)}개")
    
    # 3. 병렬로 기사 본문 추출 - 최대 5개 동시 처리
    urls = df['url'].tolist()
    logger.info(f"총 {len(urls)}개 기사 본문 추출 시작")
    
    # 기존 description을 임시 저장
    df['original_description'] = df['description']
    
    # 병렬로 기사 본문 추출
    successful_extraction = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 오류 처리를 위해 각 URL에 대해 처리
        future_to_idx = {executor.submit(extract_article_with_newspaper, urls[i]): i for i in range(len(urls))}
        
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                content = future.result()
                
                # 추출된 본문이 있고 유의미한 길이인 경우에만 사용
                if content and len(content) > 100:
                    df.at[idx, 'description'] = clean_news_text(content)
                    successful_extraction += 1
                    logger.info(f"기사 {idx+1}/{len(urls)} 본문 추출 성공 ({len(content)} 자)")
                else:
                    # 추출 실패 시 기존 description 유지
                    logger.warning(f"기사 {idx+1}/{len(urls)} 본문 추출 실패 - 기존 스니펙 유지")
            except Exception as e:
                logger.error(f"기사 {idx+1}/{len(urls)} 본문 추출 오류: {e}")
    
    logger.info(f"총 {len(urls)}개 중 {successful_extraction}개 기사 본문 추출 성공")
    
    # 4. source 필드 처리 - 없는 경우 URL에서 추출
    if 'source' not in df.columns or df['source'].isna().all():
        df['source'] = df['url'].apply(lambda x: urlparse(x).netloc.replace('www.', '') if pd.notna(x) else None)
        logger.info("URL에서 도메인을 추출하여 source 필드 생성")
    
    # 5. 날짜 형식 통일
    if 'published_date' in df.columns:
        # 날짜 형식 통일
        try:
            df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
            df['published_date'] = df['published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            logger.info("published_date 필드 형식 통일 완료")
        except Exception as e:
            logger.error(f"published_date 형식 변환 오류: {e}")
    
    # 6. 필요한 필드만 선택하여 최종 데이터프레임 구성
    required_columns = ['title', 'description', 'url', 'published_date', 'source']
    
    # 필요한 컴럼이 없는 경우 추가
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
            logger.warning(f"'{col}' 필드가 없어 빈 값으로 추가")
    
    # 키워드와 요약 컴럼 추가
    if 'keywords' not in df.columns:
        df['keywords'] = ''
    if 'summary' not in df.columns:
        df['summary'] = ''
    
    # 현재 시간 추가 (없는 경우에만)
    if 'created_at' not in df.columns:
        df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 필요한 컴럼만 선택
    final_columns = required_columns + ['keywords', 'summary', 'created_at']
    df = df[final_columns]
    
    # 디버깅을 위한 컴럼 확인
    logger.info(f"최종 데이터프레임 컴럼: {df.columns.tolist()}")
    logger.info(f"처리 완료된 기사 수: {len(df)}")
    
    return df


@error_handler
def preprocess_news_with_claude(news_id, title, description):
    """기사 내용을 기반으로 Claude API를 이용해 정확한 요약 및 키워드 추출"""
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 처리할 수 없습니다.")
        return None
        
    prompt = f"""
    당신은 경제·금융 교육을 위한 NIE 콘텐츠 제작자입니다. 다음 기사에서 가장 중요한 금융/경제 키워드를 추출하고, 요점을 정확하게 요약해주세요.
    
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
    당신은 경제·금융 교육을 위한 NIE 콘텐츠 제작자입니다. 다음 기사를 기반으로 학습자들이 금융 개념을 이해할 수 있는 교육용 퀴즈를 만들어주세요.
    
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
                    quiz[field] = "NORMAL"  # 기본값 NORMAL
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
def clean_news_text(text):
    """개선된 뉴스 텍스트 전처리 - HTML 태그, 특수 문자, 불필요한 텍스트 제거"""
    if pd.isna(text) or text is None or not isinstance(text, str):
        return ""
    
    # HTML 태그 제거
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # HTML 엔티티 디코딩 (&quot;, &amp; 등)
    text = html.unescape(text)
    
    # URL 제거
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text)
    
    # 여론조사 관련 텍스트 제거
    text = re.sub(r'\[\s*여론조사\s*\].*?\[\s*여론조사\s*\]', ' ', text)
    text = re.sub(r'\[\s*여론조사\s*\].*?$', ' ', text)
    
    # 인터뷰 표시 문자 제거
    text = re.sub(r'[Q]\s*:', ' ', text)
    text = re.sub(r'[A]\s*:', ' ', text)
    
    # 인터뷰어 표시 문자 제거 ([기자], [앵커] 등)
    brackets_patterns = [
        r'\[\s*기자\s*\]', r'\[\s*앵커\s*\]', r'\[\s*인터뷰\s*\]', 
        r'\[\s*인터뷰어\s*\]', r'\[\s*인터뷰이\s*\]', r'\[\s*사진\s*\]', 
        r'\[\s*이미지\s*\]', r'\[\s*자료\s*\]', r'\[\s*자료사진\s*\]', 
        r'\[\s*그래픽\s*\]', r'\[\s*사진=.*?\s*\]', r'\[\s*이미지=.*?\s*\]',
        r'\[\s*자료=.*?\s*\]', r'\[\s*그래픽=.*?\s*\]', r'\[\s*사진출처=.*?\s*\]',
        r'\[\s*이미지출처=.*?\s*\]', r'\[\s*자료출처=.*?\s*\]'
    ]
    
    for pattern in brackets_patterns:
        text = re.sub(pattern, ' ', text)
    
    # 일반적인 뉴스 관련 불필요 텍스트 제거
    news_patterns = [
        r'\(종합\s*\)', r'\(종합=.*?\)', r'\(종합=.*?\s*\)',
        r'\(연합뉴스\s*\)', r'\(연합뉴스=.*?\)', 
        r'\(연합\s*\)', r'\(연합=.*?\)',
        r'\(인터뷰\s*\)', r'\(인터뷰=.*?\)',
        r'\(인터뷰어\s*\)', r'\(인터뷰어=.*?\)',
        r'\(인터뷰이\s*\)', r'\(인터뷰이=.*?\)',
        r'\(인터뷰\s*=.*?\)', r'\(인터뷰어\s*=.*?\)', r'\(인터뷰이\s*=.*?\)',
        r'\(사진=.*?\)', r'\(이미지=.*?\)', r'\(자료=.*?\)', r'\(그래픽=.*?\)',
        r'\(사진\s*\)', r'\(이미지\s*\)', r'\(자료\s*\)', r'\(그래픽\s*\)',
        r'\(사진\s*=.*?\)', r'\(이미지\s*=.*?\)', r'\(자료\s*=.*?\)', r'\(그래픽\s*=.*?\)',
        r'\(사진출처=.*?\)', r'\(이미지출처=.*?\)', r'\(자료출처=.*?\)',
        r'\(사진출처\s*=.*?\)', r'\(이미지출처\s*=.*?\)', r'\(자료출처\s*=.*?\)',
        r'\(사진 출처=.*?\)', r'\(이미지 출처=.*?\)', r'\(자료 출처=.*?\)',
        r'\(\s*사진\s*\)', r'\(\s*이미지\s*\)', r'\(\s*자료\s*\)', r'\(\s*그래픽\s*\)',
        r'\(\s*사진=.*?\)', r'\(\s*이미지=.*?\)', r'\(\s*자료=.*?\)', r'\(\s*그래픽=.*?\)',
        r'\(\s*사진\s*=.*?\)', r'\(\s*이미지\s*=.*?\)', r'\(\s*자료\s*=.*?\)', r'\(\s*그래픽\s*=.*?\)',
        r'\(\s*사진출처=.*?\)', r'\(\s*이미지출처=.*?\)', r'\(\s*자료출처=.*?\)',
        r'\(\s*사진출처\s*=.*?\)', r'\(\s*이미지출처\s*=.*?\)', r'\(\s*자료출처\s*=.*?\)',
        r'\(\s*사진 출처=.*?\)', r'\(\s*이미지 출처=.*?\)', r'\(\s*자료 출처=.*?\)'
    ]
    
    for pattern in news_patterns:
        text = re.sub(pattern, ' ', text)
    
    # 여러 줄 개행 문자를 공백으로 치환
    text = re.sub(r'\n+', ' ', text)
    
    # 여러 개의 공백 문자를 하나로 추소
    text = re.sub(r'\s+', ' ', text)
    
    # 특수 문자 제거 (일부 유지)
    text = re.sub(r'[^\w\s\.,;:\-\(\)\[\]\/\?!@#$%&*+=_\'\"ㄱ-ㅣ가-힣]', '', text)
    
    # 공백 정리 및 양쪽 공백 제거
    text = text.strip()
    
    # 여전히 너무 긴 텍스트는 적절한 길이로 잘라냄
    max_length = 10000  # 최대 10,000자로 제한
    if len(text) > max_length:
        text = text[:max_length] + '...'
    
    return text

def save_news_to_database(df):
    """개선된 뉴스 데이터베이스 저장 함수 - published_date 추가 및 중복 검사 강화"""
    if df.empty:
        logger.warning("저장할 뉴스 데이터가 없습니다.")
        return []
    
    # 데이터프레임 컴럼 확인
    logger.info(f"저장할 데이터프레임 컴럼: {df.columns.tolist()}")
    
    # 텍스트 전처리 적용
    if 'title' in df.columns:
        df['title'] = df['title'].apply(clean_news_text)
    if 'description' in df.columns:
        df['description'] = df['description'].apply(clean_news_text)
    
    logger.info("텍스트 전처리 완료: HTML 태그, 불필요한 텍스트 제거")
    
    # 데이터베이스에 추가할 뉴스 ID를 저장할 목록
    inserted_news_ids = []
    total_articles = len(df)
    skipped_articles = 0
    error_articles = 0
    
    # 데이터베이스 테이블 구조 확인
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # 테이블 구조 확인
        try:
            cursor.execute("DESCRIBE news")
            table_columns = [column[0] for column in cursor.fetchall()]
            logger.info(f"news 테이블 구조: {table_columns}")
            
            # published_date 컴럼이 없는 경우 추가 (테이블 변경 권한이 있는 경우에만 작동)
            if 'published_date' not in table_columns:
                try:
                    cursor.execute("ALTER TABLE news ADD COLUMN published_date DATETIME AFTER created_at")
                    conn.commit()
                    logger.info("news 테이블에 published_date 컴럼 추가 완료")
                    table_columns.append('published_date')
                except Exception as alter_error:
                    logger.error(f"published_date 컴럼 추가 실패: {alter_error}")
        except Exception as e:
            logger.error(f"테이블 구조 확인 오류: {e}")
            table_columns = ['title', 'description', 'created_at', 'source', 'url', 'article_hash', 'keywords', 'summary']
        
        # 기존 기사 해시 및 URL 로드
        article_hashes = set()
        article_urls = set()
        
        try:
            cursor.execute("SELECT article_hash, url FROM news")
            existing_data = cursor.fetchall()
            for hash_val, url in existing_data:
                article_hashes.add(hash_val)
                article_urls.add(url)
            logger.info(f"기존 기사 {len(article_urls)}개 로드 완료")
        except Exception as e:
            logger.error(f"기존 기사 로드 오류: {e}")
        
        # 각 기사 처리
        for _, row in df.iterrows():
            try:
                # 필수 필드 추출
                title = row['title'] if not pd.isna(row['title']) else ''
                description = row['description'] if not pd.isna(row['description']) else ''
                url = row['url'] if not pd.isna(row['url']) else ''
                source = row['source'] if not pd.isna(row['source']) else ''
                
                # 기타 필드 추출
                created_at = row['created_at'] if 'created_at' in row and not pd.isna(row['created_at']) else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                published_date = row['published_date'] if 'published_date' in row and not pd.isna(row['published_date']) else None
                keywords = row['keywords'] if 'keywords' in row and not pd.isna(row['keywords']) else ''
                summary = row['summary'] if 'summary' in row and not pd.isna(row['summary']) else ''
                
                # URL이 없는 기사는 건너뜀
                if not url:
                    logger.warning(f"URL이 없는 기사 건너뜀: {title[:30]}...")
                    skipped_articles += 1
                    continue
                
                # 중복 여부 확인 (URL 및 해시 기반)
                article_hash = hashlib.md5(url.encode()).hexdigest()
                
                # 이미 저장된 기사인지 검사 (해시 또는 URL 기반)
                if article_hash in article_hashes or url in article_urls:
                    logger.info(f"중복 기사 건너뜀: {title[:30]}...")
                    skipped_articles += 1
                    continue
                
                # 저장할 필드 준비
                insert_fields = []
                insert_values = []
                insert_placeholders = []
                
                # 필수 필드 추가
                base_fields = {
                    'title': title,
                    'description': description,
                    'url': url,
                    'source': source,
                    'article_hash': article_hash,
                    'created_at': created_at,
                    'keywords': keywords,
                    'summary': summary
                }
                
                # published_date 필드 추가 (테이블에 있는 경우에만)
                if 'published_date' in table_columns and published_date:
                    base_fields['published_date'] = published_date
                
                # 추가 필드 추가
                if 'keywords' in table_columns:
                    base_fields['keywords'] = keywords
                if 'summary' in table_columns:
                    base_fields['summary'] = summary
                
                # SQL 쿼리 생성
                for field, value in base_fields.items():
                    if field in table_columns:  # 테이블에 있는 필드만 추가
                        insert_fields.append(field)
                        insert_values.append(value)
                        insert_placeholders.append('%s')
                
                # 삽입 쿼리 실행
                insert_query = f"INSERT INTO news ({', '.join(insert_fields)}) VALUES ({', '.join(insert_placeholders)})"
                cursor.execute(insert_query, insert_values)
                
                # 방금 삽입한 레코드의 ID 가져오기
                news_id = cursor.lastrowid
                inserted_news_ids.append(news_id)
                
                # 해시와 URL 등록
                article_hashes.add(article_hash)
                article_urls.add(url)
                
                logger.debug(f"기사 저장 성공 (ID: {news_id}): {title[:30]}...")
                
            except Exception as insert_error:
                logger.error(f"기사 저장 중 오류 발생: {insert_error}")
                error_articles += 1
                # 개별 기사 저장 실패가 전체 처리를 막지 않도록 함
                continue
        
        # 변경사항 적용
        conn.commit()
        cursor.close()
    
    logger.info(f"총 {total_articles}개 중 {len(inserted_news_ids)}개의 새 기사가 데이터베이스에 저장되었습니다.")
    logger.info(f"중복 건너뜀: {skipped_articles}개, 오류 발생: {error_articles}개")
    
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