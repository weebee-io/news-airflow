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
import re
import hashlib
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


# news_etl.py 파일에 다음 함수를 추가
def is_first_run():
    """최초 실행 여부를 확인"""
    first_run_marker = os.path.join(os.path.expanduser("~"), "airflow", "first_run_completed.txt")
    if not os.path.exists(first_run_marker):
        # 최초 실행 시 파일 생성
        with open(first_run_marker, "w") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return True
    return False

def fetch_news_from_api():
    """여러 API에서 뉴스 데이터 수집"""
    logger.info("뉴스 데이터 수집 시작")
    
    # 최초 실행 여부에 따라 검색 기간 결정
    if is_first_run():
        logger.info("최초 실행: 최근 7일간의 뉴스 수집")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
    else:
        logger.info("일반 실행: 최근 12시간의 뉴스 수집")
        start_date = (datetime.now() - timedelta(hours=12)).strftime("%Y%m%d")
    
    end_date = datetime.now().strftime("%Y%m%d")
    
    # API 결과 저장
    results = []
    
    try:
        # 네이버 뉴스 API 호출 시 날짜 범위 적용
        naver_finance = fetch_naver_news("금융", start_date=start_date, end_date=end_date)
        if not naver_finance.empty:
            logger.info(f"네이버 금융 뉴스: {len(naver_finance)}개 수집")
            results.append(naver_finance)
        
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


def fetch_naver_news(query="", start_date=None, end_date=None, display=100):
    """네이버 뉴스 API를 통해 금융/경제 관련 뉴스 수집 - 업데이트된 필터링 적용"""
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        logger.error("네이버 API 키가 설정되지 않았습니다.")
        return pd.DataFrame()
    
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET
    }
    
    # 날짜 범위 쿼리 추가
    date_query = ""
    if start_date and end_date:
        date_query = f" after:{start_date} before:{end_date}"
    
    # 금융/경제 관련 핵심 키워드 - 더 포커스를 맞춰 업데이트
    search_terms = [
        "금융 경제", "주식 시장", "금리 정책", "경제 전망", "투자 전략",
        "인플레이션 경제", "외환 시장", "글로벌 금융", "부동산 시장", "펀드 투자",
        "가상화폐 시장", "재테크 전략", "기업 재무", "세금 정책", "은행 상품"
    ]
    
    # 제외할 키워드 - 금융/경제와 관련 없는 뉴스 필터링
    exclude_keywords = [
        "연예", "엽고", "연예인", "방송", "드라마", "스포츠", "게임", "영화",
        "정치", "사건", "사고", "범죄", "주제", "의학", "코로나", "서비스 중단"
    ]
    
    news_items = []
    total_fetched = 0
    included_count = 0
    excluded_count = 0
    
    for term in search_terms:
        # 검색어 인코딩
        encText = urllib.parse.quote(term)
        search_query = encText + date_query
        
        params = {
            "query": search_query,
            "display": display,
            "start": 1,
            "sort": "date"  # 최신순 정렬로 변경
        }

         # 날짜 범위가 있으면 추가
        if start_date:
            params["start_date"]
        if end_date:
            params["end_date"]
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                result = response.json()
                
                if 'items' in result:
                    items = result['items']
                    total_fetched += len(items)
                    
                    for item in items:
                        # HTML 태그 제거
                        title = re.sub('<.*?>', '', item['title'])
                        description = re.sub('<.*?>', '', item['description'])
                        
                        # 제외 키워드 검사 - 하나라도 있으면 제외
                        exclude_this = False
                        for exclude_word in exclude_keywords:
                            if exclude_word in title.lower() or exclude_word in description.lower():
                                exclude_this = True
                                excluded_count += 1
                                break
                        
                        if exclude_this:
                            continue
                            
                        # 금융/경제 관련 키워드가 있는지 추가 확인
                        finance_terms = ["금융", "경제", "시장", "주식", "투자", "자산", "금리",
                                      "채권", "외환", "기업", "재무", "펀드", "세금", "정책"]
                        
                        has_finance_term = False
                        for finance_term in finance_terms:
                            if finance_term in title or finance_term in description[:100]:  # 제목이나 요약 시작 부분에 금융 키워드가 있어야 함
                                has_finance_term = True
                                break
                                
                        if not has_finance_term:
                            excluded_count += 1
                            continue
                        
                        # 네이버 뉴스 링크에서 원본 URL 추출
                        original_url = item.get('originallink', item['link'])
                        if not original_url or original_url == "":
                            original_url = item['link']
                            
                        # 사이트 이름 추출
                        source = ""
                        try:
                            url_parts = urllib.parse.urlparse(original_url)
                            source = url_parts.netloc
                            if source.startswith('www.'):
                                source = source[4:]
                        except:
                            source = item.get('originallink', '').split('/')[2] if 'originallink' in item and '/' in item['originallink'] else ''
                        
                        # 발행일 형식화
                        published_date = None
                        try:
                            # 네이버 뉴스 형식: Mon, 20 May 2024 09:00:00 +0900
                            published_date = datetime.strptime(item['pubDate'], '%a, %d %b %Y %H:%M:%S %z')
                            published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            published_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 생성된 뉴스 아이템에 저장
                        news_items.append({
                            'title': title,
                            'description': description,
                            'url': original_url,  # 원본 URL 사용
                            'published_date': published_date,
                            'source': source,
                            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        included_count += 1
                        print("----------------",  news_items)
            else:
                logger.error(f"네이버 API 오류 발생: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"네이버 뉴스 크롤링 중 오류 발생: {e}")
            
        # API 요청 사이 짠시 대기
        time.sleep(0.2)
    
    # 데이터프레임 생성
    df = pd.DataFrame(news_items)
    
    if not df.empty:
        # 중복 제거
        df = df.drop_duplicates(subset=['url'])
        
        logger.info(f"총 {total_fetched}개 기사 중 {included_count}개의 금융/경제 관련 네이버 뉴스 기사를 가져왔습니다. (제외된 기사: {excluded_count}개)")
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
    
    # 검색어 리스트 (영어) - 금융/경제 키워드 추가
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
    logger.info(f"총 {len(df)}개 기사 본문 추출 시작")
    
    # 날짜 형식 통일
    if 'pubDate' in df.columns:
        df['pubDate'] = pd.to_datetime(df['pubDate'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # 필드 이름 통일 (link가 없으면 url 사용)
    if 'link' not in df.columns and 'url' in df.columns:
        df['link'] = df['url']
    elif 'url' not in df.columns and 'link' in df.columns:
        df['url'] = df['link']
        
    # 병렬로 기사 본문 추출
    urls = df['url'].tolist() if 'url' in df.columns else df['link'].tolist()
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
    
    # 원문 링크가 없는 기사 제외
    original_len = len(df)
    if 'url' in df.columns:
        df = df[df['url'].notna() & (df['url'] != '')]
    else:
        df = df[df['link'].notna() & (df['link'] != '')]
    excluded = original_len - len(df)
    if excluded > 0:
        logger.warning(f"원문 링크가 없는 {excluded}개 기사 제외됨. 남은 기사: {len(df)}개")
    
    # 데이터베이스 구조에 맞게 컬럼 이름 변경
    column_mapping = {
        'title': 'title',
        'description': 'description',
        'link': 'url',
        'pubDate': 'published_date',
        'source': 'source',
        'keywords': 'keywords',
        'summary': 'summary'
    }
    
    # 디버깅을 위한 컬럼 확인
    logger.info(f"데이터프레임 컬럼: {df.columns.tolist()}")
    
    # 필요한 컬럼만 선택하고 이름 변경
    df = df.rename(columns=column_mapping)
    
    # source 컬럼이 없는 경우 URL에서 도메인 추출하여 source로 사용
    if 'source' not in df.columns:
        from urllib.parse import urlparse
        df['source'] = df['url'].apply(lambda x: urlparse(x).netloc if pd.notna(x) else None)
        logger.info("URL에서 도메인을 추출하여 source 컬럼 생성")
    
    # 필요한 컬럼만 선택 (만약 키워드와 요약이 없는 경우 빈 문자열로 추가)
    base_columns = ['title', 'description', 'url', 'published_date', 'source']
    df = df[base_columns]
    
    # 키워드와 요약 컬럼 추가
    if 'keywords' not in df.columns:
        df['keywords'] = ''
    if 'summary' not in df.columns:
        df['summary'] = ''
    
    # 현재 시간 추가
    df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return df


def preprocess_news_with_claude(news_id, title, description):
    """기사 내용을 기반으로 Claude API를 이용해 정확한 요약 및 키워드 추출"""
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키가 설정되지 않았습니다.")
        return None
        
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 처리할 수 없습니다.")
        return None
        
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
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
        
        message = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=1000,
            temperature=0.0,  # 정확한 응답을 위해 낮은 온도 사용
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # 응답에서 JSON 부분만 추출
        response_text = message.content[0].text
        
        # JSON 형식 추출
        json_start = response_text.find('{')
        json_end = response_text.rfind('}')
        
        if json_start == -1 or json_end == -1:
            logger.error(f"기사 ID {news_id}: JSON 형식을 찾을 수 없습니다.")
            logger.debug(f"Claude 응답: {response_text[:200]}...")
            return None
            
        json_str = response_text[json_start:json_end+1]
        
        try:
            result = json.loads(json_str)
            
            # 기본값 확인 및 설정
            if "keywords" not in result or not result["keywords"]:
                result["keywords"] = ""
                logger.warning(f"기사 ID {news_id}: 키워드가 없어 빈 문자열로 설정합니다.")
                
            if "summary" not in result or not result["summary"]:
                result["summary"] = ""
                logger.warning(f"기사 ID {news_id}: 요약문이 없어 빈 문자열로 설정합니다.")
                
            logger.info(f"기사 ID {news_id}: 키워드 및 요약 생성 완료")
            return result
            
        except json.JSONDecodeError as json_err:
            logger.error(f"기사 ID {news_id}: JSON 파싱 오류 - {json_err}")
            logger.debug(f"JSON 데이터: {json_str[:200]}...")
            return None
            
    except Exception as e:
        logger.error(f"기사 ID {news_id}: Claude 요약/키워드 추출 중 오류 - {e}")
        return None

def generate_quiz_with_claude(news_id, title, description):
    """Claude API를 사용하여 뉴스 기사 기반 퀴즈 생성 - 기사당 최소 3개 문제 생성"""
    if not ANTHROPIC_API_KEY:
        logger.error("Claude API 키가 설정되지 않았습니다.")
        return None
    
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 퀴즈를 생성할 수 없습니다.")
        return None
    
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
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
                    "newsquiz_level": "medium", # easy, medium, hard 중 하나
                    "reason": "정답 이유 설명"
                }},
                # 추가 퀴즈...
            ]
        }}
        
        중요: 반드시 최소 3개 이상의 퀴즈를 제작해야 합니다. 각 퀴즈는 다양한 난이도와 주제를 다루어야 합니다.
        """
        
        message = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=4000,
            temperature=0.7,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # 응답에서 JSON 부분만 추출
        response_text = message.content[0].text
        
        # JSON 형식 추출
        json_start = response_text.find('{')
        json_end = response_text.rfind('}')
        
        if json_start == -1 or json_end == -1:
            logger.error(f"기사 ID {news_id}: JSON 형식을 찾을 수 없습니다.")
            logger.debug(f"Claude 응답: {response_text[:200]}...")
            return None
            
        json_str = response_text[json_start:json_end+1]
        
        try:
            quiz_data = json.loads(json_str)
            
            # 최소 3개 이상의 퀴즈가 있는지 확인
            if "quizzes" not in quiz_data or len(quiz_data["quizzes"]) < 3:
                logger.warning(f"기사 ID {news_id}: 퀴즈가 3개 미만입니다. (생성된 퀴즈: {len(quiz_data.get('quizzes', []))}개)")
                # 부족하지만 있는 퀴즈라도 반환
            
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
                            quiz[field] = "medium"  # 기본값 medium
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
            
        except json.JSONDecodeError as json_err:
            logger.error(f"기사 ID {news_id}: JSON 파싱 오류 - {json_err}")
            logger.debug(f"JSON 데이터: {json_str[:200]}...")
            return None
            
    except Exception as e:
        logger.error(f"기사 ID {news_id}: Claude 퀴즈 생성 중 오류 - {e}")
        return None

def save_news_to_database(df):
    """뉴스 데이터프레임을 데이터베이스에 저장 - 중복 기사 확인 및 새 기사만 추가"""
    if df.empty:
        logger.warning("저장할 뉴스 데이터가 없습니다.")
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
        
        # 데이터베이스에 추가할 뉴스 ID를 저장할 목록
        inserted_news_ids = []
        total_articles = len(df)
        skipped_articles = 0
        
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
        conn.close()
        
        logger.info(f"총 {total_articles}개 중 {len(inserted_news_ids)}개의 새 기사가 데이터베이스에 저장되었습니다. (중복 건너뜀: {skipped_articles}개)")
        return inserted_news_ids
        
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
    """생성된 퀴즈를 데이터베이스에 저장하고 저장된 퀴즈 수 반환"""
    if not quiz_data or "quizzes" not in quiz_data:
        logger.warning(f"기사 ID {news_id}에 대한 퀴즈 데이터가 없습니다.")
        return 0
    
    try:
        conn = pymysql.connect(
            host=DB_IP,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        
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
        conn.close()
        
        return saved_count
        
    except Exception as e:
        logger.error(f"퀴즈 데이터베이스 저장 중 오류 발생: {e}")
        return 0

def generate_and_save_quizzes(news_ids):
    """기사 ID 목록에 대해 요약/키워드 추출 및 퀴즈 생성 처리 - 기사당 최소 3개의 퀴즈 생성"""
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
        conn.close()
        
        logger.info(f"총 {len(news_ids)}개 기사 중 {processed_count}개 요약/키워드 처리, {quiz_count}개 퀴즈 생성 완료")
        
    except Exception as e:
        logger.error(f"퀴즈 생성 및 저장 중 오류 발생: {e}")

def main():
    """메인 ETL 프로세스"""
    logger.info("=== 뉴스 ETL 파이프라인 시작 ===")
    
    try:
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
