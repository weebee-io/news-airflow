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
import feedparser
import pandas as pd
import time

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
            model="claude-3-5-sonnet-20241022",
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

def clean_article_text(text):
    """기사 텍스트에서 불필요한 내용 제거"""
    if not text:
        return text
        
    # 제거할 패턴 목록
    patterns_to_remove = [
        r"이미지=.*?(\n|$)",  # '이미지=챗GPT 4o' 등의 패턴
        r"/게티이미지뱅크.*?(\n|$)",  # '/게티이미지뱅크' 패턴
        r"\(사진=.*?\)",  # '(사진=...)' 패턴
        r"사진=.*?(\n|$)",  # '사진=...' 패턴
        r"자료=.*?(\n|$)",  # '자료=...' 패턴
        r"자료사진=.*?(\n|$)",  # '자료사진=...' 패턴
        r"제공=.*?(\n|$)",  # '제공=...' 패턴
        r"출처=.*?(\n|$)",  # '출처=...' 패턴
        r"그래픽=.*?(\n|$)",  # '그래픽=...' 패턴
        r"편집=.*?(\n|$)",  # '편집=...' 패턴
        r"기자 = .*?(\n|$)",  # '기자 = ...' 패턴
        r"기자=.*?(\n|$)",  # '기자=...' 패턴
        r".*?뉴스티앤티.*?(\n|$)",  # '...뉴스티앤티' 패턴
        r"\[.*?\] ",  # '[태그]' 형태의 태그
        r"\(.*?\) ",  # '(태그)' 형태의 태그
        r"【.*?】",  # '【태그】' 형태의 태그
        r"\n\s*\n+"  # 연속된 빈 줄을 하나로 통합
    ]
    
    import re
    # 각 패턴을 순차적으로 적용하여 텍스트 정리
    cleaned_text = text
    for pattern in patterns_to_remove:
        cleaned_text = re.sub(pattern, '\n', cleaned_text)
    
    # 연속된 공백 제거 및 공백 정리
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    
    # 시작과 끝의 공백 제거
    cleaned_text = cleaned_text.strip()
    
    return cleaned_text

def translate_text_with_claude(text, source_lang='en', target_lang='ko'):
    """Claude API를 사용하여 텍스트를 번역"""
    if not text or len(text.strip()) < 10:
        return text

    try:
        prompt = f"""
        당신은 전문 번역가입니다. 아래 {source_lang} 텍스트를 자연스러운 {target_lang}로 번역해주세요.
        원문의 의미와 뉘앙스를 정확히 유지하면서, 목표 언어의 자연스러운 표현으로 번역하는 것이 중요합니다.
        금융/경제 용어는 한국에서 통용되는 전문 용어로 정확하게 번역해주세요.
        
        원문:
        """
        prompt += text[:10000]  # 길이 제한
        prompt += """
        
        번역문만 제공해주세요. 다른 설명이나 주석은 포함하지 마세요.
        """
        
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=4000,
            temperature=0.0,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # 전체 응답을 번역문으로 사용
        translated_text = message.content[0].text.strip()
        return translated_text
        
    except Exception as e:
        logger.error(f"번역 중 오류 발생: {e}")
        return text  # 오류 발생 시 원본 반환
        
def translate_text_with_claude_combined(title, content, source_lang='en', target_lang='ko'):
    """Claude API를 사용하여 제목과 내용을 하나의 호출로 번역"""
    if not title and not content:
        return title, content
        
    content_to_translate = content[:3000] if len(content) > 3000 else content
    
    try:
        prompt = f"""
        당신은 금융/경제 전문 번역가입니다. 아래 {source_lang} 제목과 내용을 자연스러운 {target_lang}로 번역해주세요.
        원문의 의미와 뉘앙스를 정확히 유지하면서, 목표 언어의 자연스러운 표현으로 번역하는 것이 중요합니다.
        금융/경제 용어는 한국에서 통용되는 전문 용어로 정확하게 번역해주세요.
        
        제목: {title}
        내용: {content_to_translate}
        
        다음 JSON 형식으로만 응답해주세요:
        {{
            "translated_title": "번역된 제목",
            "translated_content": "번역된 내용"
        }}
        """
        
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=4000,
            temperature=0.0,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # JSON 형식 추출
        response_text = message.content[0].text
        json_start = response_text.find('{')
        json_end = response_text.rfind('}')
        
        if json_start == -1 or json_end == -1:
            logger.error("번역 결과에서 JSON 형식을 찾을 수 없습니다.")
            return title, content
            
        json_str = response_text[json_start:json_end+1]
        
        try:
            result = json.loads(json_str)
            translated_title = result.get("translated_title", title)
            translated_content = result.get("translated_content", content)
            return translated_title, translated_content
        except json.JSONDecodeError as json_err:
            logger.error(f"번역 결과 JSON 파싱 오류: {json_err}")
            return title, content
            
    except Exception as e:
        logger.error(f"통합 번역 중 오류 발생: {e}")
        return title, content  # 오류 발생 시 원본 반환

def fetch_news_from_api():
    logger.info("뉴스 데이터 수집 시작")
    
    now = datetime.now()
    
    logger.info(f"뉴스 수집: {now.strftime('%Y-%m-%d %H:%M')}")
    
    # API 결과 저장
    results = []
    
    try:
        # eng_rss에서 뉴스 수집 (네이버 뉴스 대체)
        kor_rss_news = fetch_kor_rss_news()
        if not kor_rss_news.empty:
            logger.info(f"kor_rss 뉴스: {len(kor_rss_news)}개 수집")
            results.append(kor_rss_news)
        
        # 추가 뉴스 소스도 같은 방식으로 수정
        eng_rss_news = fetch_eng_rss_news()
        if not eng_rss_news.empty:
            logger.info(f"Firecrawl 뉴스: {len(eng_rss_news)}개 수집")
            results.append(eng_rss_news)
        
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

def fetch_eng_rss_news():
    """CNBC
    eng_rss 피드를 통해 금융/경제 관련 뉴스 수집 및 한국어 번역"""
    import requests
    import pandas as pd

    # CNBC RSS 피드 URL 목록
    rss_urls = [
        'https://www.cnbc.com/id/20910258/device/rss/rss.html',  # Finance
        'https://www.cnbc.com/id/10000664/device/rss/rss.html',  # Markets
        'https://www.cnbc.com/id/10000115/device/rss/rss.html',  # Economy
    ]
        
    # 기사 본문 가져오는 함수
    def get_article_content(url):
        try:
            article = Article(url, language='en')
            article.download()
            article.parse()
            return article.text.strip()
        except Exception as e:
            print(f"[오류] {url}: {e}")
            return ''

    # 모든 피드에서 뉴스 수집
    news_items = []
    
    for rss_url in rss_urls:
        # RSS 피드 파싱
        feed = feedparser.parse(rss_url)
        
        # 파싱 결과를 리스트로 변환
        for entry in feed.entries:
            title = entry.title
            link = entry.link
            published = datetime(*entry.published_parsed[:6]) if 'published_parsed' in entry else None
            summary = entry.summary if 'summary' in entry else ''
            
            # 기사 본문 크롤링
            content = get_article_content(link)
            
            # 불필요한 텍스트 제거
            content = clean_article_text(content)
            
            # 중요 금융/경제 키워드 목록
            important_keywords = ['economy', 'market', 'finance', 'stock', 'investment', 'bond', 'interest rate', 
                                'inflation', 'fed', 'federal reserve', 'nasdaq', 'dow jones', 'earnings', 'treasury',
                                'gdp', 'recession', 'banking', 'crypto', 'currency']
                                
            # 제목이나 내용에 중요 키워드가 포함된 경우에만 번역
            should_translate = len(content) > 200 and any(keyword in title.lower() or keyword in content.lower()[:500] 
                                                          for keyword in important_keywords)
                                                          
            if should_translate:  # 중요 키워드가 있고 충분한 내용이 있는 경우만 번역 시도
                # 영어 원문 보존
                original_content = content
                
                # 제목과 내용을 한번에 번역
                logger.info(f"CNBC 기사 번역 시작: {title[:30]}...")
                translated_title, translated_content = translate_text_with_claude_combined(title, content, 'en', 'ko')
                
                # 번역 결과 사용
                title = translated_title
                description = translated_content
                
                # 원문도 저장 (필요 시 사용)
                summary = f"[원문] {original_content[:500]}..."
            else:
                description = content
                
            created_at = datetime.now()
            source = 'CNBC'
            
            # 해시 생성 (기사 원문 기준)
            article_hash = hashlib.sha1(description.encode('utf-8')).hexdigest()[:45]

            news_items.append({
                'created_at': created_at,
                'description': description,
                'published_date': published,
                'source': source,
                'title': title,
                'url': link,
                'summary': summary,
                'article_hash': article_hash
            })

            time.sleep(1)  # 과도한 요청 방지

    # DataFrame 생성
    df = pd.DataFrame(news_items)

    # 전처리: description이 비어있거나 중복된 기사 제거
    df = df[df['description'].str.strip() != '']
    df = df.drop_duplicates(subset=['article_hash'])
    df = df.tail(10) ############### 나중에 변경 - 각 피드에서 1개씩 테스트
    return df


def fetch_kor_rss_news():
    """kor_rss 피드를 통해 금융/경제 관련 뉴스 수집"""
    import requests
    import pandas as pd

    # 한국경제 RSS 피드 URL 목록
    rss_urls = [
        'https://www.hankyung.com/feed/economy',     # 경제
        'https://www.hankyung.com/feed/realestate',  # 부동산
        'https://www.hankyung.com/feed/finance',     # 금융
    ]
        
    # 기사 본문 가져오는 함수
    def get_article_content(url):
        try:
            article = Article(url, language='ko')
            article.download()
            article.parse()
            return article.text.strip()
        except Exception as e:
            print(f"[오류] {url}: {e}")
            return ''

    # 모든 피드에서 뉴스 수집
    news_items = []
    
    for rss_url in rss_urls:
        # RSS 피드 파싱
        feed = feedparser.parse(rss_url)
        
        # 파싱 결과를 리스트로 변환 (각 피드당 최대 2개씩)
        for entry in feed.entries[:2]:
            title = entry.title
            link = entry.link
            summary = entry.summary if 'summary' in entry else ''
            
            # 기사 본문 크롤링
            content = get_article_content(link)
            
            # 불필요한 텍스트 제거
            description = clean_article_text(content)
            if hasattr(entry, 'published'):
                try:
                    published_date = datetime.strptime(entry.published, "%a, %d %b %Y %H:%M:%S %z")
                    published_date = published_date.strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f"[오류] published_date 파싱 실패: {e}")
                    published_date = None
            source = '한국경제'
            
            # 해시 생성 (기사 원문 기준)
            article_hash = hashlib.sha1(description.encode('utf-8')).hexdigest()[:45]

            news_items.append({
                'created_at': datetime.now(),
                'description': description,
                'published_date': published_date,
                'source': source,
                'title': title,
                'url': link,
                'summary': summary,
                'article_hash': article_hash
            })
            time.sleep(1)  # 과도한 요청 방지

    # DataFrame 생성
    df = pd.DataFrame(news_items)

    # 전처리: description이 비어있거나 중복된 기사 제거
    df = df[df['description'].str.strip() != '']
    df = df.drop_duplicates(subset=['article_hash'])
    return df


@error_handler
def preprocess_news_with_claude(news_id, title, description):
    """기사 내용을 기반으로 Claude API를 이용해 정확한 요약 및 키워드 추출"""
    if not description or len(description) < 100:
        logger.warning(f"기사 ID {news_id}의 본문이 너무 짧아 처리할 수 없습니다.")
        return None
        
    prompt = f"""
    당신은 금융/경제 뉴스 분석 전문가입니다. 다음 기사에서 가장 중요한 금융/경제 키워드를 오직 단어로만 추출하고, 전반적인 핵심 요점을 정확하게 요약해주세요.
    
    기사 제목: {title}
    기사 내용: {description[:10000]}
    
    이 기사에서 핵심 금융/경제 키워드 5개와 내용을 2~3문장으로 정확하게 요약해주세요.
    
    1. 각 키워드는 기사의 핵심 금융/경제 용어 및 개념을 담고 있어야 합니다.
    2. 요약은 기사의 주요 내용과 시장에 미칠 영향이나 경제적 의의를 포함해야 합니다.
    
    반드시 다음 JSON 형식으로 응답해주세요:
    {{
        "keywords": "키워드1, 키워드2, 키워드3, 키워드4, 키워드5",
        "summary": "기사의 내용을 3줄의 불릿포인트를 통해 문장으로 요약한 내용"
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
    
    이 기사의 주요 금융/경제 개념을 이해하기 위한 객관식 퀴즈를 제작해주세요. 반드시 최소 5개 이상의 퀴즈를 제작하고, 다음 형식을 정확히 따라주세요.
    퀴즈의 난이도는 EASY 난이도로만 생성해주고, 금융/경제 지식이 많이 없는 학습자들이 쉽게 이해할 수 있도록 작성해주세요.

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
def save_news_to_database(df):
    """뉴스 데이터프레임을 데이터베이스에 저장 - 중복 기사 확인 및 새 기사만 추가"""
    if df.empty:
        logger.warning("저장할 뉴스 데이터가 없습니다.")
        return []
    
    # 데이터베이스에 추가할 뉴스 ID를 저장할 목록
    inserted_news_ids = []
    total_articles = len(df)
    print(df.published_date.head(1))
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
            published_date = row['published_date'] if 'published_date' in row and not pd.isna(row['published_date']) else ''
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
                    (title, description, created_at, source, published_date, url, article_hash, keywords, summary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    title, description, created_at, source, published_date, url, article_hash, keywords, summary
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
                time.sleep(1)  # API 요청 중간에 짠시 대기
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
            time.sleep(1)
        
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
