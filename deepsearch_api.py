import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
import pymysql
from datetime import datetime, timedelta
import sys
import time
from bs4 import BeautifulSoup
import random
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
print(f"Current working directory: {os.getcwd()}")
env_path = os.path.join(os.getcwd(), 'crawling-airflow', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
    print(f".env file found and loaded from: {env_path}")
else:
    alt_env_path = os.path.join(os.getcwd(), '.env')
    if os.path.exists(alt_env_path):
        load_dotenv(alt_env_path)
        print(f".env file found and loaded from: {alt_env_path}")
    else:
        print(f".env file not found at: {env_path} or {alt_env_path}")

def fetch_deepsearch_data():
    # Debug: print sensitive env vars masked
    print("\nEnvironment variables:")
    for key, value in os.environ.items():
        if 'key' in key.lower() or 'pass' in key.lower() or 'secret' in key.lower():
            print(f"{key}: {'*' * len(value)}")
    api_key = os.getenv('DEEPSEARCH_API') or "2786225e9d0e44aa84d92b6e5365b69e"
    print(f"Using DeepSearch API Key: {'*' * len(api_key)}")

    url = 'https://api-v2.deepsearch.com/v1/articles'
    params = {
        'keyword': '금융 뉴스',
        'date_from': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
        'date_to': datetime.now().strftime('%Y-%m-%d'),
        'page': 1,
        'page_size': 30,
        'api_key': api_key
    }
    headers = {'Content-Type': 'application/json'}

    print(f"Requesting data with parameters: {params}")
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        news_items = data.get('data', [])
        if not news_items:
            print("API에서 뉴스 항목을 찾을 수 없습니다.")
            return pd.DataFrame()

        print(f"Found {len(news_items)} news items")
        processed = []
        for item in news_items:
            article_url = item.get('content_url') or item.get('url') or ''
            if article_url:
                print(f"기사 원문 URL: {article_url}")
            full_content = ""  # placeholder, 실제 본문은 병렬 처리에서 채워짐
            processed.append({
                'title': item.get('title', ''),
                'description': full_content,
                'link': article_url,
                'pubDate': item.get('published_at', ''),
                'provider': item.get('publisher', '')
            })
        df = pd.DataFrame(processed)
        print(f"Fetched {len(df)} items into DataFrame")
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def extract_full_article_content(url):
    if not url.startswith("http"):
        return ""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)...',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0)...'
    ]
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept-Language': 'ko-KR,ko;q=0.9',
    }
    try:
        # 간단한 requests + BeautifulSoup 파싱
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup(['script','style','nav','footer','header','aside']):
            tag.decompose()

        domain = urlparse(url).netloc
        candidate_selectors = {
            'yna.co.kr': ['#articleWrap','.article-view'],
            'biz.sbs.co.kr': ['#article_content','.article_cont_area'],
            'hankyung.com': ['.article-body','#articletxt'],
            # 필요시 더 추가...
        }
        # 도메인별 우선 파싱
        if domain in candidate_selectors:
            for sel in candidate_selectors[domain]:
                block = soup.select_one(sel)
                if block:
                    return block.get_text('\n',strip=True).strip()

        # 공통 후보군
        common = [
            'article','.article','.article-content','.news-content',
            '.article-body','#articleBody','[itemprop="articleBody"]'
        ]
        texts = []
        for sel in common:
            blk = soup.select_one(sel)
            if blk:
                txt = blk.get_text('\n',strip=True)
                if len(txt)>100:
                    texts.append(txt)
        if texts:
            return max(texts, key=len).strip()

        # fallback: p 태그 모두 합치기
        ps = soup.find_all('p')
        lines = [p.get_text(strip=True) for p in ps if len(p.get_text(strip=True))>20]
        return '\n'.join(lines).strip()

    except Exception as e:
        print(f"[{url}] content fetch failed: {e}")
        return ""

def process_data(df):
    if df.empty:
        print("처리할 데이터가 없습니다.")
        return df
    # 중복 제거, 날짜 포맷팅
    df = df.drop_duplicates(['title','link'])
    if 'pubDate' in df:
        df['pubDate'] = pd.to_datetime(df['pubDate'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    # 병렬로 본문 추출
    valid = df[df['link'].str.startswith('http')].copy()
    urls = valid['link'].tolist()
    print(f"\n총 {len(urls)}개 기사에서 본문 가져오기 (병렬 처리)...")

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(extract_full_article_content, url): i for i, url in enumerate(urls)}
        for future in as_completed(future_to_url):
            idx = future_to_url[future]
            try:
                content = future.result()
                df.at[valid.index[idx], 'description'] = content or df.at[valid.index[idx], 'description']
                print(f"  [{idx+1}/{len(urls)}] fetched ({len(content)} chars)")
            except Exception as e:
                print(f"  [{idx+1}] error: {e}")

    # 칼럼 정리 - news 테이블 구조에 맞게 조정
    # news 테이블: news_id, created_at, description, published_date, source, title, url
    mapping = {'title':'title','description':'description','link':'url','pubDate':'published_date','provider':'source'}
    df = df.rename(columns=mapping)[list(mapping.values())]
    df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n총 {len(df)}개 기사 처리 완료")
    return df

def load_to_mysql(df):
    if df.empty:
        print("No data to load")
        return
    db_host = os.getenv('DB_IP')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    conn = pymysql.connect(host=db_host,user=db_user,password=db_password,database=db_name,charset='utf8mb4')
    cursor = conn.cursor()
    
    # 기존 테이블에 데이터 삽입 (news 테이블)
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT IGNORE INTO news
            (title, description, url, published_date, source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            row['title'], row['description'], row['url'],
            row['published_date'], row['source'], row['created_at']
        ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(df)} records")

def verify_data_in_db():
    print("\nVerifying data in database...")
    db_host = os.getenv('DB_IP')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    conn = pymysql.connect(host=db_host,user=db_user,password=db_password,database=db_name,charset='utf8mb4')
    cursor = conn.cursor()
    
    # news 테이블 통계 확인
    cursor.execute('SELECT COUNT(*), AVG(LENGTH(description)), MIN(LENGTH(description)), MAX(LENGTH(description)) FROM news')
    total, avg_len, min_len, max_len = cursor.fetchone()
    print(f"\nNews table stats: total={total}, avg={int(avg_len) if avg_len else 0}, min={min_len}, max={max_len}")
    
    # 최근 뉴스 확인
    cursor.execute('SELECT news_id, title, source, published_date, created_at FROM news ORDER BY created_at DESC LIMIT 3')
    print("\nRecent news articles:")
    for row in cursor.fetchall():
        print(row)
    
    # 퀴즈 테이블 통계 확인
    cursor.execute('SELECT COUNT(*) FROM news_quiz')
    quiz_count = cursor.fetchone()[0]
    print(f"\nQuiz table stats: total={quiz_count}")
    
    cursor.close()
    conn.close()

import json
import anthropic

def generate_quiz_for_article(news_id, title, description):
    """
    Claude API를 사용하여 기사를 기반으로 3개의 퀴즈 문제 생성
    """
    try:
        print(f"Generating quiz for article ID {news_id}: {title}")
        api_key = os.getenv('ANTHROPIC_API_KEY')
        client = anthropic.Anthropic(api_key=api_key)
        
        # 퀴즈 생성을 위한 프롬프트 구성
        prompt = f"""
하나의 기사를 제공해 드립니다. 이 기사의 내용을 기반으로 경제/금융 지식을 확인하는 4지선다형 문제 3개를 만들어주세요.

제목: {title}

기사 내용:
{description}

요구사항:
1. 기사에서 다루는 경제/금융 개념에 관한 문제를 만들어주세요.
2. 각 문제는 반드시 4가지 보기가 있어야 합니다.
3. 각 보기는 A, B, C, D로 시작해야 합니다.
4. 정답은 보기 중 하나여야 합니다. (A, B, C, D 중 하나)

결과는 다음과 같은 JSON 형식으로 제공해주세요:
{{
  "quizzes": [
    {{
      "question": "문제 내용",
      "choices": [
        "A. 보기 1",
        "B. 보기 2",
        "C. 보기 3",
        "D. 보기 4"
      ],
      "answer": "A"
    }},
    {{
      "question": "다른 문제",
      "choices": [
        "A. 보기 1",
        "B. 보기 2",
        "C. 보기 3",
        "D. 보기 4"
      ],
      "answer": "C"
    }},
    ...
  ]
}}

반드시 3개의 문제를 만들어주세요. 응답은 JSON 형식으로만 제공해주세요.
        """
        
        # Claude API 호출
        message = client.messages.create(
            model="claude-3-opus-20240229",  # 현재 사용 가능한 모델로 변경
            max_tokens=2000,
            temperature=0.3,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # 응답에서 JSON 추출
        response_text = message.content[0].text
        
        # JSON 추출 시도
        try:
            # 전체 응답이 JSON인 경우
            quiz_data = json.loads(response_text)
        except:
            # JSON 부분만 추출
            import re
            json_match = re.search(r'\{\s*"quizzes"[\s\S]*\}', response_text)
            if json_match:
                try:
                    quiz_data = json.loads(json_match.group(0))
                except:
                    print(f"Failed to parse JSON from response: {response_text[:100]}...")
                    return None
            else:
                print(f"No JSON found in response: {response_text[:100]}...")
                return None
        
        return quiz_data
    except Exception as e:
        print(f"Error generating quiz: {e}")
        return None

def save_quizzes_to_db(news_id, quiz_data):
    """
    생성된 퀴즈를 news_quiz 테이블에 저장
    """
    if not quiz_data or 'quizzes' not in quiz_data:
        print(f"No valid quiz data for article {news_id}")
        return False
    
    db_host = os.getenv('DB_IP')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    
    try:
        conn = pymysql.connect(host=db_host,user=db_user,password=db_password,database=db_name,charset='utf8mb4')
        cursor = conn.cursor()
        
        for quiz in quiz_data['quizzes']:
            question = quiz['question']
            choices = quiz['choices']
            answer = quiz['answer']
            
            # news_quiz 테이블 구조에 맞게 데이터 저장
            if len(choices) >= 4:
                cursor.execute('''
                    INSERT INTO news_quiz
                    (news_id, newsquiz_content, newsquiz_choice_a, newsquiz_choice_b, 
                     newsquiz_choice_c, newsquiz_choice_d, newsquiz_correct_ans, newsquiz_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    news_id, question, 
                    choices[0].replace('A. ', ''), 
                    choices[1].replace('B. ', ''), 
                    choices[2].replace('C. ', ''), 
                    choices[3].replace('D. ', ''),
                    answer, 10
                ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving quizzes to database: {e}")
        return False

def process_articles_for_quiz_generation():
    """
    news 테이블에서 기사를 가져와 퀴즈 생성 및 저장
    """
    print("\nProcessing articles for quiz generation...")
    db_host = os.getenv('DB_IP')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    
    try:
        conn = pymysql.connect(host=db_host,user=db_user,password=db_password,database=db_name,charset='utf8mb4')
        cursor = conn.cursor()
        
        # 퀴즈가 없는 기사 가져오기
        cursor.execute('''
            SELECT n.news_id, n.title, n.description 
            FROM news n
            LEFT JOIN news_quiz q ON n.news_id = q.news_id
            WHERE q.news_id IS NULL
            AND LENGTH(n.description) > 100
            LIMIT 5
        ''')
        
        articles = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not articles:
            print("No new articles to process for quiz generation")
            return
        
        print(f"Found {len(articles)} articles for quiz generation")
        
        for article in articles:
            news_id, title, description = article
            print(f"Generating quiz for article ID {news_id}")
            
            # 퀴즈 생성
            quiz_data = generate_quiz_for_article(news_id, title, description)
            
            if quiz_data:
                # 퀴즈 저장
                success = save_quizzes_to_db(news_id, quiz_data)
                if success:
                    print(f"Successfully saved quizzes for article ID {news_id}")
                else:
                    print(f"Failed to save quizzes for article ID {news_id}")
            else:
                print(f"Failed to generate quiz for article ID {news_id}")
    
    except Exception as e:
        print(f"Error in process_articles_for_quiz_generation: {e}")

def main():
    print("Starting DeepSearch ETL pipeline...")
    df = fetch_deepsearch_data()
    processed = process_data(df)
    load_to_mysql(processed)
    
    # 퀴즈 생성 프로세스 추가
    process_articles_for_quiz_generation()
    
    # 데이터베이스 확인
    verify_data_in_db()
    print("ETL pipeline completed.")

if __name__ == "__main__":
    main()
