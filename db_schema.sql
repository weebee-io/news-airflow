-- 기존 테이블 삭제 (데이터 초기화)
DROP TABLE IF EXISTS news_quiz;
DROP TABLE IF EXISTS news;

-- news 테이블: 수집된 뉴스 기사 정보 저장
CREATE TABLE IF NOT EXISTS news (
    news_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    url VARCHAR(255) NOT NULL,
    published_date DATETIME,
    source VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    keywords VARCHAR(255),
    summary TEXT,
    UNIQUE KEY unique_url (url)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- news_quiz 테이블: 뉴스 기사 기반 생성된 퀴즈 저장
CREATE TABLE IF NOT EXISTS news_quiz (
    newsquiz_id INT AUTO_INCREMENT PRIMARY KEY,
    news_id INT NOT NULL,
    newsquiz_content TEXT NOT NULL,
    newsquiz_choice_a TEXT,
    newsquiz_choice_b TEXT,
    newsquiz_choice_c TEXT,
    newsquiz_choice_d TEXT,
    newsquiz_correct_ans VARCHAR(255),
    newsquiz_score INT,
    newsquiz_level VARCHAR(255),
    reason VARCHAR(255),
    FOREIGN KEY (news_id) REFERENCES news(news_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 인덱스 추가
CREATE INDEX idx_news_source ON news(source);
CREATE INDEX idx_news_published_date ON news(published_date);
CREATE INDEX idx_news_keywords ON news(keywords);
