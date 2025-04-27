import os
import json
import time
import signal
import shutil
import requests
import pandas as pd
import re
from datetime import datetime, timezone

# ----------------------------
# 설정값
# ----------------------------
SAVE_EVERY_N = 100
PROGRESS_FILE = "progress.json"
LOG_FILE = "log.txt"
BACKUP_DIR = "backup"
OUTPUT_DIR = "output"
API_URL = "https://data-api.cryptocompare.com/news/v1/article/list"

MAX_RETRY = 3
WAIT_ON_429 = 60  # seconds

# 환경변수 기반 수집 구간 설정
start_date_str = os.getenv("START_DATE", "20240101")
end_date_str = os.getenv("END_DATE", "20241231")

START_TIMESTAMP = int(datetime.strptime(end_date_str, "%Y%m%d").replace(
    hour=23, minute=59, second=59, tzinfo=timezone.utc
).timestamp())
END_TIMESTAMP = int(datetime.strptime(start_date_str, "%Y%m%d").replace(
    hour=0, minute=0, second=0, tzinfo=timezone.utc
).timestamp())

# ----------------------------
# 준비
# ----------------------------
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 로깅 함수
def log(message):
    print(message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"{now_utc} UTC - {message}\n")

# 종료 핸들러
def graceful_exit(signum=None, frame=None):
    log("[종료 감지] 현재 상태 저장 후 종료합니다.")
    save_progress(current_date)
    exit(0)

signal.signal(signal.SIGINT, graceful_exit)

# ----------------------------
# 데이터 및 진행상태 불러오기
# ----------------------------
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        params = json.load(f)
    log(f"[재시작] {params['to_ts']} (UTC timestamp)부터 수집 재개합니다.")
else:
    params = {"sortOrder": "latest", "to_ts": START_TIMESTAMP}
    log(f"[처음 시작] {end_date_str} 기준 수집 시작합니다.")

articles_list = []
current_date = None

# ----------------------------
# 유틸리티 함수
# ----------------------------
def extract_assets(categories, body, title):
    assets = []
    for cat in categories:
        if re.fullmatch(r'[A-Z]{3,5}', cat) and (cat in body or cat in title):
            assets.append(cat)
    return assets

def save_progress(date_str):
    if not articles_list:
        return
    df = pd.DataFrame(articles_list)
    output_path = os.path.join(OUTPUT_DIR, f"news_{date_str}.xlsx")
    backup_path = os.path.join(BACKUP_DIR, f"news_{date_str}.xlsx")

    df.to_excel(output_path, index=False)
    shutil.copy(output_path, backup_path)

    with open(PROGRESS_FILE, "w") as f:
        json.dump(params, f)

    log(f"[일별 저장] {len(articles_list)}개 기사 저장 완료 → {output_path}")

    # Git add + commit + push
    os.system(f"git add {output_path} {backup_path} {PROGRESS_FILE}")
    os.system(f"git commit -m 'Auto-commit: {date_str} 수집 완료'")
    os.system("git push origin HEAD")

def request_with_retry(url, params):
    for attempt in range(MAX_RETRY):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 429:
                log(f"[429 에러] 요청 과다. {WAIT_ON_429}초 대기 후 재시도.")
                time.sleep(WAIT_ON_429)
                continue
            response.raise_for_status()
            return response
        except Exception as e:
            log(f"[요청 실패] {e} (시도 {attempt + 1}/{MAX_RETRY})")
            time.sleep(3)
    log("[요청 재시도 초과] 종료합니다.")
    graceful_exit()

# ----------------------------
# 메인 수집 루프
# ----------------------------
while True:
    if params["to_ts"] < END_TIMESTAMP:
        log("[수집 완료] 시작일 도달. 수집 종료합니다.")
        break

    response = request_with_retry(API_URL, params)

    try:
        data = response.json()
    except Exception as e:
        log(f"[JSON 파싱 실패] {e}")
        graceful_exit()

    articles = data.get("Data")
    if not articles:
        log("[완료] 더 이상 뉴스 없음. 수집 종료합니다.")
        break

    for article in articles:
        published_on = article.get("PUBLISHED_ON")
        date_str = datetime.utcfromtimestamp(published_on).strftime('%Y%m%d')

        global current_date
        if current_date is None:
            current_date = date_str

        if date_str != current_date:
            save_progress(current_date)
            articles_list = []
            current_date = date_str

        title = article.get("TITLE", "").strip()
        body = article.get("BODY", "").replace("\n", " ").strip()

        article_row = {
            "뉴스ID": article.get("ID"),
            "뉴스GUID": article.get("GUID"),
            "일자(UTC timestamp)": published_on,
            "뉴스제목": title,
            "본문": body,
            "SENTIMENT": article.get("SENTIMENT", "NEUTRAL"),
            "주요코인": " | ".join(extract_assets(
                [c.get("CATEGORY") for c in article.get("CATEGORY_DATA", [])],
                body, title
            )) or "없음",
            "카테고리": " | ".join(c.get("CATEGORY") for c in article.get("CATEGORY_DATA", [])),
            "작성자": article.get("AUTHORS", ""),
            "추천수": article.get("UPVOTES", 0),
            "비추천수": article.get("DOWNVOTES", 0),
            "출처": article.get("SOURCE_DATA", {}).get("NAME", ""),
            "뉴스URL": article.get("URL", ""),
            "이미지URL": article.get("IMAGE_URL", "")
        }

        articles_list.append(article_row)

    # 진행상황 출력
    oldest_ts = min(article['PUBLISHED_ON'] for article in articles)
    readable_time = datetime.fromtimestamp(oldest_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log(f"[진행상황] 총 수집: {len(articles_list)}개, 가장 오래된 뉴스 시점: {readable_time}")

    params["to_ts"] = oldest_ts - 1
    time.sleep(0.2)

# ----------------------------
# 마지막 저장
# ----------------------------
save_progress(current_date)

if os.path.exists(PROGRESS_FILE):
    os.remove(PROGRESS_FILE)

log(f"[최종 저장 완료] 모든 기사 저장 및 진행 파일 삭제 완료.")
