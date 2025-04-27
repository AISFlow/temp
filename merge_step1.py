import pandas as pd
import os
from glob import glob

# ----------------------------
# 설정
# ----------------------------
OUTPUT_DIR = "output"
SPLIT_OUTPUT_DIR = "split_by_year"

# ----------------------------
# 준비
# ----------------------------
os.makedirs(SPLIT_OUTPUT_DIR, exist_ok=True)

# ----------------------------
# 파일 리스트 수집
# ----------------------------
file_pattern = os.path.join(OUTPUT_DIR, "news_*.xlsx")
xlsx_files = sorted(glob(file_pattern))

print(f"총 {len(xlsx_files)}개 파일을 읽어 연도별로 분리합니다.")

# ----------------------------
# 연도별 데이터 저장 준비
# ----------------------------
yearly_data = {}

# ----------------------------
# 파일 읽기 및 연도별 분배
# ----------------------------
for file in xlsx_files:
    try:
        df = pd.read_excel(file)
        df['연도'] = pd.to_datetime(
            df['일자(UTC timestamp)'], unit='s', utc=True
        ).dt.year

        for year in df['연도'].unique():
            year_df = df[df['연도'] == year].drop(columns=['연도'])
            if year not in yearly_data:
                yearly_data[year] = []
            yearly_data[year].append(year_df)

        print(f"읽기 완료: {file} ({len(df)} rows)")

    except Exception as e:
        print(f"오류 발생: {file}, {e}")

# ----------------------------
# 연도별로 저장
# ----------------------------
for year, dfs in yearly_data.items():
    final_df = pd.concat(dfs, ignore_index=True)
    save_path = os.path.join(SPLIT_OUTPUT_DIR, f"news_{year}.xlsx")
    final_df.to_excel(save_path, index=False)
    print(f"✅ 저장 완료: {save_path} ({len(final_df)} rows)")

print("🎯 전체 연도별 분리 완료")
