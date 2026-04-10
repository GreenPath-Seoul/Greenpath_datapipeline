import pandas as pd
import os

# BASE_DIR 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 파일 경로
course_stop_path = os.path.join(BASE_DIR, "data", "CourseStopDb.csv")
course_path = os.path.join(BASE_DIR, "data", "CourseDb.csv")

# ✅ CSV 로드 (이게 핵심!)
course_stop_df = pd.read_csv(course_stop_path)
course_df = pd.read_csv(course_path)

# 🔥 거리 null 처리 (첫 지점)
course_stop_df["distance_from_prev"] = course_stop_df["distance_from_prev"].fillna(0)
course_stop_df = pd.read_csv(course_stop_path, encoding="utf-8-sig")
course_stop_df.columns = course_stop_df.columns.str.strip()

# 🔥 코스별 거리 합 계산
distance_sum = (
    course_stop_df
    .groupby("CourseCode")["distance_from_prev"]
    .sum()
    .reset_index()
)

# 컬럼 이름 맞추기
distance_sum.columns = ["code", "distanceKm"]

# 🔥 기존 course_df와 merge
updated_course_df = course_df.drop(columns=["distanceKm"], errors="ignore") \
    .merge(distance_sum, on="code", how="left")

# 소수점 정리
updated_course_df["distanceKm"] = updated_course_df["distanceKm"].round(2)

# 저장 (같은 data 폴더에 저장)
output_path = os.path.join(BASE_DIR, "data", "CourseDb_updated.csv")
updated_course_df.to_csv(output_path, index=False, encoding="utf-8-sig")

print("✅ 코스 거리 업데이트 완료!")