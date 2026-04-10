import os
import pandas as pd
import requests
from dotenv import load_dotenv
import time

load_dotenv()

# 파일 경로 설정 (절대 경로 보다는 실행 위치 기준으로 설정)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLACE_DB_PATH = os.path.join(BASE_DIR, "data", "PlaceDb.csv")
COURSE_STOP_DB_PATH = os.path.join(BASE_DIR, "data", "CourseStopDb.csv")

# 카카오 API 설정
KAKAO_REST_API_KEY = os.getenv("KAKAO_REST_API_KEY")

def get_kakao_distance(origin_lon, origin_lat, dest_lon, dest_lat):
    """
    카카오 모빌리티 길찾기 API를 사용하여 두 지점 간의 도로 거리를 가져옵니다.
    단위: 미터(m)
    """
    if not KAKAO_REST_API_KEY:
        print("Error: KAKAO_REST_API_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.")
        return None

    url = "https://apis-navi.kakaomobility.com/v1/directions"
    params = {
        "origin": f"{origin_lon},{origin_lat}",
        "destination": f"{dest_lon},{dest_lat}",
        "priority": "RECOMMEND"
    }
    headers = {
        "Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 401:
            print("Error: 카카오 API 키가 유효하지 않습니다.")
            return None
            
        response.raise_for_status()
        data = response.json()
        
        if "routes" in data and len(data["routes"]) > 0:
            # 첫 번째 경로의 총 거리(meters) 반환
            distance = data["routes"][0]["summary"]["distance"]
            return distance
        else:
            print(f"Warn: 경로를 찾을 수 없습니다 ({origin_lon},{origin_lat} -> {dest_lon},{dest_lat})")
            return None
    except Exception as e:
        print(f"Error: API 호출 중 오류 발생: {e}")
        return None

def main():
    if not os.path.exists(PLACE_DB_PATH) or not os.path.exists(COURSE_STOP_DB_PATH):
        print(f"Error: 데이터 파일을 찾을 수 없습니다.\n- {PLACE_DB_PATH}\n- {COURSE_STOP_DB_PATH}")
        return

    # 데이터 로드
    print(f"데이터 로드 중...\n- Places: {PLACE_DB_PATH}\n- CourseStops: {COURSE_STOP_DB_PATH}")
    place_df = pd.read_csv(PLACE_DB_PATH)
    course_stop_df = pd.read_csv(COURSE_STOP_DB_PATH)

    # 장소 코드별 위경도 매핑 생성
    place_coords = {}
    for _, row in place_df.iterrows():
        place_coords[str(row['code'])] = (row['longitude'], row['latitude'])

    # 코스별로 그룹화하여 처리
    courses = course_stop_df.groupby('CourseCode')
    total_courses = len(courses)
    
    print(f"총 {total_courses}개의 코스를 처리합니다.")

    for i, (course_code, group) in enumerate(courses, 1):
        print(f"[{i}/{total_courses}] 코스 {course_code} 처리 중...")
        
        # index를 보존하면서 stopOrder 순으로 정렬된 리스트 생성
        sorted_indices = group.sort_values('stopOrder').index.tolist()
        
        prev_place_code = None
        
        for idx in sorted_indices:
            row = course_stop_df.loc[idx]
            current_place_code = str(row['PlaceCode'])
            stop_order = row['stopOrder']
            
            if stop_order == 1:
                # 첫 번째 장소는 거리와 시간 모두 null
                course_stop_df.loc[idx, 'distance_from_prev'] = None
                course_stop_df.loc[idx, 'duration_from_prev'] = None
                print(f"  Stop {stop_order}: {current_place_code} (시작점 - null)")
            else:
                if prev_place_code and prev_place_code in place_coords and current_place_code in place_coords:
                    origin_lon, origin_lat = place_coords[prev_place_code]
                    dest_lon, dest_lat = place_coords[current_place_code]
                    
                    # API 호출 (디버깅을 위해 약간의 딜레이 추가 가능)
                    # time.sleep(0.1) 
                    distance_m = get_kakao_distance(origin_lon, origin_lat, dest_lon, dest_lat)
                    
                    if distance_m is not None:
                        # 거리 계산 (km 단위)
                        distance_km = distance_m / 1000.0
                        
                        # 시간 계산 (1km당 4분)
                        duration_min = distance_km * 4
                        
                        # 값 채우기
                        course_stop_df.loc[idx, 'distance_from_prev'] = round(distance_km, 2)
                        course_stop_df.loc[idx, 'duration_from_prev'] = round(duration_min, 1)
                        print(f"  Stop {stop_order}: {prev_place_code} -> {current_place_code} | {distance_km:.2f}km, {duration_min:.1f}분")
                    else:
                        print(f"  Stop {stop_order}: {current_place_code} (거리 정보 가져오기 실패)")
                else:
                    missing = []
                    if prev_place_code not in place_coords: missing.append(f"prev({prev_place_code})")
                    if current_place_code not in place_coords: missing.append(f"curr({current_place_code})")
                    print(f"  Stop {stop_order}: 좌표 정보 부족 - {', '.join(missing)}")
            
            prev_place_code = current_place_code

    # 파일 저장 (인코딩 유지)
    print("저장 중...")
    course_stop_df.to_csv(COURSE_STOP_DB_PATH, index=False, encoding='utf-8-sig')
    print("완료!")

if __name__ == "__main__":
    main()
