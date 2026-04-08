# scripts/update_place_image.py

import argparse
import io
import mimetypes
import os
import re
import sys
from typing import Optional, Tuple
from dotenv import load_dotenv
load_dotenv()
import boto3
import pandas as pd
import requests


CSV_PATH = "data-pipeline/data/PlaceDb.csv"
GOOGLE_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
GOOGLE_PHOTO_BASE_URL = "https://places.googleapis.com/v1"
DEFAULT_IMAGE_WIDTH = 600


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"환경변수 {name} 가 설정되지 않았습니다.")
    return value


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s+", "-", value)
    value = re.sub(r"[^a-z0-9가-힣\-_]+", "", value)
    return value or "place"


def guess_extension(content_type: Optional[str]) -> str:
    if not content_type:
        return ".jpg"

    content_type = content_type.split(";")[0].strip().lower()
    ext = mimetypes.guess_extension(content_type)
    if ext == ".jpe":
        return ".jpg"
    return ext or ".jpg"


def build_public_url(bucket: str, region: str, key: str, custom_domain: Optional[str] = None) -> str:
    if custom_domain:
        return f"https://{custom_domain}/{key}"

    # ap-northeast-2 포함 일반적인 퍼블릭 URL
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def search_place_photo_name(
    api_key: str,
    place_name: str,
    address: str,
    language_code: str = "ko",
) -> Optional[Tuple[str, str]]:
    """
    returns: (photo_name, display_name)
    """
    text_query = f"{place_name} {address}".strip()

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.photos",
    }
    payload = {
        "textQuery": text_query,
        "languageCode": language_code,
        "maxResultCount": 1,
    }

    resp = requests.post(GOOGLE_TEXT_SEARCH_URL, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    places = data.get("places", [])
    if not places:
        return None

    place = places[0]
    photos = place.get("photos", [])
    if not photos:
        return None

    photo_name = photos[0].get("name")
    display_name = (
        place.get("displayName", {}).get("text")
        if isinstance(place.get("displayName"), dict)
        else place_name
    )

    if not photo_name:
        return None

    return photo_name, display_name or place_name


def download_google_photo(api_key: str, photo_name: str, max_width_px: int = DEFAULT_IMAGE_WIDTH) -> Tuple[bytes, str]:
    """
    Place Photos (New):
    GET https://places.googleapis.com/v1/{photo_name}/media?key=API_KEY&maxWidthPx=600
    skipHttpRedirect=false 가 기본이라 보통 이미지 바이너리로 응답됨.
    """
    url = f"{GOOGLE_PHOTO_BASE_URL}/{photo_name}/media"
    params = {
        "key": api_key,
        "maxWidthPx": max_width_px,
    }

    resp = requests.get(url, params=params, timeout=60, allow_redirects=True)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "image/jpeg")
    return resp.content, content_type


def upload_bytes_to_s3(
    s3_client,
    bucket: str,
    region: str,
    image_bytes: bytes,
    content_type: str,
    object_key: str,
    custom_domain: Optional[str] = None,
) -> str:
    fileobj = io.BytesIO(image_bytes)

    extra_args = {
        "ContentType": content_type,
    }
    
    s3_client.upload_fileobj(
        Fileobj=fileobj,
        Bucket=bucket,
        Key=object_key,
        ExtraArgs=extra_args,
    )

    return build_public_url(bucket, region, object_key, custom_domain)


def validate_columns(df: pd.DataFrame) -> None:
    required = [
        "code",
        "name",
        "address",
        "latitude",
        "longitude",
        "summary",
        "description",
        "휴무일",
        "운영시간",
        "imageUrl",
        "category",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"CSV에 필요한 컬럼이 없습니다: {missing}")


def should_replace_image(url: str) -> bool:
    if pd.isna(url):
        return True
    url = str(url).strip()
    if not url:
        return True
    if "picsum.photos" in url:
        return True
    return False


def update_images_by_codes(codes: list[str], csv_path: str = CSV_PATH) -> None:
    google_api_key = require_env("GOOGLE_MAPS_API_KEY")
    aws_bucket = require_env("AWS_S3_BUCKET")
    aws_region = require_env("AWS_REGION")

    s3_prefix = os.getenv("AWS_S3_PREFIX", "places")
    s3_custom_domain = os.getenv("AWS_S3_CUSTOM_DOMAIN")  # 선택
    csv_encoding = os.getenv("CSV_ENCODING", "utf-8-sig")
    overwrite_all = os.getenv("OVERWRITE_ALL", "false").lower() == "true"
    acl_public_read = os.getenv("S3_PUBLIC_READ", "false").lower() == "true"

    s3_client = boto3.client("s3", region_name=aws_region)

    df = pd.read_csv(csv_path, encoding=csv_encoding)
    validate_columns(df)

    target_codes = {code.strip() for code in codes if code.strip()}
    if not target_codes:
        raise ValueError("수정할 code를 1개 이상 넣어야 합니다.")

    matched_mask = df["code"].astype(str).isin(target_codes)
    matched_rows = df[matched_mask]

    if matched_rows.empty:
        print("일치하는 code가 없습니다.")
        return

    print(f"총 {len(matched_rows)}개 행을 확인합니다.")

    updated_count = 0
    skipped_count = 0
    failed_codes = []

    for idx, row in matched_rows.iterrows():
        code = str(row["code"]).strip()
        name = str(row["name"]).strip()
        address = str(row["address"]).strip()
        current_url = "" if pd.isna(row["imageUrl"]) else str(row["imageUrl"]).strip()

        if not overwrite_all and not should_replace_image(current_url):
            print(f"[SKIP] {code} - 기존 imageUrl 유지: {current_url}")
            skipped_count += 1
            continue

        try:
            print(f"[SEARCH] {code} - {name}")
            photo_result = search_place_photo_name(
                api_key=google_api_key,
                place_name=name,
                address=address,
            )

            if not photo_result:
                print(f"[WARN] {code} - 검색 결과 또는 사진 없음")
                failed_codes.append(code)
                continue

            photo_name, matched_display_name = photo_result
            image_bytes, content_type = download_google_photo(
                api_key=google_api_key,
                photo_name=photo_name,
                max_width_px=DEFAULT_IMAGE_WIDTH,
            )

            ext = guess_extension(content_type)
            safe_code = slugify(code)
            safe_name = slugify(name)
            object_key = f"{s3_prefix}/{safe_code}-{safe_name}{ext}"

            s3_url = upload_bytes_to_s3(
                s3_client=s3_client,
                bucket=aws_bucket,
                region=aws_region,
                image_bytes=image_bytes,
                content_type=content_type,
                object_key=object_key,
                custom_domain=s3_custom_domain,
            )

            df.at[idx, "imageUrl"] = s3_url
            print(f"[OK] {code} - {matched_display_name} -> {s3_url}")
            updated_count += 1

        except Exception as e:
            print(f"[ERROR] {code} - {e}")
            failed_codes.append(code)

    # 원본 파일 덮어쓰기
    df.to_csv(csv_path, index=False, encoding=csv_encoding)

    print("\n===== 결과 =====")
    print(f"수정 완료: {updated_count}")
    print(f"건너뜀: {skipped_count}")
    print(f"실패: {len(failed_codes)}")
    if failed_codes:
        print("실패 code:", ", ".join(failed_codes))


def parse_args():
    parser = argparse.ArgumentParser(
        description="PlaceDb.csv에서 특정 code들의 imageUrl을 Google Places + S3로 갱신합니다."
    )
    parser.add_argument(
        "--codes",
        nargs="+",
        required=True,
        help="수정할 place code 목록. 예: P0030 P0031 P0042",
    )
    parser.add_argument(
        "--csv",
        default=CSV_PATH,
        help=f"CSV 경로 (기본값: {CSV_PATH})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = parse_args()
        update_images_by_codes(args.codes, args.csv)
    except Exception as e:
        print(f"[FATAL] {e}")
        sys.exit(1)