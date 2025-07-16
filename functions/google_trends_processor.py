import logging
import json
import os
import datetime
import pytz
import pandas as pd
import numpy as np
import time
import random

import azure.functions as func

from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

from data_sources.google_trends_crawler import (
    get_trends_data_for_group,
)

# --- STANDARD_COUNTRY_MAP ---
STANDARD_COUNTRY_MAP = {}

# 맵 파일 경로
MAP_FILE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "config", "standard_country_map.json"
)

try:
    with open(MAP_FILE_PATH, "r", encoding="utf-8") as f:
        STANDARD_COUNTRY_MAP = json.load(f)
    logging.info(
        f"Standard country mapping data loaded successfully from {MAP_FILE_PATH}."
    )
except FileNotFoundError:
    logging.error(f"Mapping file not found at {MAP_FILE_PATH}. Using fallback map.")
    # 로드 실패 시 최소한의 기본 맵 (새로운 키 이름으로)
    STANDARD_COUNTRY_MAP = {
        "아르헨티나": {
            "korean_name": "아르헨티나",
            "english_name": "Argentina",
            "country_code_3": "ARG",
            "country_code_2": "AR",
        },
        "해외여행": {
            "korean_name": "해외여행_전체",
            "english_name": "Global Travel",
            "country_code_3": "GLOBAL",
            "country_code_2": "XX",
        },
    }
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON mapping file: {e}. Using fallback map.")
    STANDARD_COUNTRY_MAP = {
        "아르헨티나": {
            "korean_name": "아르헨티나",
            "english_name": "Argentina",
            "country_code_3": "ARG",
            "country_code_2": "AR",
        },
        "해외여행": {
            "korean_name": "해외여행_전체",
            "english_name": "Global Travel",
            "country_code_3": "GLOBAL",
            "country_code_2": "XX",
        },
    }
except Exception as e:
    logging.error(f"Unexpected error loading mapping file: {e}. Using fallback map.")
    STANDARD_COUNTRY_MAP = {
        "아르헨티나": {
            "korean_name": "아르헨티나",
            "english_name": "Argentina",
            "country_code_3": "ARG",
            "country_code_2": "AR",
        },
        "해외여행": {
            "korean_name": "해외여행_전체",
            "english_name": "Global Travel",
            "country_code_3": "GLOBAL",
            "country_code_2": "XX",
        },
    }


# --- [Azure Function: 큐 메시지 소비자 (Consumer)] ---
# 이 함수는 큐에 메시지가 들어올 때마다 자동으로 실행
def register_google_trends_processor(app_instance):

    @app_instance.queue_trigger(
        arg_name="msg",
        queue_name=os.environ.get("GoogleTrendsQueueName"),
        connection="AzureWebJobsStorage",
    )
    @app_instance.event_hub_output(
        arg_name="event_output",
        event_hub_name=os.environ.get("GoogleTrendsEventHubName"),
        connection="EventHubConnectionString",
    )
    def googleTrendsProcessor(
        msg: func.QueueMessage, event_output: func.Out[str]
    ) -> None:

        logging.info("Google Trends Processor 시작")

        logging.info(f"큐 메시지 수신: {msg.get_body().decode('utf-8')}")
        message_body = json.loads(msg.get_body().decode("utf-8"))

        # 메시지에서 키워드 리스트를 가져온다.
        keywords_to_process = message_body.get("keywords")
        timeframe = message_body.get("timeframe", "today 3-m")
        geo = message_body.get("geo", "KR")

        if not keywords_to_process:
            logging.error("큐 메시지에 'keywords' 리스트가 없습니다. 건너뜁니다.")
            return

        # data_sources의 get_trends_data_for_group 함수를 호출
        processed_trend_data_list = get_trends_data_for_group(
            keywords_to_process,
            timeframe=timeframe,
            geo=geo,
        )

        # 데이터를 성공적으로 가져왔다면 Event Hub로 보낸다.
        if processed_trend_data_list:
            kst_timezone = pytz.timezone("Asia/Seoul")
            current_crawl_time_utc = datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat()
            current_crawl_time_kst = datetime.datetime.now(kst_timezone).isoformat()

            events_to_send = []
            for item in processed_trend_data_list:
                keyword = item.get("keyword")

                # -- 국가명 표준화 로직
                # keyword에서 "여행" 을 제거하여 순수한 한글 국가명 추출
                korean_country_name = (
                    keyword.replace(" 여행", "") if "여행" in keyword else keyword
                )

                # STANDARD_COUNTRY_MAP을 사용하여 해당 국가의 모든 표준 정보 딕셔너리 조회
                country_info = STANDARD_COUNTRY_MAP.get(korean_country_name, {})

                # '해외여행' 앵커 키워드에 대한 처리
                if keyword == "해외여행":
                    country_info = STANDARD_COUNTRY_MAP.get("해외여행", {})

                # 조회된 정보 딕셔너리에서 각 컬럼 값 추출
                country_korean_name = country_info.get("korean_name", "Unknown_Korean")
                country_english_name = country_info.get(
                    "english_name", "Unknown_English"
                )
                country_code_3 = country_info.get("country_code_3", "N/A")
                country_code_2 = country_info.get("country_code_2", "N/A")
                # --- 국가명 표준화 로직 끝 ---

                raw_growth_val = (
                    float(item.get("trend_score_raw_growth"))
                    if pd.notna(item.get("trend_score_raw_growth"))
                    else 0.0
                )
                raw_growth = (
                    float(item.get("trend_score_raw_growth"))
                    if pd.notna(item.get("trend_score_raw_growth"))
                    else None
                )
                current_interest = (
                    int(item.get("trend_score_current_interest"))
                    if pd.notna(item.get("trend_score_current_interest"))
                    else None
                )  # int 또는 float으로 명시적 변환
                anchor_growth = (
                    float(item.get("anchor_growth"))
                    if pd.notna(item.get("anchor_growth"))
                    else None
                )
                anchor_interest = (
                    int(item.get("anchor_interest"))
                    if pd.notna(item.get("anchor_interest"))
                    else None
                )
                if raw_growth_val > 0:
                    scaled_raw_growth = np.log10(1 + raw_growth_val)
                elif raw_growth_val < 0:
                    # 음수 성장은 원본 값을 유지. 음수값이 크지 않기에
                    scaled_raw_growth = raw_growth_val
                else:
                    # raw_growth가 0인 경우
                    scaled_raw_growth = 0.0

                # final_trend_score 계산
                W_growth = 0.7
                W_interest = 0.3

                max_log_growth_scale = 10.0
                normalized_scaled_raw_growth = 0.0

                if scaled_raw_growth > 0:
                    # 양수 성장률을 0-100 스케일로 변환
                    normalized_scaled_raw_growth = (
                        scaled_raw_growth / max_log_growth_scale
                    ) * 100.0
                    # 최대 100을 넘지 않도록
                    normalized_scaled_raw_growth = min(
                        normalized_scaled_raw_growth, 100.0
                    )
                elif scaled_raw_growth < 0:
                    # 음수 성장률에 대한 처리
                    normalized_scaled_raw_growth = 0.0
                else:
                    # 0인 경우
                    normalized_scaled_raw_growth = 0.0

                final_trend_score = (normalized_scaled_raw_growth * W_growth) + (
                    current_interest * W_interest
                )
                # 최종 스코어가 0-100을 벗어나지 않도록 설정
                final_trend_score = max(0.0, min(final_trend_score, 100.0))

                final_data_to_send = {
                    "dataType": "googleTrend",
                    "keyword": keyword,
                    "country_korean_name": country_korean_name,
                    "country_english_name": country_english_name,
                    "country_code_3": country_code_3,
                    "country_code_2": country_code_2,
                    "final_trend_score": final_trend_score,
                    "trend_score_raw_growth": raw_growth_val,
                    "scaled_raw_growth": scaled_raw_growth,
                    "trend_score_current_interest": current_interest,
                    "anchor_growth": anchor_growth,
                    "anchor_interest": anchor_interest,
                    "crawled_at_kst": current_crawl_time_kst,
                }
                events_to_send.append(
                    json.dumps(final_data_to_send, ensure_ascii=False)
                )

            # 여러 이벤트를 한 번에 Event Hub로 보냄
            event_output.set(events_to_send)
            logging.info(
                f"처리된 Google Trend 데이터 {len(events_to_send)}개 Event Hub로 전송 완료."
            )
        else:
            logging.warning(
                f"큐 메시지 '{message_body}' 처리 후 트렌드 데이터를 얻지 못했습니다. Event Hub로 전송하지 않습니다."
            )
