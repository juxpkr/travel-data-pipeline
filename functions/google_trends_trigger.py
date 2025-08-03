import logging
import datetime
import json
import os
import azure.functions as func
from azure.storage.queue import QueueClient, BinaryBase64EncodePolicy
import time
import random
import sys

# --- MASTER_COUNTRY_CRAWLER_MAP 로딩 ---
MASTER_COUNTRY_CRAWLER_MAP = {}

MASTER_MAP_FILE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "config", "master_country_crawler.json"
)

try:
    with open(MASTER_MAP_FILE_PATH, "r", encoding="utf-8") as f:
        MASTER_COUNTRY_CRAWLER_MAP = json.load(f)
    logging.info(
        f"Master country mapping data loaded successfully from {MASTER_MAP_FILE_PATH}."
    )
except FileNotFoundError:
    logging.critical(
        f"Master country mapping file not found at {MASTER_MAP_FILE_PATH}. Exiting."
    )
    sys.exit(1)  # 중요한 파일이 없으므로 프로그램 종료

except json.JSONDecodeError as e:
    logging.critical(f"Error decoding JSON master country mapping file: {e}. Exiting.")
    sys.exit(1)  # JSON 파일 손상이므로 프로그램 종료

except Exception as e:
    logging.critical(
        f"Unexpected error loading master country mapping file: {e}. Exiting."
    )
    sys.exit(1)  # 기타 심각한 오류이므로 프로그램 종료

# '해외여행' 앵커 키워드
anchor_keyword = "해외여행"


def register_google_trends_crawler(app_instance):

    # Google Trends 검색 키워드 목록을 MASTER_COUNTRY_CRAWLER_MAP에서 동적으로 생성
    # 모든 국가 정보를 돌면서 google_trend_keyword_kor 필드의 값을 추출
    all_search_keywords_values = []
    for country_code_3, country_info in MASTER_COUNTRY_CRAWLER_MAP.items():
        keyword = country_info.get("google_trend_keyword_kor")
        if keyword:  # 키워드가 유효한 경우에만 추가
            all_search_keywords_values.append(keyword)
        else:
            logging.warning(
                f"Country '{country_info.get('country_name_kor', country_code_3)}' (Code: {country_code_3}) "
                f"is missing 'google_trend_keyword_kor' in MASTER_COUNTRY_CRAWLER_MAP. Skipping for Google Trends."
            )

    total_keyword_count = len(all_search_keywords_values)
    logging.info(
        f"Dynamically loaded {total_keyword_count} Google Trends keywords from MASTER_COUNTRY_CRAWLER_MAP."
    )

    # Google Trends 데이터를 수집하는 Azure Function
    @app_instance.timer_trigger(
        schedule="0 0 13,19 * * *",  # 매일 13시 19시에 실행
        run_on_startup=False,
        use_monitor=False,
        arg_name="myTimer",
    )
    def googleTrendsCrawler(myTimer: func.TimerRequest) -> None:
        # 함수 시작 시간 로깅
        utc_timestamp = datetime.datetime.utcnow().isoformat()
        if myTimer.past_due:
            logging.info("Timer run was overdue!")
        logging.info(f"Python googleTrendsCrawler function started at {utc_timestamp}.")

        queue_connection_string = os.environ.get("AzureWebJobsStorage")
        queue_name = os.environ.get("GoogleTrendsQueueName")

        if not queue_connection_string:
            logging.error(
                "AzureWebJobsStorage connection string is not set. Cannot proceed with queue operations."
            )
            return
        if not queue_name:
            logging.error(
                "GoogleTrendsQueueName is not set. Cannot proceed with queue operations."
            )
            return

        try:
            queue_client = QueueClient.from_connection_string(
                conn_str=queue_connection_string,
                queue_name=queue_name,
                message_encode_policy=BinaryBase64EncodePolicy(),
            )
            logging.info(f"Successfully connected to queue '{queue_name}'.")
        except Exception as e:
            # 큐 연결 자체에 실패한 경우만 심각한 오류로 처리하고 종료
            logging.critical(
                f"Failed to connect to Azure Queue Storage '{queue_name}': {e}. Exiting Google Trends crawler."
            )
            return  # 큐 연결 실패 시 더 이상 진행하지 않음

        # Google Trends API에 보낼 키워드 묶음 4개 (앵커 키워드 포함 시 총 5개)
        batch_size_for_trends_api = 4

        messages_to_send_in_batches = []
        # 4개씩 키워드를 묶어서 메시지를 만듬
        for i in range(0, total_keyword_count, batch_size_for_trends_api):
            current_country_keywords_chunk = all_search_keywords_values[
                i : i + batch_size_for_trends_api
            ]
            # 여기에 앵커 키워드를 추가하여 총 5개 키워드 묶음을 만듬
            keywords_for_api_request = current_country_keywords_chunk + [anchor_keyword]

            task_message = {
                # 키워드 리스트 자체를 보냄
                "keywords": keywords_for_api_request,
                "timeframe": "today 3-m",
                "geo": "KR",  # 한국 지역에서 검색하는 것을 유지
                "request_time": datetime.datetime.utcnow().isoformat(),
            }
            messages_to_send_in_batches.append(
                json.dumps(task_message, ensure_ascii=False).encode("utf-8")
            )

        total_messages_sent = 0

        try:
            # Azure Queue Storage Batching 대신 개별 메시지를 보내되, 지연을 줘서 과도한 요청 방지
            for message_content in messages_to_send_in_batches:
                queue_client.send_message(message_content)
                # UTF-8 디코딩하여 로깅 시 가독성 확보
                logging.info(
                    f"큐에 메시지 전송 완료: {message_content.decode('utf-8')[:100]}..."
                )
                total_messages_sent += 1
                # 요청 사이에 랜덤 지연을 줘서 API 부하를 줄임
                time.sleep(random.uniform(1, 3))
        except Exception as e:
            logging.error(
                f"Error sending messages to queue: {e}", exc_info=True
            )  # 예외 정보도 함께 로깅

        logging.info(
            f"Python googleTrendsCrawler (Producer) function completed. Total {total_messages_sent} batches sent to '{queue_name}'"
        )
