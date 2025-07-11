import logging
import datetime
import json
import os
import azure.functions as func
from azure.storage.queue import QueueClient, BinaryBase64EncodePolicy
import time
import random


def register_google_trends_crawler(app_instance):

    # 해외 국가들과 그에 해당하는 Google Trends 검색 키워드 목록
    country_search_keywords_list = {
        "Argentina": "아르헨티나 여행",
        "Australia": "호주 여행",
        "Austria": "오스트리아 여행",
        "Belgium": "벨기에 여행",
        "Brazil": "브라질 여행",
        "Bulgaria": "불가리아 여행",
        "Cambodia": "캄보디아 여행",
        "Canada": "캐나다 여행",
        "Chile": "칠레 여행",
        "China": "중국 여행",
        "Colombia": "콜롬비아 여행",
        "Costa Rica": "코스타리카 여행",
        "Croatia": "크로아티아 여행",
        "Cuba": "쿠바 여행",
        "Czech Republic": "체코 여행",
        "Denmark": "덴마크 여행",
        "Egypt": "이집트 여행",
        "Estonia": "에스토니아 여행",
        "Finland": "핀란드 여행",
        "France": "프랑스 여행",
        "Georgia": "조지아 여행",
        "Germany": "독일 여행",
        "Greece": "그리스 여행",
        "Hungary": "헝가리 여행",
        "Iceland": "아이슬란드 여행",
        "India": "인도 여행",
        "Indonesia": "인도네시아 여행",
        "Iran": "이란 여행",
        "Ireland": "아일랜드 여행",
        "Israel": "이스라엘 여행",
        "Italia": "이탈리아 여행",
        "Japan": "일본 여행",
        "Laos": "라오스 여행",
        "Latvia": "라트비아 여행",
        "Lithuania": "리투아니아 여행",
        "Malaysia": "말레이시아 여행",
        "Mexico": "멕시코 여행",
        "Mongolia": "몽골 여행",
        "Morocco": "모로코 여행",
        "Netherlands": "네덜란드 여행",
        "New Zealand": "뉴질랜드 여행",
        "Norway": "노르웨이 여행",
        "Peru": "페루 여행",
        "Philippines": "필리핀 여행",
        "Poland": "폴란드 여행",
        "Portugal": "포르투갈 여행",
        "Qatar": "카타르 여행",
        "Russia": "러시아 여행",
        "Serbia": "세르비아 여행",
        "Slovakia": "슬로바키아 여행",
        "Slovenia": "슬로베니아 여행",
        "Spain": "스페인 여행",
        "Swiss": "스위스 여행",
        "Taiwan": "대만 여행",
        "Thailand": "태국 여행",
        "Republic of Turkiye": "튀르키예 여행",
        "Ukraine": "우크라이나 여행",
        "United Arab Emirates": "아랍에미리트 여행",
        "United Kingdom": "영국 여행",
        "United States of America": "미국 여행",
        "Vietnam": "베트남 여행",
    }
    anchor_keyword = "해외여행"

    # Google Trends 데이터를 수집하는 Azure Function
    @app_instance.timer_trigger(
        schedule="0 0 0 1 1 *",
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

        queue_connection_string = os.environ["AzureWebJobsStorage"]
        queue_name = "google-trends-crawl-requests"

        queue_client = QueueClient.from_connection_string(
            conn_str=queue_connection_string,
            queue_name=queue_name,
            message_encode_policy=BinaryBase64EncodePolicy(),
        )

        all_search_keywords_values = list(country_search_keywords_list.values())
        total_keyword_count = len(all_search_keywords_values)
        # Google Trends API에 보낼 키워드 묶음 4개
        batch_size_for_trends_api = 4

        messages_to_send_in_batches = []
        # 4개씩 키워드를 묶어서 메시지를 만든다
        for i in range(0, total_keyword_count, batch_size_for_trends_api):
            current_country_keywords_chunk = all_search_keywords_values[
                i : i + batch_size_for_trends_api
            ]
            # 여기에 앵커 키워드를 추가하여 총 5개 키워드 묶음을 만든다.
            keywords_for_api_request = current_country_keywords_chunk + [anchor_keyword]

            task_message = {
                # 키워드 리스트 자체를 보냄
                "keywords": keywords_for_api_request,
                "timeframe": "today 3-m",
                "geo": "KR",
                "request_time": datetime.datetime.utcnow().isoformat(),
            }
            messages_to_send_in_batches.append(
                json.dumps(task_message, ensure_ascii=False)
            )

        total_messages_sent = 0
        # Azure Queue Storage batch 전송 최대 메시지 수
        queue_batch_max_size = 32

        try:
            # Azure Queue Storage Batching으로 메시지를 보낸다.
            for i in range(0, len(messages_to_send_in_batches), queue_batch_max_size):
                batch_to_send = messages_to_send_in_batches[
                    i : i + queue_batch_max_size
                ]

                queue_client.send_message_batch(batch_to_send)

                logging.info(f"Sent batch of {len(batch_to_send)} messages to queue")
                total_messages_sent += len(batch_to_send)
                time.sleep(random.uniform(1, 3))

        except Exception as e:
            logging.error(f"Error sending messages batch to queue: {e}")

        logging.info(
            f"Python googleTrendsCrawler (Producer) function completed. Total {total_messages_sent} batches sent to '{queue_name}'"
        )
