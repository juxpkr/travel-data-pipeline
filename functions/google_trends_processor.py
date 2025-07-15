import logging
import json
import os
import datetime
import pytz
import pandas as pd
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
                )  # int 또는 float으로 명시적 변환
                # Producer에서 이미 country_name 정보를 메시지에 포함시켰다면, 여기서 사용.
                # 아니라면, keyword_to_country_name 딕셔너리를 Consumer에서도 유지해야 한다.
                # 여기서는 키워드를 기준으로 Event Hub에 보낸다.
                final_data_to_send = {
                    "dataType": "googleTrend",
                    "keyword": keyword,
                    "trend_score_raw_growth": raw_growth,
                    "trend_score_current_interest": current_interest,
                    "anchor_growth": anchor_growth,
                    "anchor_interest": anchor_interest,
                    "crawled_at_utc": current_crawl_time_utc,
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
