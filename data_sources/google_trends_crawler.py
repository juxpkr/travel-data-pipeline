import logging
import datetime
import json
import os
import random
import time
import pytz
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
from requests.exceptions import RequestException
from pytrends.exceptions import TooManyRequestsError


# 재시도 로깅을 위한 헬퍼 함수
def retry_log(retry_state):
    logging.warning(
        f"Google Trends API 호출 재시도 중 (data_sources): "
        f"시도 횟수: {retry_state.attempt_number}번째, "
        f"다음 시도까지 대기 시간: {retry_state.next_action.sleep}초. "
        f"마지막으로 발생한 오류: {retry_state.outcome.exception()}"
    )


# 특정 키워드 그룹의 Google Trends 데이터를 가져와 처리하는 로직 함수
def get_trends_data_for_group(
    keywords_in_group: list, timeframe: str = "today 3-m", geo: str = "KR"
) -> list:
    logging.info(f"Google Trends 데이터 처리 시작: 그룹 {keywords_in_group}")

    pytrends_connector = TrendReq(hl="ko-KR", tz=540)
    pd.set_option("future.no_silent_downcasting", True)

    anchor_keyword = "해외여행"

    @retry(
        wait=wait_exponential(multiplier=1, min=120, max=600),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(
            (RequestException, ResponseError, TooManyRequestsError)
        ),
        before_sleep=retry_log,
    )
    def _fetch_trend_data_with_retry():
        logging.info(f"Google Trends API 요청 중: {keywords_in_group}")
        pytrends_connector.build_payload(
            keywords_in_group, cat=0, timeframe=timeframe, geo=geo, gprop=""
        )
        time.sleep(random.uniform(30, 60))
        time_series_data = pytrends_connector.interest_over_time()
        return time_series_data

    try:
        time_series_data = _fetch_trend_data_with_retry()

        if time_series_data is None or time_series_data.empty:
            logging.warning(f"그룹 '{keywords_in_group}'에 대한 데이터가 없습니다.")
            return []

        if "isPartial" in time_series_data.columns:
            time_series_data = time_series_data.drop(columns=["isPartial"])

        result_for_group = []

        last_15_days_data = time_series_data.iloc[-15:]
        previous_15_days_data = time_series_data.iloc[-30:-15]

        W_growth = 0.7  # 가중치 정의
        W_interest = 0.3  # 가중치 정의

        for keyword_in_group in keywords_in_group:
            if keyword_in_group == anchor_keyword:
                continue

            if keyword_in_group in time_series_data.columns:
                raw_growth = 0.0
                if previous_15_days_data[keyword_in_group].mean() > 0:
                    raw_growth = (
                        last_15_days_data[keyword_in_group].mean()
                        - previous_15_days_data[keyword_in_group].mean()
                    ) / previous_15_days_data[keyword_in_group].mean()
                elif last_15_days_data[keyword_in_group].mean() > 0:
                    # 이전 평균이 0에 가깝지만, 최근 평균이 유의미하게 증가한 경우
                    # 아주 작은값(epsilon)을 사용하여 분모 0이 되는 오류를 방지하고, 실제 성장 규모를 반영
                    epsilon = 1e-6
                    raw_growth = last_15_days_data[keyword_in_group].mean() / epsilon

                current_interest = time_series_data[keyword_in_group].iloc[-1]
                if pd.isna(current_interest):
                    current_interest = 0.0

                # 앵커 키워드 데이터 추출
                anchor_growth = 0.0
                anchor_growth = 0.0
                anchor_interest = 0.0
                if anchor_keyword in time_series_data.columns:
                    if previous_15_days_data[anchor_keyword].mean() > 0:
                        anchor_growth = (
                            last_15_days_data[anchor_keyword].mean()
                            - previous_15_days_data[anchor_keyword].mean()
                        ) / previous_15_days_data[anchor_keyword].mean()
                    elif (
                        last_15_days_data[anchor_keyword].mean() > 0
                    ):  # 이전 평균이 0인데 현재 값이 있으면 100% 성장
                        anchor_growth = 1.0
                    anchor_interest = time_series_data[anchor_keyword].iloc[-1]
                    if pd.isna(anchor_interest):
                        anchor_interest = 0.0

                result_for_group.append(
                    {
                        "keyword": keyword_in_group,
                        "trend_score_raw_growth": raw_growth,
                        "trend_score_current_interest": current_interest,
                        "anchor_growth": anchor_growth,
                        "anchor_interest": anchor_interest,
                    }
                )
            else:
                logging.warning(
                    f"키워드 '{keyword_in_group}'에 대한 데이터 컬럼을 찾을 수 없습니다. 건너뜁니다."
                )

        return result_for_group

    except RequestException as e:
        logging.exception(f"그룹 '{keywords_in_group}'에 대한 요청 오류: {e}")
        return []
    except Exception as e:
        logging.exception(f"그룹 '{keywords_in_group}' 처리 중 예상치 못한 오류: {e}")
        return []
