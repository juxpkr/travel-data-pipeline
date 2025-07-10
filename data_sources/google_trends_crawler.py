import logging
import datetime
import requests
import json
from pytrends.request import TrendReq
import pandas as pd
import os
import random
import time
import pytz  # crawled_at_kst를 위해 필요할 수 있음


# -------------------------------------------------------------
# 여러 키워드를 비교하고 '성장률' 기반의 트렌드 지수를 계산
# -------------------------------------------------------------
def get_integrated_travel_trends(
    country_keywords: dict, timeframe: str = "today 3-m", geo: str = "KR"
) -> list:

    pytrends_connector = TrendReq(hl="ko-KR", tz=540)  # Google Trends 연결 도구 초기화

    # 검색 키워드 -> 국가명 역방향 매핑 딕셔너리 (결과를 국가명으로 바꾸기 위함)
    keyword_to_country_name = {v: k for k, v in country_keywords.items()}

    # 모든 검색 키워드를 리스트로 만들기
    all_search_keywords = list(country_keywords.values())
    total_keyword_count = len(all_search_keywords)

    # 각 키워드에 대한 원시 성장률과 현재 관심도 점수를 저장할 딕셔너리
    # { "검색키워드": {"raw_growth": 성장률_값, "current_interest": 현재_관심도_값}, ... }
    raw_trends_data = {}

    # 모든 그룹에 공통으로 포함될 '기준 키워드' 설정
    anchor_keyword = "해외여행"

    # '기준 키워드'가 각 그룹에서 받은 Raw 성장률과 현재 관심도를 모아둘 리스트
    anchor_keyword_raw_growth_rates = []
    anchor_keyword_current_interests = []

    logging.info(
        f"Starting Google Trends analysis for {total_keyword_count} keywords, processing in groups of 4 country keywords + 1 anchor keyword ('{anchor_keyword}')."
    )

    # 4개씩 국가 키워드를 묶는 반복문
    for i in range(0, total_keyword_count, 4):
        current_country_keywords_chunk = all_search_keywords[i : i + 4]
        current_pytrends_group = current_country_keywords_chunk + [anchor_keyword]

        logging.info(f"Processing keyword group: {current_pytrends_group}.")

        try:
            pytrends_connector.build_payload(
                current_pytrends_group, cat=0, timeframe=timeframe, geo=geo, gprop=""
            )
            # interest_over_time으로 각 키워드의 시간별 관심도를 가져옴
            # timeframe 'today 3-m' (지난 3개월 = 약 90일) 기준으로 슬라이싱
            time_series_data = pytrends_connector.interest_over_time()

            # 요청 사이에 잠깐 쉬기
            time.sleep(random.uniform(60, 120))

            # 가져온 데이터가 비어있다면 (검색량이 너무 적거나 문제 발생)
            if time_series_data.empty:
                logging.warning(
                    f"No Google Trends time series data found for group: {current_pytrends_group}. Skipping this group."
                )
                continue

            if "isPartial" in time_series_data.columns:
                time_series_data = time_series_data.drop(columns=["isPartial"])

            # 각 키워드에 대한 '성장률' 및 '현재 관심도' 계산 로직
            # 일별 데이터 기준 슬라이싱
            # '최근 15일' 데이터: DataFrame의 끝에서 15개 행 선택
            last_15_days_data = time_series_data.iloc[-15:]
            # '직전 15일' 데이터: DataFrame의 끝에서 30번째부터 15번째까지의 행 선택
            previous_15_days_data = time_series_data.iloc[-30:-15]

            for keyword_in_group in current_pytrends_group:
                if keyword_in_group in time_series_data.columns:
                    last_15_days_avg = last_15_days_data[keyword_in_group].mean()
                    previous_15_days_avg = previous_15_days_data[
                        keyword_in_group
                    ].mean()

                    growth_rate = 0.0
                    # 성장률 계산 로직

                    if previous_15_days_avg > 0:
                        growth_rate = (
                            last_15_days_avg - previous_15_days_avg
                        ) / previous_15_days_avg
                    elif last_15_days_avg > 0:
                        growth_rate = 1.0

                    # 모두 0인 경우는 growth_rate 0.0 유지

                    # 현재 관심도 점수 계산 (가장 최근 1일의 관심도)
                    # mean()의 결과가 NaN일 수 있으므로 0.0으로 기본값 설정
                    current_interest_score = time_series_data[keyword_in_group].iloc[-1]
                    if pd.isna(current_interest_score):
                        current_interest_score = 0.0

                    # raw_trends_data에 성장률과 현재 관심도 점수를 함께 저장
                    raw_trends_data[keyword_in_group] = {
                        "raw_growth": growth_rate,
                        "current_interest": current_interest_score,
                    }

                    # 기준 키워드('해외여행')의 점수는 따로 저장
                    if keyword_in_group == anchor_keyword:
                        anchor_keyword_raw_growth_rates.append(growth_rate)
                        anchor_keyword_current_interests.append(current_interest_score)

                else:
                    logging.warning(
                        f"Keyword '{keyword_in_group}' data not found in DataFrame columns. Skipping calculation."
                    )

        except Exception as e:
            logging.error(
                f"Error processing Google Trends data for group {current_pytrends_group}: {e}. Skipping this group."
            )
            continue

    # -------------------------------------------------------------------
    # 모든 국가의 성장률과 현재 관심도를 0-100 스케일로 최종 정규화
    # -------------------------------------------------------------------
    result_list = []

    if not raw_trends_data:  # 수집된 원시 트렌드 데이터가 없으면 빈 리스트 반환
        logging.warning(
            "No raw trend data collected. Cannot perform final normalization."
        )
        return []

    # '기준 키워드'('해외여행')의 평균 성장률과 평균 현재 관심도를 계산
    # (여러 그룹에서 얻은 값 통합)
    avg_anchor_growth = (
        sum(anchor_keyword_raw_growth_rates) / len(anchor_keyword_raw_growth_rates)
        if anchor_keyword_raw_growth_rates
        else 0.0
    )
    avg_anchor_interest = (
        sum(anchor_keyword_current_interests) / len(anchor_keyword_current_interests)
        if anchor_keyword_current_interests
        else 0.0
    )

    # '기준 키워드'('해외여행')의 성장률이 0이거나 음수일 때의 대체 정규화 로직

    if avg_anchor_growth <= 0:
        logging.warning(
            f"Anchor keyword ('{anchor_keyword}') growth rate is zero or negative ({avg_anchor_growth}). Normalization based on anchor may not be effective. Using fallback normalization."
        )

        # 기준 키워드를 제외한 모든 국가 키워드의 성장률과 관심도 값만 필터링
        all_country_raw_growth_values = [
            v["raw_growth"] for k, v in raw_trends_data.items() if k != anchor_keyword
        ]
        all_country_current_interest_values = [
            v["current_interest"]
            for k, v in raw_trends_data.items()
            if k != anchor_keyword
        ]

        # 필터링된 데이터가 없으면 빈 리스트 반환
        if not all_country_raw_growth_values or not all_country_current_interest_values:
            logging.warning(
                "No country-specific data for fallback normalization after filtering anchor keyword. Returning empty list."
            )
            return []

        # 대체 정규화 범위 설정 (음수 성장률은 0점 처리)
        min_growth_val_fallback = min(0.0, min(all_country_raw_growth_values))
        max_growth_val_fallback = max(0.0, max(all_country_raw_growth_values))
        growth_range_fallback = max_growth_val_fallback - min_growth_val_fallback

        min_interest_val_fallback = min(0.0, min(all_country_current_interest_values))
        max_interest_val_fallback = max(0.0, max(all_country_current_interest_values))
        interest_range_fallback = max_interest_val_fallback - min_interest_val_fallback

        # 대체 정규화 수행
        for keyword_from_dict, data_values in raw_trends_data.items():
            if keyword_from_dict == anchor_keyword:
                continue  # 기준 키워드 제외

            normalized_growth_score_fallback = 0.0
            if growth_range_fallback > 0:
                normalized_growth_score_fallback = (
                    (data_values["raw_growth"] - min_growth_val_fallback)
                    / growth_range_fallback
                ) * 100
            elif data_values["raw_growth"] > 0:
                normalized_growth_score_fallback = 100.0

            normalized_interest_score_fallback = 0.0
            if interest_range_fallback > 0:
                normalized_interest_score_fallback = (
                    (data_values["current_interest"] - min_interest_val_fallback)
                    / interest_range_fallback
                ) * 100
            elif data_values["current_interest"] > 0:
                normalized_interest_score_fallback = 100.0

            # 최종 트렌드 지수 계산 (가중치 조합) - 대체 방식
            # W_growth, W_interest는 함수 내에서 정의되어야 함.
            # 지금 이 함수 내부에 W_growth, W_interest 정의가 없으니 문제가 될 수 있다.
            # 임시로 0.7, 0.3을 여기에 정의하거나, 함수 인자로 받아야 함.
            W_growth = 0.7
            W_interest = 0.3

            final_combined_trend_score_fallback = (
                normalized_growth_score_fallback * W_growth
            ) + (normalized_interest_score_fallback * W_interest)
            final_score_fallback = int(
                round(max(0, min(100, final_combined_trend_score_fallback)))
            )

            country_name = keyword_to_country_name.get(
                keyword_from_dict, keyword_from_dict
            )
            result_list.append(
                {
                    "country": country_name,
                    "trend_score": final_score_fallback,
                    "crawled_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),  # UTC로 기록
                    "crawled_at_kst": datetime.datetime.now(
                        pytz.timezone("Asia/Seoul")
                    ).isoformat(),  # KST로 기록
                }
            )

        logging.info(
            "Normalization completed using fallback method (anchor keyword issue)."
        )
        return result_list  # 대체 정규화가 완료되면 바로 결과 반환

    # 모든 국가 키워드에 대한 Raw 성장률과 현재 관심도 값을 필터링
    all_country_raw_growth_values = [
        v["raw_growth"] for k, v in raw_trends_data.items() if k != anchor_keyword
    ]
    all_country_current_interest_values = [
        v["current_interest"] for k, v in raw_trends_data.items() if k != anchor_keyword
    ]

    # '성장률 점수'를 0-100으로 정규화할 범위 설정
    min_growth_norm_val = (
        min(0.0, min(all_country_raw_growth_values))
        if all_country_raw_growth_values
        else 0.0
    )
    max_growth_norm_val = (
        max(0.0, max(all_country_raw_growth_values))
        if all_country_raw_growth_values
        else 0.0
    )
    growth_range = max_growth_norm_val - min_growth_norm_val

    # '현재 관심도 점수'를 0-100으로 정규화할 범위 설정
    min_interest_norm_val = (
        min(0.0, min(all_country_current_interest_values))
        if all_country_current_interest_values
        else 0.0
    )
    max_interest_norm_val = (
        max(0.0, max(all_country_current_interest_values))
        if all_country_current_interest_values
        else 0.0
    )
    interest_range = max_interest_norm_val - min_interest_norm_val

    # 각 국가별 최종 트렌드 지수 계산
    W_growth = 0.7  # 성장률 가중치
    W_interest = 0.3  # 현재 관심도 가중치

    for keyword_from_dict, data_values in raw_trends_data.items():
        if keyword_from_dict == anchor_keyword:  # 기준 키워드는 최종 결과에서 제외
            continue

        # '성장률 점수' 정규화 (0-100 스케일)
        normalized_growth_score = 0.0
        if growth_range > 0:
            normalized_growth_score = (
                (data_values["raw_growth"] - min_growth_norm_val) / growth_range
            ) * 100
        elif (
            data_values["raw_growth"] > 0
        ):  # 범위가 0인데 양수 값인 경우 (모든 성장률 동일 시), 100점
            normalized_growth_score = 100.0

        # '현재 관심도 점수' 정규화 (필요시)
        normalized_interest_score = 0.0
        if interest_range > 0:
            normalized_interest_score = (
                (data_values["current_interest"] - min_interest_norm_val)
                / interest_range
            ) * 100
        elif data_values["current_interest"] > 0:
            normalized_interest_score = 100.0

        # 최종 트렌드 지수 계산 (가중치 조합)
        final_combined_trend_score = (normalized_growth_score * W_growth) + (
            normalized_interest_score * W_interest
        )

        # 최종 점수 보정 (0-100 범위 유지)
        final_score = int(
            round(max(0, min(100, final_combined_trend_score)))
        )  # 0~100 범위로 고정

        # 국가명으로 변환
        country_name = keyword_to_country_name.get(keyword_from_dict, keyword_from_dict)

        result_list.append(
            {
                "country": country_name,
                "trend_score": final_score,
                "crawled_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),  # UTC로 기록
                "crawled_at_kst": datetime.datetime.now(
                    pytz.timezone("Asia/Seoul")
                ).isoformat(),  # KST로 기록
            }
        )

    logging.info(
        f"Successfully calculated and normalized {len(result_list)} country trend scores based on anchor keyword."
    )
    return result_list
