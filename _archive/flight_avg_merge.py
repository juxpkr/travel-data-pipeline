import pandas as pd
import json
from typing import Optional

def merge_flight_with_avg(
    flight_csv: str,
    avg_csv: str,
    output_csv: Optional[str] = None,
) -> pd.DataFrame:
    """
    flight 데이터와 flight_price_avg 데이터를 도착_공항_코드+월 기준으로 결합하고,
    실제 가격이 평균 가격보다 높은지/낮은지/같은지 등 점수 및 상태 컬럼을 추가하며,
    저장 전 컬럼명을 한글로 보기 좋게 정리합니다.
    Args:
        flight_csv (str): flight 데이터 csv 경로
        avg_csv (str): 평균가격 데이터 csv 경로
        output_csv (str, optional): 결합 결과를 저장할 경로. 지정하지 않으면 저장하지 않음.
    Returns:
        pd.DataFrame: 결합된 데이터프레임
    """
    # 데이터 로드
    flight = pd.read_csv(flight_csv)
    avg = pd.read_csv(avg_csv)

    # 도착_월 추출
    flight['도착_월'] = pd.to_datetime(flight['도착_시간']).dt.month

    ### 평균 가격 merge
    merged = flight.merge(
        avg[['city_code', 'month', 'avg_price']],
        left_on=['도착_공항_코드', '도착_월'],
        right_on=['city_code', 'month'],
        how='left'
    )

    # merged = flight.merge(
    # avg[['city_code', 'month', 'avg_price', 'min_price', 'max_price']],
    # left_on=['도착_공항_코드', '도착_월'],
    # right_on=['city_code', 'month'],
    # how='left')



    # 가격 차이, 증감률, 상태, 점수 컬럼 추가   
    merged['가격차이'] = merged['가격'] - merged['avg_price']
    merged['증감률(%)'] = (merged['가격차이'] / merged['avg_price'] * 100).round(2)
    merged['가격상태'] = merged['가격차이'].apply(lambda x: '상승' if x > 0 else ('하락' if x < 0 else '동일'))
    merged['점수'] = merged['가격상태'].replace({'상승': -1, '동일': 0, '하락': 1})
    # 가격 위치 비율 정규화 (0 ~ 1)
    # merged["가격_위치_정규화"] = (
    #     (merged["가격"] - merged["min_price"]) / (merged["max_price"] - merged["min_price"])
    # ).round(4)

    # 예외 처리 (min_price == max_price인 경우 → NaN → 0.5로 간주)
    # merged["가격_위치_정규화"] = merged["가격_위치_정규화"].fillna(0.5)
    

    # 컬럼명 정리 (한글 보기 좋게)
    rename_dict = {
        'city_code': '도착_도시코드',
        'month': '월',
        'avg_price': '평균가격',
    }
    merged = merged.rename(columns=rename_dict)


    # ### ✅ 서울 출발 + 7월 + 도시별 최저가 필터링
    # if filter_seoul_july_min_price:
    #     merged = (
    #         merged[(merged["출발_공항_코드"] == "ICN") & (merged["도착_월"] == 7)]
    #         .sort_values("가격", ascending=True)
    #         .drop_duplicates(subset=["도착_도시코드_3자리"], keep="first")
    #         .reset_index(drop=True)
    #     )


    ### 국가 코드 매핑 ---------------------------------------------
    with open("standard_country_map.json", encoding="utf-8") as f:
        country_map = json.load(f)

    # 2자리 코드 → 3자리 코드 딕셔너리 생성
    code2_to_code3 = {
        k: v["country_code_3"]
        for k, v in country_map.items()
        if "country_code_2" in v and "country_code_3" in v and len(k) == 2
    }
    

    ### 도시 코드 매핑: country_city_meta_data.csv 기반 ---------------------------------------------
    # 공항코드 → 도시코드 매핑용 메타 데이터 불러오기
    city_meta_df = pd.read_csv("country_city_meta_data.csv")

    # 문자열 리스트 형태의 'airport_codes' 열을 정제
    city_meta_df['airport_codes'] = city_meta_df['airport_codes'].str.replace(r"[\[\]' ]", "", regex=True)
    city_meta_df = city_meta_df.dropna(subset=['airport_codes', 'final_city_code'])

    # 공항코드를 하나씩 분해해서 각 행으로 분리
    city_meta_df = city_meta_df.assign(공항코드=city_meta_df['airport_codes'].str.split(",")).explode('공항코드')

    # 도착_공항_코드 기준으로 도시코드 붙이기
    merged = merged.merge(
        city_meta_df[['공항코드', 'final_city_code']],
        left_on='도착_공항_코드',
        right_on='공항코드',
        how='left'
    )

    # 컬럼명 정리
    merged = merged.rename(columns={'final_city_code': '도착_도시코드_3자리'})

    # 중간 컬럼 정리
    merged = merged.drop(columns=['공항코드'], errors='ignore')



    # merge 데이터에 country_code_3 컬럼 추가
    merged["도착_국가_3자리"] = merged["도착_국가_코드"].map(code2_to_code3)
    merged["출발_국가_3자리"] = merged["출발_국가_코드"].map(code2_to_code3)

    # 필요시 저장
    if output_csv:
        merged.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f'결합 및 점수화(컬럼명 정리) 완료: {output_csv}')

    return merged

# CLI로도 사용 가능하게
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="flight와 flight_avg를 도착_공항_코드+월 기준으로 결합 및 점수화")
    parser.add_argument('flight_csv', help='flight 데이터 csv 경로')
    parser.add_argument('avg_csv', help='평균가격 데이터 csv 경로')
    parser.add_argument('output_csv', help='결합 결과 저장 경로')
    args = parser.parse_args()
    merge_flight_with_avg(args.flight_csv, args.avg_csv, args.output_csv) 
    # python flight_avg_merge.py Azure_Fin_api_2025-07-16_12-35-28.csv flight_price_avg.csv flight_price_merged.csv

    # 7월
    # 서울 출발만 - 도시 중복 제거
    # 가격 최소 노선만 남기기
    # 7월 내 퍙군 최소최대값 