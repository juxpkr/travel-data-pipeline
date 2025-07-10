import logging
import datetime
import json
import os
import azure.functions as func

# data_sources 크롤링 로직 함수를 import
from data_sources.google_trends_crawler import get_integrated_travel_trends


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

        # get_integrated_travel_trends 함수를 호출하여 통합 트렌드 성장률 데이터를 가져옴
        all_country_growth_trends_data = get_integrated_travel_trends(
            country_keywords=country_search_keywords_list,
            timeframe="today 3-m",  # 지난 3개월 데이터
            geo="KR",  # 한국에서의 검색 관심도
        )

        if all_country_growth_trends_data:
            logging.info(
                f"Total {len(all_country_growth_trends_data)} country trends extracted and calculated."
            )
            logging.info(
                json.dumps(all_country_growth_trends_data, indent=2, ensure_ascii=False)
            )

            try:
                timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = f"country_trends_{timestamp_str}.json"
                # 파일 저장 경로를 TRAVEL-DATA-PIPELINE/local_output/ 디렉토리 안에 저장
                output_dir = os.path.join(os.getcwd(), "local_output")
                os.makedirs(output_dir, exist_ok=True)  # 디렉토리가 없으면 생성
                output_path = os.path.join(output_dir, file_name)

                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(
                        all_country_growth_trends_data, f, indent=4, ensure_ascii=False
                    )
                logging.info(f"Successfully saved trends data to {output_path}.")
            except Exception as file_ex:
                logging.error(f"Failed to save trends data to file: {file_ex}.")
        else:
            logging.warning("No country trends data extracted or calculated.")

        logging.info("Python googleTrendsCrawler function completed.")
