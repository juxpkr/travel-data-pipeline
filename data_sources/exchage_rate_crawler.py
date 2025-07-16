import logging
import datetime
import requests
from bs4 import BeautifulSoup
import json
import os
import pytz
import time
import random
import sys
from data_sources.retry_utils import exchange_rate_api_retry


# 평균 환율 (일평균, 월평균, 연평균) 조회용 URL
AVERAGE_EXCHANGE_CRAWL_URL = "https://www.kebhana.com/cms/rate/wpfxd651_06i_01.do"
# 실시간 환율 조회용 URL
REALTIME_EXCHANGE_CRAWL_URL = "https://www.kebhana.com/cms/rate/wpfxd651_01i_01.do"

# Referer URL 상수
REFERER_AVERAGE_EXCHANGE_URL = (
    "https://www.kebhana.com/cms/rate/index.do?contentUrl=/cms/rate/wpfxd651_06i.do"
)
REFERER_REALTIME_EXCHANGE_URL = (
    "https://www.kebhana.com/cms/rate/index.do?contentUrl=/cms/rate/wpfxd651_01i.do"
)

# HTTP 요청 헤더
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/javascript, text/html, application/xml, text/xml, */*",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
}

# --- STANDARD_COUNTRY_MAP 로딩 ---
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

except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON mapping file: {e}. Using fallback map.")

except Exception as e:
    logging.error(f"Unexpected error loading mapping file: {e}. Using fallback map.")

# 통화 코드와 국가명 매핑 딕셔너리
currency_code_to_country_name_map = {
    "USD": "미국",
    "JPY": "일본",
    "EUR": [
        "오스트리아",
        "벨기에",
        "핀란드",
        "프랑스",
        "독일",
        "그리스",
        "아일랜드",
        "이탈리아",
        "라트비아",
        "리투아니아",
        "네덜란드",
        "포르투갈",
        "슬로바키아",
        "슬로베니아",
        "스페인",
        "에스토니아",
        "키프로스",
        "룩셈부르크",
        "몰타",
        "크로아티아",
    ],
    "CNY": "중국",
    "TWD": "대만",
    "BND": "브루나이",
    "DZD": "알제리",
    "CLP": "칠레",
    "GBP": "영국",
    "AUD": "호주",
    "CAD": "캐나다",
    "CHF": "스위스",
    "HKD": "홍콩",
    "SGD": "싱가포르",
    "THB": "태국",
    "PHP": "필리핀",
    "SEK": "스웨덴",
    "NZD": "뉴질랜드",
    "NOK": "노르웨이",
    "DKK": "덴마크",
    "SAR": "사우디아라비아",
    "MYR": "말레이시아",
    "MXN": "멕시코",
    "BRL": "브라질",
    "AED": "아랍에미리트",
    "VND": "베트남",
    "ZAR": "남아프리카공화국",
    "IDR": "인도네시아",
    "INR": "인도",
    "RUB": "러시아",
    "PLN": "폴란드",
    "CZK": "체코",
    "HUF": "헝가리",
    "TRY": "튀르키예",
    "ILS": "이스라엘",
    "KZT": "카자흐스탄",
    "PKR": "파키스탄",
    "BDT": "방글라데시",
    "NPR": "네팔",
    "LKR": "스리랑카",
    "MNT": "몽골",
    "EGP": "이집트",
    "QAR": "카타르",
    "KWD": "쿠웨이트",
    "BHD": "바레인",
    "LBP": "레바논",
    "OMR": "오만",
    "JOD": "요르단",
    "KHR": "캄보디아",
    "LAK": "라오스",
    "MMK": "미얀마",
    "MOP": "마카오",
    "MVR": "몰디브",
    "NLG": "네덜란드",
    "PAB": "파나마",
    "PEN": "페루",
    "RON": "루마니아",
    "SDG": "수단",
    "UGX": "우간다",
    "UZS": "우즈베키스탄",
    "VEF": "베네수엘라",
    "XAF": "중앙아프리카 CFA 프랑",
    "XOF": "서아프리카 CFA 프랑",
    "ZMW": "잠비아",
    "KES": "케냐",
    "COP": "콜롬비아",
    "TZS": "탄자니아",
    "FJD": "피지",
    "LYD": "리비아",
    "ETB": "에티오피아",
}


# --- 날짜 관련 헬퍼 함수 ---
def get_first_day_of_year_yyyymmdd(year: int) -> str:
    return f"{year}0101"


def get_first_day_of_month_yyyymmdd(year: int, month: int) -> str:
    return f"{year}{month:02d}01"


def get_last_day_of_month_yyyymmdd(year: int, month: int) -> str:
    # 다음 달 1일에서 하루를 빼서 해당 월의 마지막 날짜를 구함
    if month == 12:
        return (datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)).strftime(
            "%Y%m%d"
        )
    return (datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    )


def get_current_kst_datetime(kst_timezone: pytz.timezone) -> datetime.datetime:
    return datetime.datetime.now(kst_timezone)


def get_kst_date_yyyymmdd(dt: datetime.date) -> str:
    return dt.strftime("%Y%m%d")


def get_kst_date_yyyy_mm_dd(dt: datetime.date) -> str:
    return dt.strftime("%Y-%m-%d")


# -------------------------------------------------------------
# 내부 헬퍼 함수: 실제 웹 요청 및 HTML 파싱
# -------------------------------------------------------------
@exchange_rate_api_retry
def _fetch_and_parse_exchange_rate(
    target_url: str, headers: dict, data: dict, kst_timezone: pytz.timezone
) -> list:
    all_extracted_rates = []

    try:
        # headers 딕셔너리를 복사하여 Referer를 추가
        current_request_headers = headers.copy()  # headers 인자를 복사하여 사용

        log_inquiry_code = data.get("inqDvCd") or data.get("inqKindCd")
        logging.info(
            f"Attempting to send POST request to: {target_url} with inquiry code: {log_inquiry_code} and payload: {data}"
        )
        response = requests.post(target_url, headers=headers, data=data, timeout=15)
        response.raise_for_status()

        logging.info(
            f"Successfully received response (Status: {response.status_code}) from {target_url} for inquiry code: {log_inquiry_code}"
        )

        soup = BeautifulSoup(response.text, "html.parser")
        exchange_rate_table = soup.find("table", class_="tblBasic leftNone")

        if exchange_rate_table:
            table_body = exchange_rate_table.find("tbody")
            if not table_body:
                logging.error(
                    f"Table body not found for URL: {target_url}, Payload: {data}. Full HTML: {response.text[:1000]}"
                )
                raise ValueError("tbody not found in exchange rate table.")

            rows = table_body.find_all("tr")

            # URL에 따른 파싱 로직 및 인덱스 설정
            expected_min_cells = 0
            currency_full_text_idx = 0
            buy_rate_idx = 0
            sell_rate_idx = 0
            send_rate_idx = 0
            receive_rate_idx = 0
            standard_rate_idx = 0

            if target_url == REALTIME_EXCHANGE_CRAWL_URL:
                logging.info(
                    f"Setting parsing indices for REALTIME exchange rates from {target_url}"
                )
                expected_min_cells = 11  # 실시간 환율 테이블의 최소 셀 개수
                currency_full_text_idx = 0
                buy_rate_idx = 1
                sell_rate_idx = 3
                send_rate_idx = 5
                receive_rate_idx = 6
                standard_rate_idx = 8
                current_request_headers["Referer"] = REFERER_REALTIME_EXCHANGE_URL

            elif target_url == AVERAGE_EXCHANGE_CRAWL_URL:
                logging.info(
                    f"Setting parsing indices for AVERAGE exchange rates from {target_url}"
                )
                expected_min_cells = 9  # 평균 환율 테이블의 최소 셀 개수
                currency_full_text_idx = 0
                buy_rate_idx = 1
                sell_rate_idx = 2
                send_rate_idx = 3
                receive_rate_idx = 4
                standard_rate_idx = 6
                current_request_headers["Referer"] = REFERER_AVERAGE_EXCHANGE_URL

            else:
                logging.error(
                    f"Unknown target_url provided to _fetch_and_parse_exchange_rate for parsing: {target_url}"
                )
                raise ValueError(
                    f"Unsupported URL for exchange rate parsing: {target_url}"
                )

            for row in rows:
                cells = row.find_all("td")

                if len(cells) < expected_min_cells:
                    logging.warning(
                        f"Skipping row due to insufficient cells for URL {target_url}, Inquiry Code: {log_inquiry_code}: Expected {expected_min_cells} cells, but found {len(cells)}. Raw row: {row.get_text(strip=True)}"
                    )
                    continue

                try:
                    # 각 셀의 인덱스 대신 위에 정의된 인덱스 변수를 사용
                    currency_full_text = cells[currency_full_text_idx].get_text(
                        strip=True
                    )
                    currency_parts = currency_full_text.split()
                    currency_code = (
                        currency_parts[1]
                        .replace("(100)", "")
                        .replace("(10)", "")
                        .strip()
                        if len(currency_parts) > 1
                        else currency_full_text.strip()
                    )

                    buy_rate = float(
                        cells[buy_rate_idx].get_text(strip=True).replace(",", "") or 0.0
                    )
                    sell_rate = float(
                        cells[sell_rate_idx].get_text(strip=True).replace(",", "")
                        or 0.0
                    )
                    send_rate = float(
                        cells[send_rate_idx].get_text(strip=True).replace(",", "")
                        or 0.0
                    )
                    receive_rate = float(
                        cells[receive_rate_idx].get_text(strip=True).replace(",", "")
                        or 0.0
                    )
                    standard_rate = float(
                        cells[standard_rate_idx].get_text(strip=True).replace(",", "")
                        or 0.0
                    )

                    current_crawl_time_utc = (
                        datetime.datetime.now(datetime.timezone.utc).isoformat(
                            timespec="seconds"
                        )
                        + "Z"
                    )
                    current_crawl_time_kst = datetime.datetime.now(
                        kst_timezone
                    ).isoformat(timespec="seconds")

                    rate_entry = {
                        "currency_code": currency_code,
                        "country_name": currency_code_to_country_name_map.get(
                            currency_code, None
                        ),
                        "buy_rate": buy_rate,
                        "sell_rate": sell_rate,
                        "send_rate": send_rate,
                        "receive_rate": receive_rate,
                        "standard_rate": standard_rate,
                        "crawled_at_utc": current_crawl_time_utc,
                        "crawled_at_kst": current_crawl_time_kst,
                    }
                    all_extracted_rates.append(rate_entry)
                    logging.info(
                        f"Extracted from {target_url.split('/')[-1]} (Inquiry Code: {log_inquiry_code}): {currency_code}, Standard Rate: {standard_rate}"
                    )

                except ValueError as ve:
                    logging.error(
                        f"Failed to convert rate string to float for {currency_code} (URL: {target_url}, Inquiry Code: {log_inquiry_code}): {ve}. Raw strings: B={cells[buy_rate_idx].get_text()}, S={cells[sell_rate_idx].get_text()}, Std={cells[standard_rate_idx].get_text()}",
                        exc_info=True,
                    )
                    continue
                except IndexError as ie:
                    logging.error(
                        f"Index error while parsing row (URL: {target_url}, Inquiry Code: {log_inquiry_code}): {ie}. Check cell indices. Raw row: {row.get_text(strip=True)}",
                        exc_info=True,
                    )
                    continue
                except Exception as ex:
                    logging.error(
                        f"An unexpected error occurred during row parsing (URL: {target_url}, Inquiry Code: {log_inquiry_code}): {ex}. Raw row: {row.get_text(strip=True)}",
                        exc_info=True,
                    )
                    continue

        else:
            logging.error(
                f"Exchange rate table NOT found on the page for URL: {target_url}, Inquiry Code: {log_inquiry_code}. Check HTML structure or Payload. Full HTML: {response.text[:1000]}"
            )
    except requests.exceptions.RequestException as re:
        logging.error(
            f"Network or HTTP error fetching data from {target_url}, Inquiry Code: {log_inquiry_code}: {re}",
            exc_info=True,
        )
        raise
    except ValueError as e:
        logging.error(
            f"Data parsing error in _fetch_and_parse_exchange_rate for {target_url}, Inquiry Code: {log_inquiry_code}: {e}",
            exc_info=True,
        )
        raise
    except Exception as e:
        logging.error(
            f"An unexpected general error occurred in _fetch_and_parse_exchange_rate for {target_url}, Inquiry Code: {log_inquiry_code}: {e}",
            exc_info=True,
        )
        raise
    return all_extracted_rates


# -------------------------------------------------------------
# get_exchange_rate_data 함수 (모든 유형 환율 통합 함수)
# 이 함수는 Azure Functions 트리거 파일에서 호출
# -------------------------------------------------------------
def get_exchange_rate_data() -> list:
    kst_timezone = pytz.timezone("Asia/Seoul")
    current_kst_dt = get_current_kst_datetime(kst_timezone)
    today_date_kst = current_kst_dt.date()
    today_yyyymmdd = get_kst_date_yyyymmdd(today_date_kst)
    today_with_hyphens = get_kst_date_yyyy_mm_dd(today_date_kst)
    current_year = today_date_kst.year
    current_month = today_date_kst.month

    combined_currency_data = {}

    # ------------------------------------------------------------------------------------------------------
    # 실시간 환율 데이터 크롤링
    # ------------------------------------------------------------------------------------------------------
    logging.info("Starting realtime exchange rate crawling...")
    realtime_request_data = {
        "ajax": "true",
        "curCd": "",
        "tmpInqStrDt": today_with_hyphens,
        "pbldDvCd": "3",
        "pbldsqn": "",
        "hid_key_data": "",
        "inqStrDt": today_yyyymmdd,
        "inqKindCd": "1",
        "hid_enc_data": "",
        "requestTarget": "searchContentDiv",
    }
    realtime_rates = _fetch_and_parse_exchange_rate(
        REALTIME_EXCHANGE_CRAWL_URL,
        REQUEST_HEADERS,
        realtime_request_data,
        kst_timezone,
    )
    time.sleep(random.uniform(1, 3))

    for entry in realtime_rates:
        currency_code = entry["currency_code"]  # USD, JPY, EUR 등
        rate_value = entry["standard_rate"]

        # currency_code를 국가명으로 매핑 (currency_code_to_country_name_map 사용)
        country_or_countries_for_code = currency_code_to_country_name_map.get(
            currency_code, []
        )

        if isinstance(country_or_countries_for_code, list):  # EUR처럼 리스트인 경우
            # '유럽연합'에 대한 리스트의 각 국가에 대해 데이터 복제
            for country_name in country_or_countries_for_code:
                # combined_currency_data의 키를 '국가명'으로 사용
                if country_name not in combined_currency_data:
                    combined_currency_data[country_name] = (
                        {  # <--- '국가명'을 키로 사용
                            "dataType": "exchangeRate",
                            "currency_code": currency_code,  # 통화 코드는 그대로 "EUR"
                            "country_name": country_name,  # <--- 해당 유로존 개별 국가명
                            "realtime_rate": None,
                            "realtime_crawled_at_utc": None,
                            "realtime_crawled_at_kst": None,
                            "daily_avg_rate": None,
                            "monthly_avg_rates": {},
                            "yearly_avg_rate": None,
                        }
                    )
                combined_currency_data[country_name]["realtime_rate"] = rate_value
                combined_currency_data[country_name]["realtime_crawled_at_utc"] = entry[
                    "crawled_at_utc"
                ]
                combined_currency_data[country_name]["realtime_crawled_at_kst"] = entry[
                    "crawled_at_kst"
                ]
        else:  # USD, JPY 등 단일 국가명인 경우
            country_name = country_or_countries_for_code  # "미국", "일본" 등
            if country_name not in combined_currency_data:
                combined_currency_data[country_name] = {  # <--- '국가명'을 키로 사용
                    "dataType": "exchangeRate",
                    "currency_code": currency_code,
                    "country_name": country_name,
                    "realtime_rate": None,
                    "realtime_crawled_at_utc": None,
                    "realtime_crawled_at_kst": None,
                    "daily_avg_rate": None,
                    "monthly_avg_rates": {},
                    "yearly_avg_rate": None,
                }
            combined_currency_data[country_name]["realtime_rate"] = rate_value
            combined_currency_data[country_name]["realtime_crawled_at_utc"] = entry[
                "crawled_at_utc"
            ]
            combined_currency_data[country_name]["realtime_crawled_at_kst"] = entry[
                "crawled_at_kst"
            ]

    logging.info(
        f"Completed realtime exchange rate crawling. {len(realtime_rates)} records processed."
    )

    # ------------------------------------------------------------------------------------------------------
    # 당일 일평균 환율 데이터 크롤링
    # ------------------------------------------------------------------------------------------------------
    logging.info("Starting daily average exchange rate crawling...")
    daily_request_data = {
        "ajax": "true",
        "curCd": "",
        "tmpInqStrDt": today_with_hyphens,
        "inqStrDt": today_yyyymmdd,
        "inqEndDt": today_yyyymmdd,
        "inqDvCd": "1",
        "pbldDvCd": "1",
        "tmpPbldDvCd": "1",
        "tmpInqStrDtY_m": current_kst_dt.strftime("%m"),
        "tmpInqStrDtY_y": current_kst_dt.strftime("%Y"),
        "tmpInqStrDt_p": today_yyyymmdd,
        "tmpInqEndDt_p": today_yyyymmdd,
        "requestTarget": "searchContentDiv",
        "pbldsqn": "",
        "hid_key_data": "",
        "hid_enc_data": "",
    }
    daily_avg_rates = _fetch_and_parse_exchange_rate(
        AVERAGE_EXCHANGE_CRAWL_URL, REQUEST_HEADERS, daily_request_data, kst_timezone
    )
    time.sleep(random.uniform(1, 3))

    for entry in daily_avg_rates:
        currency_code = entry["currency_code"]
        rate_value = entry["standard_rate"]

        country_or_countries_for_code = currency_code_to_country_name_map.get(
            currency_code, []
        )

        if isinstance(country_or_countries_for_code, list):
            for country_name in currency_code_to_country_name_map[
                "EUR"
            ]:  # euro_countries는 실시간 루프에서 정의, 여기서는 다시 가져와야 함.
                # 또는 직접 currency_code_to_country_name_map["EUR"] 사용
                if country_name not in combined_currency_data:
                    combined_currency_data[country_name] = {
                        "dataType": "exchangeRate",
                        "currency_code": currency_code,
                        "country_name": country_name,
                        "realtime_rate": None,
                        "realtime_crawled_at_utc": None,
                        "realtime_crawled_at_kst": None,
                        "daily_avg_rate": None,
                        "monthly_avg_rates": {},
                        "yearly_avg_rate": None,
                    }
                combined_currency_data[country_name]["daily_avg_rate"] = rate_value
        else:
            country_name = country_or_countries_for_code
            if country_name not in combined_currency_data:
                combined_currency_data[country_name] = {
                    "dataType": "exchangeRate",
                    "currency_code": currency_code,
                    "country_name": country_name,
                    "realtime_rate": None,
                    "realtime_crawled_at_utc": None,
                    "realtime_crawled_at_kst": None,
                    "daily_avg_rate": None,
                    "monthly_avg_rates": {},
                    "yearly_avg_rate": None,
                }
            combined_currency_data[country_name]["daily_avg_rate"] = rate_value
    logging.info(
        f"Completed daily average exchange rate crawling. {len(daily_avg_rates)} records processed."
    )

    # ------------------------------------------------------------------------------------------------------
    # 월평균 환율 데이터 크롤링
    # ------------------------------------------------------------------------------------------------------
    logging.info("Starting monthly average exchange rate crawling (last 3 months)...")
    monthly_avg_rates_temp = {}
    for i in range(3):
        target_month = current_month - i
        target_year = current_year
        if target_month <= 0:
            target_month += 12
            target_year -= 1
        month_first_day_yyyymmdd = get_first_day_of_month_yyyymmdd(
            target_year, target_month
        )
        month_end_day_yyyymmdd_for_inqEndDt = today_yyyymmdd
        monthly_request_data = {
            "ajax": "true",
            "curCd": "",
            "tmpInqStrDt": f"{target_year}-{target_month:02d}-01",
            "inqStrDt": month_first_day_yyyymmdd,
            "inqEndDt": month_end_day_yyyymmdd_for_inqEndDt,
            "inqDvCd": "2",
            "pbldDvCd": "1",
            "tmpPbldDvCd": "1",
            "tmpInqStrDtY_m": f"{target_month:02d}",
            "tmpInqStrDtY_y": str(target_year),
            "tmpInqStrDt_p": month_first_day_yyyymmdd,
            "tmpInqEndDt_p": month_end_day_yyyymmdd_for_inqEndDt,
            "requestTarget": "searchContentDiv",
            "pbldsqn": "",
            "hid_key_data": "",
            "hid_enc_data": "",
        }
        monthly_avg_result = _fetch_and_parse_exchange_rate(
            AVERAGE_EXCHANGE_CRAWL_URL,
            REQUEST_HEADERS,
            monthly_request_data,
            kst_timezone,
        )
        time.sleep(random.uniform(1, 3))

        for entry in monthly_avg_result:
            currency_code = entry["currency_code"]
            rate_value = entry["standard_rate"]
            country_or_countries_for_code = currency_code_to_country_name_map.get(
                currency_code, []
            )

            if isinstance(country_or_countries_for_code, list):
                for country_name in currency_code_to_country_name_map[
                    "EUR"
                ]:  # 여기도 euro_countries를 다시 가져와야 함.
                    if country_name not in monthly_avg_rates_temp:
                        monthly_avg_rates_temp[country_name] = {}
                    monthly_avg_rates_temp[country_name][
                        f"{target_year}{target_month:02d}"
                    ] = rate_value
            else:
                country_name = country_or_countries_for_code
                if country_name not in monthly_avg_rates_temp:
                    monthly_avg_rates_temp[country_name] = {}
                monthly_avg_rates_temp[country_name][
                    f"{target_year}{target_month:02d}"
                ] = rate_value

    for (
        combined_key,
        monthly_data,
    ) in monthly_avg_rates_temp.items():  # combined_key는 통화코드 또는 국가명
        if combined_key not in combined_currency_data:
            combined_currency_data[combined_key] = {
                "dataType": "exchangeRate",
                "currency_code": (
                    combined_key
                    if len(combined_key) == 3 and combined_key.isupper()
                    else None
                ),  # 통화코드는 3글자 대문자로 가정
                "country_name": currency_code_to_country_name_map.get(
                    combined_key, None
                ),
                "realtime_rate": None,
                "realtime_crawled_at_utc": None,
                "realtime_crawled_at_kst": None,
                "daily_avg_rate": None,
                "monthly_avg_rates": {},
                "yearly_avg_rate": None,
            }
        combined_currency_data[combined_key]["monthly_avg_rates"] = monthly_data
    logging.info(
        f"Completed monthly average exchange rate crawling. {len(monthly_avg_rates_temp)} currency maps processed."
    )

    # ------------------------------------------------------------------------------------------------------
    # 연평균 환율 데이터 크롤링
    # ------------------------------------------------------------------------------------------------------
    logging.info("Starting yearly average exchange rate crawling...")
    yearly_request_data = {
        "ajax": "true",
        "curCd": "",
        "tmpInqStrDt": f"{current_year}-01-01",
        "inqStrDt": get_first_day_of_year_yyyymmdd(current_year),
        "inqEndDt": today_yyyymmdd,
        "inqDvCd": "3",
        "pbldDvCd": "1",
        "tmpPbldDvCd": "1",
        "tmpInqStrDtY_m": "01",
        "tmpInqStrDtY_y": str(current_year),
        "tmpInqStrDt_p": get_first_day_of_year_yyyymmdd(current_year),
        "tmpInqEndDt_p": today_yyyymmdd,
        "requestTarget": "searchContentDiv",
        "pbldsqn": "",
        "hid_key_data": "",
        "hid_enc_data": "",
    }
    yearly_avg_rates = _fetch_and_parse_exchange_rate(
        AVERAGE_EXCHANGE_CRAWL_URL, REQUEST_HEADERS, yearly_request_data, kst_timezone
    )
    time.sleep(random.uniform(1, 3))

    for entry in yearly_avg_rates:
        currency_code = entry["currency_code"]
        rate_value = entry["standard_rate"]
        country_or_countries_for_code = currency_code_to_country_name_map.get(
            currency_code, []
        )

        if isinstance(country_or_countries_for_code, list):
            for country_name in currency_code_to_country_name_map[
                "EUR"
            ]:  # 여기도 euro_countries를 다시 가져와야 함.
                if country_name not in combined_currency_data:
                    combined_currency_data[country_name] = {
                        "dataType": "exchangeRate",
                        "currency_code": currency_code,
                        "country_name": country_name,
                        "realtime_rate": None,
                        "realtime_crawled_at_utc": None,
                        "realtime_crawled_at_kst": None,
                        "daily_avg_rate": None,
                        "monthly_avg_rates": {},
                        "yearly_avg_rate": None,
                    }
                combined_currency_data[country_name]["yearly_avg_rate"] = rate_value
        else:
            country_name = country_or_countries_for_code
            if country_name not in combined_currency_data:
                combined_currency_data[country_name] = {
                    "dataType": "exchangeRate",
                    "currency_code": currency_code,
                    "country_name": country_name,
                    "realtime_rate": None,
                    "realtime_crawled_at_utc": None,
                    "realtime_crawled_at_kst": None,
                    "daily_avg_rate": None,
                    "monthly_avg_rates": {},
                    "yearly_avg_rate": None,
                }
            combined_currency_data[country_name][
                "yearly_avg_rate"
            ] = rate_value  # <--- 여기 오류! combined_currency_data[currency_code] 대신 [country_name] 사용해야 함.
    logging.info(
        f"Completed yearly average exchange rate crawling. {len(yearly_avg_rates)} records processed."
    )

    logging.info(
        f"Starting country standardization for {len(combined_currency_data)} currency records."
    )
    final_exchange_rate_data_with_country_info = []

    for (
        original_key,
        rate_details,
    ) in combined_currency_data.items():  # original_key는 통화코드 또는 유로존 국가명
        country_info = STANDARD_COUNTRY_MAP.get(
            original_key, {}
        )  # 먼저 original_key로 STANDARD_COUNTRY_MAP에서 찾음

        # 만약 original_key가 STANDARD_COUNTRY_MAP에 직접 매핑되지 않았다면,
        # rate_details에 있는 currency_code를 사용하여 다시 시도합니다.
        # (예: combined_currency_data의 키가 유로존 국가명이고, 맵에는 통화코드만 매핑되어 있는 경우)
        if (
            not country_info
            and "currency_code" in rate_details
            and rate_details["currency_code"] in STANDARD_COUNTRY_MAP
        ):
            country_info = STANDARD_COUNTRY_MAP.get(rate_details["currency_code"], {})

        # 맵에서 정보를 찾지 못하면 original_key 또는 "Unknown_..."으로 대체
        country_korean_name = country_info.get("korean_name", original_key)
        country_english_name = country_info.get("english_name", "Unknown_English")
        country_code_3 = country_info.get("country_code_3", "N/A")
        country_code_2 = country_info.get("country_code_2", "N/A")

        rate_details["country_korean_name"] = country_korean_name
        rate_details["country_english_name"] = country_english_name
        rate_details["country_code_3"] = country_code_3
        rate_details["country_code_2"] = country_code_2

        if "country_name" in rate_details:
            del rate_details["country_name"]

        exchange_rate_score = 0.0
        exchange_rate_change_percent = None

        realtime_rate = rate_details.get("realtime_rate")
        yearly_avg_rate = rate_details.get("yearly_avg_rate")

        # 실시간 환율과 연평균 환율이 모두 유효하고 연평균이 0보다 큰 경우에만 점수 계산
        if (
            realtime_rate is not None
            and yearly_avg_rate is not None
            and yearly_avg_rate > 0
        ):
            # 변동률 계산 : (실시간 환율 - 연평균 환율) / 연평균 환율 * 100
            exchange_rate_change_percent = (
                (realtime_rate - yearly_avg_rate) / yearly_avg_rate
            ) * 100

            # 점수 변환 (환율이 내리면 가점, 오르면 감점)
            max_change_percent = 10.0  # 최대 허용 상승 변동률
            min_change_percent = -10.0
            range_of_change = max_change_percent - min_change_percent

            if range_of_change > 0:
                # 점수 계산 : (최대 좋은 값 - 현재 변동률) / (총 범위) * 100
                # 환율은 낮을수록 좋으므로, 변동률이 낮을수록(마이너스값) 점수가 높아지도록 계산
                calculated_score = (
                    (max_change_percent - exchange_rate_change_percent)
                    / range_of_change
                ) * 100.0
                exchange_rate_score = max(0.0, min(calculated_score, 100.0))
            else:
                # 연 평균이 0 이거나 범위설정이 잘못 된 경우
                exchange_rate_score = 50.0  # 기본값

        rate_details["exchange_rate_score"] = round(exchange_rate_score, 2)

        final_exchange_rate_data_with_country_info.append(rate_details)

    logging.info(
        f"Total {len(final_exchange_rate_data_with_country_info)} combined currency records prepared with standardized country info."
    )
    return final_exchange_rate_data_with_country_info
