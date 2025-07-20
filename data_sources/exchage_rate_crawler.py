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

# data_sources.retry_utils는 외부 모듈이므로, 현재 환경에 맞게 경로 설정이 필요할 수 있습니다.
# 예를 들어, 같은 디렉토리에 retry_utils.py가 있다면 'from .retry_utils import exchange_rate_api_retry'
# 아니면 sys.path.append를 통해 경로를 추가해야 합니다.
# 여기서는 원본 코드의 import를 존중하여 그대로 두었습니다.
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

# --- MASTER_COUNTRY_CRAWLER_MAP 로딩 ---
# 기존 STANDARD_COUNTRY_MAP 대신 MASTER_COUNTRY_CRAWLER_MAP 사용
MASTER_COUNTRY_CRAWLER_MAP = {}
EUROZONE_COUNTRIES_INFO = []  # 유로존 국가 정보 리스트 (EUR 통화에 매핑될 국가들)

# 맵 파일 경로
# 부모 디렉토리의 config 폴더 안의 master_country_crawler.json을 참조합니다.
MASTER_MAP_FILE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "config", "master_country_crawler.json"
)

try:
    with open(MASTER_MAP_FILE_PATH, "r", encoding="utf-8") as f:
        MASTER_COUNTRY_CRAWLER_MAP = json.load(f)
    logging.info(
        f"Master country mapping data loaded successfully from {MASTER_MAP_FILE_PATH}."
    )

    # MASTER_COUNTRY_CRAWLER_MAP을 순회하며 EUR 통화를 사용하는 유로존 국가들을 미리 식별
    for country_code_3, country_info in MASTER_COUNTRY_CRAWLER_MAP.items():
        if (
            country_info.get("is_euro_zone")
            and country_info.get("currency_code") == "EUR"
        ):
            EUROZONE_COUNTRIES_INFO.append(country_info)
    logging.info(f"Identified {len(EUROZONE_COUNTRIES_INFO)} Eurozone countries.")

except FileNotFoundError:
    logging.critical(f"Mapping file not found at {MASTER_MAP_FILE_PATH}. Exiting.")
    sys.exit(1)  # 중요한 파일이 없으므로 프로그램 종료

except json.JSONDecodeError as e:
    logging.critical(f"Error decoding JSON mapping file: {e}. Exiting.")
    sys.exit(1)  # JSON 파일 손상이므로 프로그램 종료

except Exception as e:
    logging.critical(f"Unexpected error loading mapping file: {e}. Exiting.")
    sys.exit(1)  # 기타 심각한 오류이므로 프로그램 종료


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

        # Referer 설정
        if target_url == REALTIME_EXCHANGE_CRAWL_URL:
            current_request_headers["Referer"] = REFERER_REALTIME_EXCHANGE_URL
        elif target_url == AVERAGE_EXCHANGE_CRAWL_URL:
            current_request_headers["Referer"] = REFERER_AVERAGE_EXCHANGE_URL

        log_inquiry_code = data.get("inqDvCd") or data.get("inqKindCd")
        logging.info(
            f"Attempting to send POST request to: {target_url} with inquiry code: {log_inquiry_code} and payload: {data}"
        )
        response = requests.post(
            target_url, headers=current_request_headers, data=data, timeout=15
        )
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
                # Referer는 위에서 이미 설정했으므로 여기서 중복 설정 제거

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
                # Referer는 위에서 이미 설정했으므로 여기서 중복 설정 제거

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
                    currency_full_text = cells[currency_full_text_idx].get_text(
                        strip=True
                    )
                    currency_parts = currency_full_text.split()
                    # 통화 코드 추출 로직 유지 (뒤의 (100) 등 제거)
                    currency_code = (
                        currency_parts[1]
                        .replace("(100)", "")
                        .replace("(10)", "")
                        .strip()
                        if len(currency_parts) > 1
                        else currency_full_text.strip()
                    )

                    # Rates can be empty or "-" which should be treated as 0.0 or None
                    buy_rate_str = (
                        cells[buy_rate_idx].get_text(strip=True).replace(",", "")
                    )
                    sell_rate_str = (
                        cells[sell_rate_idx].get_text(strip=True).replace(",", "")
                    )
                    send_rate_str = (
                        cells[send_rate_idx].get_text(strip=True).replace(",", "")
                    )
                    receive_rate_str = (
                        cells[receive_rate_idx].get_text(strip=True).replace(",", "")
                    )
                    standard_rate_str = (
                        cells[standard_rate_idx].get_text(strip=True).replace(",", "")
                    )

                    buy_rate = (
                        float(buy_rate_str)
                        if buy_rate_str and buy_rate_str != "-"
                        else 0.0
                    )
                    sell_rate = (
                        float(sell_rate_str)
                        if sell_rate_str and sell_rate_str != "-"
                        else 0.0
                    )
                    send_rate = (
                        float(send_rate_str)
                        if send_rate_str and send_rate_str != "-"
                        else 0.0
                    )
                    receive_rate = (
                        float(receive_rate_str)
                        if receive_rate_str and receive_rate_str != "-"
                        else 0.0
                    )
                    standard_rate = (
                        float(standard_rate_str)
                        if standard_rate_str and standard_rate_str != "-"
                        else 0.0
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
                        # 여기서는 country_name을 직접 매핑하지 않고,
                        # get_exchange_rate_data에서 MASTER_COUNTRY_CRAWLER_MAP을 통해 최종 매핑
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
                        f"Failed to convert rate string to float for currency_full_text: '{currency_full_text}' (URL: {target_url}, Inquiry Code: {log_inquiry_code}): {ve}. Raw strings: B='{buy_rate_str}', S='{sell_rate_str}', Send='{send_rate_str}', Receive='{receive_rate_str}', Std='{standard_rate_str}'",
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

    # combined_currency_data의 키는 country_code_3 (CAN, USA 등) 또는 country_name_kor (유로존 국가의 경우)
    combined_currency_data = {}

    # MASTER_COUNTRY_CRAWLER_MAP에서 통화 코드를 키로 하여 국가 정보를 빠르게 찾을 수 있는 맵 생성
    # EUR처럼 여러 국가가 동일한 통화 코드를 사용하는 경우를 대비하여 리스트로 저장
    currency_code_to_country_map_for_processing = {}
    for country_code_3, country_info in MASTER_COUNTRY_CRAWLER_MAP.items():
        currency_code = country_info.get("currency_code")
        if currency_code:
            if currency_code not in currency_code_to_country_map_for_processing:
                currency_code_to_country_map_for_processing[currency_code] = []
            currency_code_to_country_map_for_processing[currency_code].append(
                country_info
            )

    # 예외적으로 EUR만 따로 처리할 것이므로, 유로존 국가 리스트를 직접 참조
    # EUROZONE_COUNTRIES_INFO는 이미 전역으로 로딩 시점에 준비되어 있습니다.

    # 헬퍼 함수: 환율 데이터를 combined_currency_data에 추가하는 로직
    def _add_rate_to_combined_data(
        entry_currency_code: str,
        rate_type: str,
        rate_value,
        crawled_utc=None,
        crawled_kst=None,
        month_year_key=None,
    ):
        target_countries = []
        if entry_currency_code == "EUR":
            target_countries = EUROZONE_COUNTRIES_INFO
        else:
            # 단일 통화 코드에 매핑되는 국가를 찾음 (대부분의 경우 리스트에 하나만 있을 것임)
            # 만약 해당 통화 코드가 마스터 맵에 없다면 건너뛴다.
            if entry_currency_code not in currency_code_to_country_map_for_processing:
                logging.warning(
                    f"Currency code '{entry_currency_code}' not found in MASTER_COUNTRY_CRAWLER_MAP. Skipping."
                )
                return
            target_countries = currency_code_to_country_map_for_processing.get(
                entry_currency_code, []
            )

        if not target_countries:
            logging.warning(
                f"No target countries found for currency code '{entry_currency_code}'. Skipping rate update for type '{rate_type}'."
            )
            return

        for country_info in target_countries:
            # combined_currency_data의 키는 country_code_3으로 통일
            country_key = country_info.get("country_code_3")
            if not country_key:
                logging.error(
                    f"Country info missing 'country_code_3' for {country_info.get('country_name_kor', 'Unknown Country')}. Skipping."
                )
                continue

            if country_key not in combined_currency_data:
                combined_currency_data[country_key] = {
                    "dataType": "exchangeRate",
                    "currency_code": country_info.get("currency_code"),
                    "country_korean_name": country_info.get("country_name_kor"),
                    "country_english_name": country_info.get("country_name_eng"),
                    "country_code_2": country_info.get("country_code_2"),
                    "country_code_3": country_info.get("country_code_3"),
                    "is_euro_zone": country_info.get(
                        "is_euro_zone", False
                    ),  # is_euro_zone 추가
                    "realtime_rate": None,
                    "realtime_crawled_at_utc": None,
                    "realtime_crawled_at_kst": None,
                    "daily_avg_rate": None,
                    "monthly_avg_rates": {},
                    "yearly_avg_rate": None,
                }

            if rate_type == "realtime":
                combined_currency_data[country_key]["realtime_rate"] = rate_value
                combined_currency_data[country_key][
                    "realtime_crawled_at_utc"
                ] = crawled_utc
                combined_currency_data[country_key][
                    "realtime_crawled_at_kst"
                ] = crawled_kst
            elif rate_type == "daily_avg":
                combined_currency_data[country_key]["daily_avg_rate"] = rate_value
            elif rate_type == "monthly_avg":
                if month_year_key:
                    combined_currency_data[country_key]["monthly_avg_rates"][
                        month_year_key
                    ] = rate_value
            elif rate_type == "yearly_avg":
                combined_currency_data[country_key]["yearly_avg_rate"] = rate_value
            else:
                logging.warning(
                    f"Unknown rate type: {rate_type} for currency code: {entry_currency_code}"
                )

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
        _add_rate_to_combined_data(
            entry["currency_code"],
            "realtime",
            entry["standard_rate"],
            crawled_utc=entry["crawled_at_utc"],
            crawled_kst=entry["crawled_at_kst"],
        )

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
        _add_rate_to_combined_data(
            entry["currency_code"], "daily_avg", entry["standard_rate"]
        )
    logging.info(
        f"Completed daily average exchange rate crawling. {len(daily_avg_rates)} records processed."
    )

    # ------------------------------------------------------------------------------------------------------
    # 월평균 환율 데이터 크롤링
    # ------------------------------------------------------------------------------------------------------
    logging.info("Starting monthly average exchange rate crawling (last 3 months)...")
    for i in range(3):
        target_month = current_month - i
        target_year = current_year
        if target_month <= 0:
            target_month += 12
            target_year -= 1
        month_first_day_yyyymmdd = get_first_day_of_month_yyyymmdd(
            target_year, target_month
        )

        # 월평균의 경우, 해당 월의 마지막 날짜까지 조회하는 것이 일반적이나,
        # 현재 코드에서는 `today_yyyymmdd`를 사용하고 있어 이 부분은 기존 로직을 따름
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
            _add_rate_to_combined_data(
                entry["currency_code"],
                "monthly_avg",
                entry["standard_rate"],
                month_year_key=f"{target_year}{target_month:02d}",
            )
    logging.info(
        f"Completed monthly average exchange rate crawling. {len(monthly_avg_result)} records processed for the last 3 months."
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
        _add_rate_to_combined_data(
            entry["currency_code"], "yearly_avg", entry["standard_rate"]
        )
    logging.info(
        f"Completed yearly average exchange rate crawling. {len(yearly_avg_rates)} records processed."
    )

    logging.info(
        f"Starting country standardization and final data compilation for {len(combined_currency_data)} currency records."
    )
    final_exchange_rate_data_with_country_info = []

    for country_key, rate_details in combined_currency_data.items():
        # country_key는 MASTER_COUNTRY_CRAWLER_MAP의 키(country_code_3)와 동일
        country_info = MASTER_COUNTRY_CRAWLER_MAP.get(country_key, {})

        if not country_info:
            logging.warning(
                f"No corresponding country info found in MASTER_COUNTRY_CRAWLER_MAP for key '{country_key}'. Skipping."
            )
            continue  # 매핑 정보가 없으면 해당 데이터는 건너뜀

        # rate_details에 이미 올바른 country_korean_name, country_english_name, country_code_2, country_code_3이
        # _add_rate_to_combined_data 함수에서 채워졌을 것이므로, 여기서는 그대로 사용하거나 재확인합니다.
        # 기존 필드 이름을 유지하면서 데이터를 추가하는 방향으로 코드를 개선했습니다.

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
            min_change_percent = -10.0  # 최대 허용 하락 변동률
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
        else:
            logging.warning(
                f"Cannot calculate exchange rate score for {country_info.get('country_name_kor', country_key)} "
                f"due to missing or zero realtime_rate ({realtime_rate}) or yearly_avg_rate ({yearly_avg_rate}). Setting score to 0."
            )
            exchange_rate_score = 0.0  # 점수 계산 불가 시 0점으로 설정 (필요에 따라 50.0 등 기본값 설정 가능)

        rate_details["exchange_rate_change_percent"] = (
            round(exchange_rate_change_percent, 2)
            if exchange_rate_change_percent is not None
            else None
        )
        rate_details["exchange_rate_score"] = round(exchange_rate_score, 2)

        final_exchange_rate_data_with_country_info.append(rate_details)

    logging.info(
        f"Total {len(final_exchange_rate_data_with_country_info)} combined currency records prepared with standardized country info."
    )
    return final_exchange_rate_data_with_country_info
