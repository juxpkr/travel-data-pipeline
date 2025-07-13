import logging
import datetime
import requests
from bs4 import BeautifulSoup
import json
import os
import pytz

from data_sources.retry_utils import create_retry_decorator
from requests.exceptions import RequestException


# 재시도 데코레이터 생성
exchange_rate_api_retry = create_retry_decorator(
    min_wait_seconds=20,
    max_wait_seconds=120,
    max_attempts=3,
    retry_exceptions=(requests.exceptions.RequestException),
)

# 통화 코드와 국가명 매핑 딕셔너리
# Stream Analytics에서 Google Trends와 조인하기 위해 필수적임
currency_code_to_country_name_map = {
    "USD": "미국",
    "JPY": "일본",
    "EUR": "유럽연합",
    "CNY": "중국",
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


# -------------------------------------------------------------
# 환율 데이터 수집을 위한 핵심 로직 함수
# 이 함수는 Azure Functions 트리거 파일에서 호출된다.
# -------------------------------------------------------------
@exchange_rate_api_retry  # 재시도 데코레이터
def get_exchange_rate_data() -> list:
    # 이 함수가 반환할 모든 환율 데이터를 저장할 리스트
    all_exchange_rates = []

    kst_timezone = pytz.timezone("Asia/Seoul")
    current_crawl_time_kst = datetime.datetime.now(kst_timezone).isoformat()

    # 요청 URL (하나은행 환율 조회 API)
    target_url = "https://www.kebhana.com/cms/rate/wpfxd651_01i_01.do"

    # 현재 날짜 설정 (YYYYMMDD 및 YYYY-MM-DD 형식)
    current_date = datetime.date.today()
    today_yyyymmdd = current_date.strftime("%Y%m%d")
    today_with_hyphens = current_date.strftime("%Y-%m-%d")

    # 4.2 요청 헤더 (웹 브라우저처럼 보이도록 설정)
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
        "Accept": "text/javascript, text/html, application/xml, text/xml, */*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://www.kebhana.com/cms/rate/index.do?contentUrl=/cms/rate/wpfxd651_01i.do",
        "X-Requested-With": "XMLHttpRequest",
    }

    # 요청 페이로드
    request_data = {
        "ajax": "true",
        "curCd": "",  # 모든 통화를 가져오기 위해 빈 값으로 설정
        "tmpInqStrDt": today_with_hyphens,
        "pbldDvCd": "3",
        "pbldsqn": "",
        "hid_key_data": "",
        "inqStrDt": today_yyyymmdd,
        "inqKindCd": "1",
        "hid_enc_data": "",
        "requestTarget": "searchContentDiv",
    }

    try:
        logging.info(f"Attempting to send POST request to: {target_url}")

        response = requests.post(
            target_url, headers=request_headers, data=request_data, timeout=15
        )

        response.raise_for_status()  # HTTP 상태 코드 200 OK가 아니면 오류 발생

        logging.info(f"Successfully received response (Status: {response.status_code})")
        logging.info(f"Response content (first 500 chars): \n{response.text[:500]}")

        # ---------------------- HTML 파싱 ----------------------

        # BeautifulSoup 객체 생성
        soup = BeautifulSoup(response.text, "html.parser")
        logging.info("BeautifulSoup object created successfully.")

        # 환율 데이터 HTML 테이블 찾기
        exchange_rate_table = soup.find("table", class_="tblBasic leftNone")

        if exchange_rate_table:
            logging.info("Exchange rate table found. Starting data extraction.")

            # Table의 <tbody> 찾기
            table_body = exchange_rate_table.find("tbody")

            if not table_body:
                logging.error("Table body not found.")
                raise ValueError("tbody not found in exchange rate table.")

            # <tr> (행) 찾기
            rows = table_body.find_all("tr")
            logging.info(f"Found {len(rows)} data rows in table.")

            # 각 행을 순회하며 데이터 추출
            for row in rows:
                cells = row.find_all("td")

                if len(cells) < 11:  # 최소한의 셀 개수 확인
                    logging.warning(
                        f"Skipping row due to insufficient cells: {row.get_text(strip=True)}"
                    )
                    continue

                try:
                    # 통화 정보 추출 (예: '미국 USD'에서 'USD' 추출)
                    currency_full_text = cells[0].get_text(strip=True)
                    currency_parts = currency_full_text.split()
                    if len(currency_parts) > 1:
                        currency_code = (
                            currency_parts[1]
                            .replace("(100)", "")
                            .replace("(10)", "")
                            .strip()
                        )
                    else:
                        currency_code = currency_full_text.strip()

                    # 환율 값 추출 (인덱스 주의!!)
                    buy_rate_str = cells[1].get_text(strip=True)
                    sell_rate_str = cells[3].get_text(strip=True)
                    send_rate_str = cells[5].get_text(strip=True)
                    receive_rate_str = cells[6].get_text(strip=True)
                    standard_rate_str = cells[8].get_text(strip=True)

                    # 문자열을 숫자로 변환 (쉼표 제거, 없으면 0.0)
                    try:
                        buy_rate = float(
                            buy_rate_str.replace(",", "") if buy_rate_str else 0.0
                        )
                        sell_rate = float(
                            sell_rate_str.replace(",", "") if sell_rate_str else 0.0
                        )
                        send_rate = float(
                            send_rate_str.replace(",", "") if send_rate_str else 0.0
                        )
                        receive_rate = float(
                            receive_rate_str.replace(",", "")
                            if receive_rate_str
                            else 0.0
                        )
                        standard_rate = float(
                            standard_rate_str.replace(",", "")
                            if standard_rate_str
                            else 0.0
                        )

                    except ValueError as ve:
                        logging.error(
                            f"Failed to convert rate string to float for {currency_code}: {ve}. Raw strings: B={buy_rate_str}, S={sell_rate_str}, Std={standard_rate_str}"
                        )
                        continue  # 이 행의 데이터는 건너뛰고 다음 행으로

                    # 추출된 데이터를 딕셔너리로 구성
                    rate_entry = {
                        "dataType": "exchangeRate",
                        "currency_code": currency_code,
                        "country_name": currency_code_to_country_name_map.get(
                            currency_code, None
                        ),
                        "date": current_date.strftime("%Y-%m-%d"),
                        "buy_rate": buy_rate,
                        "sell_rate": sell_rate,
                        "send_rate": send_rate,
                        "receive_rate": receive_rate,
                        "standard_rate": standard_rate,
                        "crawled_at_kst": current_crawl_time_kst,
                    }
                    all_exchange_rates.append(rate_entry)
                    logging.info(
                        f"Extracted: {currency_code}, Standard Rate: {standard_rate}"
                    )

                except IndexError as ie:
                    logging.error(
                        f"Index error while parsing row: {ie}. Check cell indices. Raw row: {row.get_text(strip=True)}"
                    )
                except Exception as ex:
                    logging.error(
                        f"An unexpected error occurred during row parsing: {ex}. Raw row: {row.get_text(strip=True)}"
                    )

            logging.info(f"Total {len(all_exchange_rates)} exchange rates extracted.")

        else:
            logging.error(
                "Exchange rate table (class 'tblBasic leftNone') NOT found on the page. Check HTML structure."
            )

    except requests.exceptions.HTTPError as e:
        logging.error(
            f"HTTP Error occurred: {e.response.status_code} - {e.response.text}"
        )
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection Error: {e}")
    except requests.exceptions.Timeout as e:
        logging.error(f"Request Timeout: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"An unexpected requests error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return all_exchange_rates
