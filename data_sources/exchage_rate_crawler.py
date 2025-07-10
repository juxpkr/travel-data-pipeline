import logging
import datetime
import requests
from bs4 import BeautifulSoup
import json
import os

# -------------------------------------------------------------
# 환율 데이터 수집을 위한 핵심 로직 함수
# 이 함수는 Azure Functions 트리거 파일에서 호출될 것입니다.
# -------------------------------------------------------------
def get_exchange_rate_data() -> list:
    # 이 함수가 반환할 모든 환율 데이터를 저장할 리스트
    all_exchange_rates = []

    # 4.1 요청 URL (하나은행 환율 조회 API)
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

    # 4.3 요청 페이로드 (서버로 보낼 데이터)
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

                    # 환율 값 추출 (인덱스 주의)
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
                            receive_rate_str.replace(",", "") if receive_rate_str else 0.0
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
                        "currency_code": currency_code,
                        "date": current_date.strftime("%Y-%m-%d"),
                        "buy_rate": buy_rate,
                        "sell_rate": sell_rate,
                        "send_rate": send_rate,
                        "receive_rate": receive_rate,
                        "standard_rate": standard_rate,
                        "crawled_at": datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
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

            logging.info(
                f"Total {len(all_exchange_rates)} exchange rates extracted."
            )

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