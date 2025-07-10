import logging
import datetime
import json
import os
import azure.functions as func

# data_sources 크롤링 로직 함수 import
from data_sources.exchage_rate_crawler import get_exchange_rate_data


# 이 함수는 외부(function_app.py)로부터 Azure Functions 앱 인스턴스(app_instance)를 받아
# 그 인스턴스에 실제 트리거 함수를 등록하는 역할을 함
def register_exchange_rate_crawler(app_instance):  # app 객체를 인자로 받는다
    """
    Azure Functions 앱 인스턴스에 exchangeRateCrawler 함수를 등록
    이 함수는 function_app.py에서 호출되어야 함!
    """

    # @app_instance.timer_trigger: 외부에서 받은 app_instance에 데코레이터를 적용한다!!
    @app_instance.timer_trigger(
        schedule="0 0 0 1 1 *",
        run_on_startup=False,
        use_monitor=False,
        arg_name="myTimer",
    )
    def exchangeRateCrawler(myTimer: func.TimerRequest) -> None:
        # 함수 시작 시간 로깅
        utc_timestamp = datetime.datetime.utcnow().isoformat()
        if myTimer.past_due:
            logging.info("Timer run was overdue!")
        logging.info(f"Python exchangeRateCrawler function started at {utc_timestamp}.")

        # get_exchange_rate_data 함수를 호출하여 실제 환율 데이터를 가져온다
        all_exchange_rates_data = get_exchange_rate_data()

        # 가져온 데이터가 있다면 처리
        if all_exchange_rates_data:
            logging.info(
                f"Total {len(all_exchange_rates_data)} exchange rates extracted."
            )
            logging.info(
                json.dumps(all_exchange_rates_data, indent=2, ensure_ascii=False)
            )

            # 로컬 파일에 저장 (Azure Blob Storage 대신 -> 나중에 Blob에 저장)
            try:
                timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = f"local_exchange_rates_{timestamp_str}.json"

                output_dir = os.path.join(os.getcwd(), "local_output")
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, file_name)
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(all_exchange_rates_data, f, indent=4, ensure_ascii=False)
                logging.info(
                    f"Successfully saved exchange rates data to local file: {output_path}."
                )
            except Exception as file_ex:
                logging.error(
                    f"Failed to save exchange rates data to local file: {file_ex}."
                )
        else:
            logging.warning("No exchange rates data extracted.")

        logging.info("Python exchangeRateCrawler function completed.")
