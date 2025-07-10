import azure.functions as func
import logging
import os
import sys

# --- 로컬 개발 환경에서만 필요한 sys.path 설정 ---
if (
    os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT") != "Production"
    and os.environ.get("FUNCTIONS_WORKER_RUNTIME") == "python"
):
    # function_app.py가 프로젝트 루트 디렉토리(TRAVEL-DATA-PIPELINE)에 있으므로,
    # os.path.dirname(__file__)은 이미 프로젝트 루트 디렉토리의 경로를 반환한다.
    project_root_dir = os.path.abspath(os.path.dirname(__file__))
    sys.path.append(project_root_dir)
    logging.info(
        f"Local development: Appended '{project_root_dir}' to sys.path for module resolution."
    )
else:
    logging.info(
        "Running in Azure environment or worker runtime not set to Python, skipping sys.path modification."
    )
# ------------------------------------------------

app = func.FunctionApp()

# --- 각 함수 모듈을 임포트하고 함수를 'app' 객체에 등록하는 로직 ---
from functions.exchange_rate_trigger import register_exchange_rate_crawler

# 임포트한 register_exchange_rate_crawler 함수를 호출하여
# 'app' 객체에 실제 exchangeRateCrawler 함수를 등록
register_exchange_rate_crawler(app)

# --- Google Trends Crawler 함수 ---
from functions.google_trends_trigger import register_google_trends_crawler

register_google_trends_crawler(app)


logging.info("Azure Function App initialization complete.")
