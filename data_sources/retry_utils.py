import logging
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
from requests.exceptions import RequestException
from pytrends.exceptions import ResponseError, TooManyRequestsError


# 재시도 로깅을 위한 헬퍼 함수
def retry_log(retry_state):
    logging.warning(
        f"API 호출 재시도 중 : "
        f"시도 횟수: {retry_state.attempt_number}번째, "
        f"다음 시도까지 대기 시간: {retry_state.next_action.sleep:.2f}초. "
        f"마지막으로 발생한 오류 : {retry_state.outcome.exception()}"
    )


# 웹 크롤링/API 호출을 위한 재시도 함수
# 각 크롤러에서 이 함수를 호출하여 필요한 설정으로 재시도 로직을 적용할 수 있음
def create_retry_decorator(
    min_wait_seconds: int = 120,
    max_wait_seconds: int = 600,
    max_attempts: int = 3,
    retry_exceptions=None,
):
    if retry_exceptions is None:
        retry_exceptions = (RequestException, ResponseError, TooManyRequestsError)

    return retry(
        wait=wait_exponential(multiplier=1, min=min_wait_seconds, max=max_wait_seconds),
        stop=stop_after_attempt(max_attempts),
        retry=retry_if_exception_type(retry_exceptions),
        before_sleep=retry_log,
    )


# Google Trends API용 데코레이터
google_trends_api_retry = create_retry_decorator(
    min_wait_seconds=600,  # 10분
    max_wait_seconds=1200,  # 20분
    max_attempts=5,
    retry_exceptions=(RequestException, TooManyRequestsError, ResponseError),
)

# 환율 API용 데코레이터
exchange_rate_api_retry = create_retry_decorator(
    min_wait_seconds=20,  # 20초 (환율은 덜 민감)
    max_wait_seconds=120,  # 120초 (2분)
    max_attempts=3,  # 3회
    retry_exceptions=(RequestException,),  # RequestException만 재시도
)
