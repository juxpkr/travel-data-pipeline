# travel-data-pipiline

```
TRAVEL-DATA-PIPELINE/
├── .venv/                         # 파이썬 가상 환경 (각자 설치)
├── .vscode/                       # VS Code 설정 (디버깅, 확장 권장 등)
├── data_sources/                  # 핵심 비즈니스 로직: 데이터 수집/처리/변환 함수들.
│   ├── __init__.py                # 파이썬 패키지임을 알리는 빈 파일.
│   └── exchange_rate_crawler.py   # 예시: 환율 크롤링 로직.
├── functions/                     # 각 Azure Function의 트리거 및 바인딩 정의.
│   ├── __init__.py                # 파이썬 패키지임을 알리는 빈 파일.
│   └── exchange_rate_trigger.py   # 예시: Timer Trigger 함수
├── local_output/                  # 로컬 테스트에서 생성되는 결과물 저장 디렉토리
│                                  # gitignore에 추가되어 Git에 커밋되지 않음
├── function_app.py                # Azure Functions 앱의 중심
│                                  # 모든 개별 함수들을 여기에 등록
│                                  # 직접 함수 코드를 작성X
├── host.json                      # Functions Host 전역 설정.
├── local.settings.json            # 로컬 환경 변수 설정 (API 키, 연결 문자열 등. Git에 커밋 금지!).
├── requirements.txt               # 프로젝트 의존성 목록. `pip install -r`로 설치
├── .funcignore                    # Azure 배포 시 제외할 파일/폴더 지정
├── .gitignore                     # Git 버전 관리 시 제외할 파일/폴더 지정
└── README.md                      # 프로젝트 설명
```
1. venv 통일 : python 3.11.9 

2. local.settings.json 파일 생성

{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "EventHubConnectionString": "",
    "EventHubName": "",
    "AzureWebHookUrl": "",
    "BlobStorageConnectionString": ""
  }
}

3. Timre Trigger 설정 -> 로컬 수동 실행
schedule="0 0 0 1 1 *", run_on_startup=False
함수 자동 시작 방지
