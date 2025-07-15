import logging
import azure.functions as func
### 추가 ----------------------------------
from datetime import datetime, timedelta, timezone
# from serpapi import GoogleSearch
import requests
import pandas as pd
import os
import json

from dotenv import load_dotenv
load_dotenv()  # .env 파일의 환경 변수를 불러옵니다.

# from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient
from preprocessing_flight import extract_flight_info
### --------------------------------------

app = func.FunctionApp()
@app.timer_trigger(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) # **매일 자정(00:00:00)**에 함수가 실행되도록 변경
# timer_trigger를 사용하여 매일 자정마다 함수가 실행되도록 설정하고, 실시간 API 호출을 추가하는 방식
# run_on_startup=True 로 설정 
# @app.event_hub_output(arg_name="event", event_hub_name= os.environ["SolarEventHubName"], connection="SolarEventHubConnectionString")
@app.event_hub_output(arg_name="event", event_hub_name=os.environ["EventHubName"], connection="EventHubConnectionAppSetting")

def flight_timer_trigger_app(myTimer: func.TimerRequest, event: func.Out[str]) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    # ----------------------------------------------------------------
    url = "https://kiwi-com-cheap-flights.p.rapidapi.com/one-way"

    cities_ae = "City:dubai_ae,City:abu_dhabi_ae"
    cities_ar = "City:buenos_aires_ar"
    cities_at = "City:vienna_at,City:salzburg_at"
    cities_au = "City:sydney_au,City:canberra_au,City:brisbane_au,City:gold_coast_au,City:melbourne_au,City:perth_au"
    cities_be = "City:brussels_be"
    cities_bg = "City:sofia_bg"
    cities_ca = "City:calgary_ca,City:montreal_ca,City:ottawa_ca,City:toronto_ca,City:vancouver_ca,City:quebec_city_ca"
    cities_ch = "City:zurich_ch,City:geneva_ch"
    cities_cl = "City:santiago_cl"
    cities_cn = "City:beijing_cn,City:tianjin_cn,City:hunan_cn,City:sichuan_cn,City:shanghai_cn,City:chongqing_cn,City:guangzhou_cn,City:hebei_cn"
    cities_co = "City:bogota_co"
    cities_cr = "City:san_jose_cr"
    cities_cz = "City:prague_cz"
    cities_de = "City:berlin_de,City:hamburg_de,City:munich_de,City:frankfurt_de,City:hannover_de"
    cities_dk = "City:copenhagen_dk"
    cities_ee = "City:tallinn_ee"
    cities_es = "City:madrid_es,City:barcelona_es,City:seville_es,City:valencia_es,City:palma_de_mallorca_es,City:bilbao_es"
    cities_fr = "City:paris_fr,City:toulouse_fr"
    cities_gb = "City:london_gb,City:birmingham_gb,City:edinburgh_gb"
    cities_ge = "City:tbilisi_ge"
    cities_gr = "City:athens_gr,City:santorini_gr"
    cities_hk = "City:hong_kong_hk"
    cities_hu = "City:budapest_hu"
    cities_id = "City:jakarta_id,City:bali_id,City:lombok_id"
    cities_ie = "City:dublin_ie"
    cities_in = "City:mumbai_in,City:delhi_in"
    cities_it = "City:rome_it,City:florence_it,City:venice_it,City:milan_it,City:naples_it,City:palermo_it"
    cities_jp = "City:okinawa_jp,City:sapporo_jp,City:sendai_jp,City:tokyo_jp,City:yokohama_jp,City:nagoya_jp,City:kanazawa_jp,City:osaka_jp,City:hiroshima_jp,City:nagasaki_jp"
    cities_kh = "City:phnom_penh_kh,City:sihanoukville_kh"
    cities_la = "City:vientiane_la,City:luang_prabang_la"
    cities_lt = "City:vilnius_lt"
    cities_ma = "City:marrakech_ma"
    cities_mo = "City:macao_mo"
    cities_mx = "City:mexico_city_mx,City:cancun_mx"
    cities_my = "City:kuala_lumpur_my,City:kota_kinabalu_my"
    cities_nl = "City:amsterdam_nl"
    cities_no = "City:oslo_no,City:bergen_no,City:tromso_no"
    cities_nz = "City:auckland_nz"
    cities_ph = "City:puerto_princesa_ph,City:manila_ph,City:cebu_ph,City:boracay_ph"
    cities_pl = "City:gdansk_pl,City:krakow_pl,City:wroclaw_pl"
    cities_pt = "City:lisbon_pt,City:porto_pt"
    cities_qa = "City:doha_qa"
    cities_rs = "City:belgrade_rs"
    cities_sg = "City:singapore_sg"
    cities_si = "City:ljubljana_si"
    cities_th = "City:bangkok_th,City:phuket_th,City:chiang_mai_th,City:krabi_th,City:koh_samui_th"
    cities_tw = "City:taipei_tw,City:kao_hsiung_tw"
    cities_us = "City:new_york_city_us,City:san_francisco_us,City:los_angeles_us,City:las_vegas_us,City:boston_us,City:washington_us,City:chicago_us,City:orlando_us,City:san_diego_us,City:seattle_us,City:denver_us,City:new_orleans_us,City:miami_us,City:honolulu_us,City:anchorage_us"
    cities_vn = "City:ho_chi_minh_city_vn,City:hanoi_vn,City:da_nang_vn,City:nha_trang_vn,City:dalat_vn,City:phu_quoc_vn"
    all_cities = [cities_ae, cities_ar, cities_at, cities_au, cities_be, cities_bg, cities_ca, cities_ch, cities_cl, cities_cn, cities_co, cities_cr, cities_cz, cities_de, cities_dk, cities_ee, cities_es, cities_fr, cities_gb, cities_ge, cities_gr, cities_hk, cities_hu, cities_id, cities_ie, cities_in, cities_it, cities_jp, cities_kh, cities_la, cities_lt, cities_ma, cities_mo, cities_mx, cities_my, cities_nl, cities_no, cities_nz, cities_ph, cities_pl, cities_pt, cities_qa, cities_rs, cities_sg, cities_si, cities_th, cities_tw, cities_us, cities_vn]

    KST = timezone(timedelta(hours=9))
    today = datetime.now(KST)
    start_date = today.strftime("%Y-%m-%dT00:00:00")
    end_date = (today + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
    
    
    all_dfs = []

    # 쿼리문 (API 파라미터)
    for city in all_cities:
        querystring = {
            "source": "City:ICN",
            "destination": city,

            "outboundDepartmentDateStart": start_date,
            "outboundDepartmentDateEnd": end_date,


            "currency": "krw", # 가격 통화
            "locale": "ko", # 언어 설정
            "adults": "1",
            "children": "0",
            "infants": "0",
            "handbags": "1", # 손 가방 수
            "holdbags": "0", # 위탁 수하물 수
            "cabinClass": "ECONOMY", # 좌석 클래스
            "sortBy": "QUALITY", # 품질 기준으로 정렬
            "sortOrder": "ASCENDING",
            "applyMixedClasses": "true",  # 혼합 클래스 허용
            "allowReturnFromDifferentCity": "true",
            "allowChangeInboundDestination": "true",
            "allowChangeInboundSource": "true",
            "allowDifferentStationConnection": "true",
            "enableSelfTransfer": "true",
            "allowOvernightStopover": "true",
            "enableTrueHiddenCity": "true",
            "enableThrowAwayTicketing": "true",
            "outbound": "SUNDAY,WEDNESDAY,THURSDAY,FRIDAY,SATURDAY,MONDAY,TUESDAY", # 출발일 설정
            "transportTypes": "FLIGHT",
            "contentProviders": "FLIXBUS_DIRECTS,FRESH,KAYAK,KIWI",
            "limit": "1000" #error:"Value of `limit` cannot be higher than 1000."
        }

        headers = {
                    "x-rapidapi-key": "f60c522bbfmsh6983e28b32e3c17p1e976cjsn2492bc556c59", #os.environ["RAPIDAPI_KEY"],
                    # "x-rapidapi-key": "b5567b8804mshde03ee048341148p11f10cjsne5272c20acc4",
                    "x-rapidapi-host": "kiwi-com-cheap-flights.p.rapidapi.com",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" #추가
        }
        
        try:
            # API 호출 (반복문 - 도시마다 돌림)
            response = requests.get(url, headers=headers, params=querystring)
            
            logging.info(f"Status Code: {response.status_code}")
            logging.info(f"Content-Type: {response.headers.get('Content-Type', '')}")
            logging.info(f"Raw Text: {response.text}")

            if response.status_code == 200:
                try:
                    # [1]. JSON 응답 파싱
                    data = response.json() 

                    # ### API 응답 원본 저장 (로컬 - 확인용)
                    # current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    # raw_filename = f"Azure_Raw_api_{current_time}.json"
                    # with open(raw_filename, 'w', encoding='utf-8') as f:
                    #     json.dump(data, f, ensure_ascii=False, indent=2)
                    # logging.info(f"API 응답 [원본] 데이터를 '{raw_filename}' 파일로 저장했습니다.")


                    # [2]. 전처리 (사용자 정의 모듈 사용)
                    df = extract_flight_info(data)   # preprocessing_flight.py
                    all_dfs.append(df)

                    # ### 전처리 본 저장 (로컬 - 확인용)
                    # preprocessing_filename = f"Azure_Fin_api_{current_time}.csv"
                    # df.to_csv(preprocessing_filename, index=False, encoding='utf-8-sig')
                    # logging.info(f"CSV 저장 완료: {preprocessing_filename}")


                except json.JSONDecodeError as e:
                    logging.error(f"JSON 파싱 오류: {e}")
                    logging.error(f"응답 내용 : {response.text}")

            else:
                logging.error(f"API 호출 실패! 상태 코드: {response.status_code}")
                logging.error(f"오류 응답: {response.text}")
        
        except requests.exceptions.RequestException as e:
            logging.error(f"네트워크 오류: {e}")
        except Exception as e:
            logging.error(f"예상치 못한 오류: {e}")
            import traceback
            logging.error(f"오류 상세: {traceback.format_exc()}")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        current_time = datetime.now(KST).strftime("%Y-%m-%d_%H-%M-%S")

        # ### 전처리 본 저장 (🔹로컬 - 확인용)
        # final_filename = f"Azure_Fin_api_{current_time}.csv"
        # final_df.to_csv(final_filename, index=False, encoding='utf-8-sig')
        # logging.info(f"통합 CSV 저장 완료: {final_filename}")

        file_name = f"flight_price_{current_time}.csv"

        ### 🔹 Blob 업로드
        csv_data = final_df.to_csv(index=False, encoding='utf-8-sig')
        
        try:
            blob_connection_string = os.environ["BlobStorageConnectionString"]  # 로컬 또는 Azure 포털에서 환경변수 설정
            container_name = "flight-price-data"  # 저장할 컨테이너 이름 (미리 생성되어 있어야 함)

            blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

            blob_client.upload_blob(csv_data, overwrite=True)
            logging.info(f"Blob Storage에 파일 업로드 완료: {file_name}")
        except Exception as e:
            logging.error(f"Blob 업로드 실패: {e}")
            
        ### 이벤트 허브에 저장
        event.set(final_df.to_csv(index=False))
        logging.info("Event Hub로 최종 결과 전송 완료")
    else:
        logging.warning("저장할 데이터가 없습니다.")
