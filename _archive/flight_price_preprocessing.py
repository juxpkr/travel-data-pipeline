import pandas as pd

def extract_flight_info(data):
    itineraries = data.get('itineraries', [])
    rows = []

    for idx, itinerary in enumerate(itineraries):
        price = itinerary.get('price', {}).get('amount')
        priceBeforeDiscount = itinerary.get('price', {}).get('priceBeforeDiscount', price)
        is_discounted = price != priceBeforeDiscount

        itinerary_sector = itinerary.get('sector', {})
        sector_sectorSegments = itinerary_sector.get('sectorSegments', [])

        for sector in sector_sectorSegments:
            segment = sector.get('segment', {})
            source = segment.get('source', {})
            source_station = source.get('station', {})
            destination = segment.get('destination', {})
            dest_station = destination.get('station', {})

            row = {
                "출발_공항_코드": source_station.get('code'),
                "출발_도시_이름": source_station.get('city', {}).get('name'),
                "출발_도시_ID": source_station.get('city', {}).get('legacyId'),
                "출발_국가_코드": source_station.get('country', {}).get('code'),
                "출발_위도": source_station.get('gps', {}).get('lat'),
                "출발_경도": source_station.get('gps', {}).get('lng'),
                "출발_시간": source.get('localTime'),

                "도착_공항_코드": dest_station.get('code'),
                "도착_도시_이름": dest_station.get('city', {}).get('name'),
                "도착_도시_ID": dest_station.get('city', {}).get('legacyId'),
                "도착_국가_코드": dest_station.get('country', {}).get('code'),
                "도착_위도": dest_station.get('gps', {}).get('lat'),
                "도착_경도": dest_station.get('gps', {}).get('lng'),
                "도착_시간": destination.get('localTime'),

                "항공사": segment.get('carrier', {}).get('name'),
                "항공사_코드": segment.get('carrier', {}).get('code'),
                "운임_클래스": segment.get('cabinClass'),
                "비행_시간_초": segment.get('duration'),
                "가격": price,
                "할인여부": is_discounted
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    return df
