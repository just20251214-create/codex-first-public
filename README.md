# Yahooquery US Equity Loader

이 스크립트는 `yahooquery`의 스크리너 API를 사용해서 미국 주식시장(US region) 상장 기업 심볼을 수집하고, 각 기업의 요약 정보를 MongoDB에 저장합니다.

## 요구 사항

- Python 3.10+
- MongoDB 접속 정보

## 설치

```bash
pip install -r requirements.txt
```

## 사용 방법

```bash
python yahooquery_to_mongodb.py \
  --mongodb-uri mongodb://localhost:27017 \
  --mongodb-db market_data \
  --mongodb-collection us_equities
```

### 환경 변수

- `MONGODB_URI`: MongoDB 연결 문자열
- `MONGODB_DB`: 데이터베이스 이름
- `MONGODB_COLLECTION`: 컬렉션 이름
- `BATCH_SIZE`: yahooquery 모듈 조회 배치 크기
- `SCREENER_PAGE_SIZE`: Yahoo Finance screener 페이지 크기
- `MAX_SYMBOLS`: 처리할 심볼 수 제한

### 수집 모듈

기본 모듈은 아래 값이며, `--modules` 옵션으로 변경할 수 있습니다.

```
assetProfile,summaryProfile,quoteType,price,summaryDetail,defaultKeyStatistics,financialData
```

## 주의 사항

- Yahoo Finance API 정책/속도 제한에 따라 대량 요청 시 오류가 발생할 수 있습니다.
- 환경에 따라 프록시/네트워크 제한이 있을 수 있으니 필요하면 VPN 또는 프록시 설정을 적용하세요.
