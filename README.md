# 정부 표준 PDF → JSON → DB 정규화 시스템

정부/공공기관 문서의 PDF를 JSON으로 변환하고, 3NF/BCNF 정규화를 거쳐 MySQL 데이터베이스에 적재하는 통합 시스템

## 🎯 주요 기능

- **PDF → JSON 변환**: PDF 문서를 구조화된 JSON 형식으로 추출
- **데이터 정규화**: 3NF/BCNF 표준에 따른 데이터 정규화
- **DB 적재**: MySQL 데이터베이스에 정규화된 데이터 적재
- **동적 연도 처리**: 문서별 연도 자동 감지 및 처리
- **스케줄 분할**: 병합된 분기(예: "1/4분기~2/4분기")를 개별 레코드로 분할
- **100% 정확도**: 특허, 논문, 인력 등 숫자 데이터 정확한 추출

## 📁 프로젝트 구조

```
/home/user/webapp/
├── main.py                              # 메인 통합 파이프라인
├── extract_pdf_to_json.py              # PDF → JSON 변환 모듈
├── normalize_government_standard.py     # 데이터 정규화 엔진
├── load_government_standard_db.py       # DB 적재 모듈
├── config.py                            # 설정 파일
├── requirements.txt                     # 의존성 패키지
├── input/                               # PDF 입력 폴더
├── output/                              # JSON 출력 폴더
├── normalized_output_proper/            # 정규화된 CSV 출력
└── reports/                             # 처리 보고서
```

## 🚀 사용법

### 1. 기본 실행 (input 폴더의 PDF 처리)
```bash
python main.py
```

### 2. 샘플 데이터로 테스트 (PDF 없이)
```bash
python main.py --sample
```

### 3. DB 적재 스킵 (정규화까지만)
```bash
python main.py --skip-db
```

### 4. 특정 PDF 파일 처리
```bash
python main.py --file path/to/document.pdf
```

### 5. 디버그 모드
```bash
python main.py --debug
```

## 📊 데이터 구조

### 내역사업 기반 계층 구조
```
내역사업명 (Sub-Project)
    ├── 사업개요 (Overview)
    ├── 추진실적 (Performance) 
    │   ├── 특허 (Patents)
    │   ├── 논문 (Papers)
    │   └── 인력양성 (Human Resources)
    └── 추진계획 (Plan)
        ├── 일정 (Schedule) - 분기별 분할
        └── 예산 (Budget)
```

### 정규화된 테이블
- `sub_projects`: 내역사업 마스터
- `raw_data`: 원본 데이터 보존
- `normalized_schedules`: 정규화된 일정 (분기별 분할)
- `normalized_performances`: 정규화된 성과 지표
- `normalized_budgets`: 정규화된 예산 데이터
- `normalized_overviews`: 사업개요 정보

## 🔧 설치

### 필수 패키지 설치
```bash
pip install -r requirements.txt
```

### MySQL 설정 (DB 적재 시)
`config.py`에서 MySQL 연결 정보 설정:
```python
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'government_standard',
    'charset': 'utf8mb4'
}
```

## 📈 처리 흐름

1. **PDF 추출**: `extract_pdf_to_json.py`
   - PDF 파일을 읽어 페이지별 텍스트와 테이블 추출
   - 구조화된 JSON 형식으로 저장

2. **데이터 정규화**: `normalize_government_standard.py`
   - JSON 데이터를 3NF/BCNF 표준에 맞춰 정규화
   - 분기별 일정 분할 ("1/4~2/4" → Q1, Q2 개별 레코드)
   - 연도별 성과/예산 데이터 분리

3. **DB 적재**: `load_government_standard_db.py`
   - 정규화된 데이터를 MySQL에 적재
   - 외래키 제약조건 및 CASCADE 설정
   - 트랜잭션 관리로 데이터 무결성 보장

## 🎓 정부 표준 준수

- **원본 데이터 보존**: raw_data 테이블에 원본 그대로 저장
- **정규화 뷰 제공**: normalized_* 테이블로 분석 가능한 구조 제공
- **감사 추적**: 모든 데이터에 원본 참조 ID 유지
- **표준 코드 체계**: GOV-YYYY-XXX 형식의 프로젝트 코드

## 📝 라이센스

MIT License

## 👥 문의

기술 지원이 필요하시면 이슈를 등록해 주세요.