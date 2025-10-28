# 정부/공공기관 표준 데이터 정규화 시스템

## 📋 개요
PDF 문서에서 추출한 데이터를 정부/공공기관 표준에 맞춰 정규화하고 데이터베이스에 적재하는 종합 시스템입니다.

## 🎯 주요 특징

### 1. 정부 표준 준수
- **원본 데이터 보존**: 감사 추적을 위한 완전한 원본 데이터 보관
- **정규화 데이터 분리**: 분석 최적화를 위한 별도 정규화 테이블
- **3NF/BCNF 준수**: 데이터 중복 최소화 및 무결성 보장

### 2. 핵심 기능

#### 📅 일정 정규화
- **병합 분기 분리**: "1/4분기~3/4분기" → 개별 분기별 레코드 생성
- **연중 작업 처리**: 4개 분기 모두에 대한 레코드 생성
- **날짜 자동 계산**: 분기별 시작/종료 날짜 자동 생성

```python
# 예시: 병합된 분기 처리
"1/4분기~2/4분기" → 
  - Q1: 2024-01-01 ~ 2024-03-31
  - Q2: 2024-04-01 ~ 2024-06-30
```

#### 📊 성과 데이터 정규화
- **100% 정확도**: 특허, 논문 등 숫자 데이터 정확한 추출
- **지표별 분리**: 카테고리(특허/논문/인력양성) × 타입(국내/국외/SCIE 등)
- **원본 텍스트 보존**: 추후 검증을 위한 원본 유지

```python
# 예시: 특허 데이터 정규화
"특허 1,001 125 74 10" →
  - 국내출원: 1,001건
  - 국내등록: 125건
  - 국외출원: 74건
  - 국외등록: 10건
```

#### 💰 예산 정규화
- **다년도 처리**: 연도별 개별 레코드 생성
- **예산 유형 분리**: 정부/민간/지방비/기타
- **실적/계획 구분**: is_actual 플래그로 구분

## 🏗️ 시스템 구조

```
정부 표준 정규화 시스템
├── 데이터 정규화 (normalize_government_standard.py)
│   ├── 원본 데이터 저장 (raw_data)
│   ├── 일정 정규화 (normalized_schedules)
│   ├── 성과 정규화 (normalized_performances)
│   └── 예산 정규화 (normalized_budgets)
├── 데이터베이스 적재 (load_government_standard_db.py)
│   ├── 테이블 자동 생성
│   ├── 외래키 관계 설정
│   └── 데이터 무결성 검증
└── 시각화 및 검증 (main_government_standard.py)
    ├── 데이터 완전성 검사
    ├── 통계 차트 생성
    └── 보고서 생성
```

## 📊 데이터베이스 스키마

### 마스터 테이블
```sql
CREATE TABLE sub_projects (
    id INT PRIMARY KEY,
    project_code VARCHAR(100),      -- 프로젝트 코드
    department_name VARCHAR(200),    -- 담당부처
    main_project_name VARCHAR(500),  -- 세부사업명
    sub_project_name VARCHAR(500),   -- 내역사업명
    document_year INT                -- 문서 연도
);
```

### 원본 데이터 (감사 추적용)
```sql
CREATE TABLE raw_data (
    id INT PRIMARY KEY,
    sub_project_id INT,
    data_type VARCHAR(50),           -- 'overview', 'performance', 'plan'
    raw_content JSON,                -- 원본 데이터 JSON
    page_number INT,
    table_index INT,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id)
);
```

### 정규화 테이블
```sql
-- 일정
CREATE TABLE normalized_schedules (
    id INT PRIMARY KEY,
    sub_project_id INT,
    year INT,
    quarter INT,                     -- 1, 2, 3, 4 또는 0(연중)
    task_category VARCHAR(200),
    task_description TEXT,
    original_period VARCHAR(100)     -- 원본 기간 표현
);

-- 성과
CREATE TABLE normalized_performances (
    id INT PRIMARY KEY,
    sub_project_id INT,
    indicator_category VARCHAR(100), -- '특허', '논문', '인력양성'
    indicator_type VARCHAR(200),     -- '국내출원', 'SCIE', '박사배출'
    value INT,
    unit VARCHAR(50)
);

-- 예산
CREATE TABLE normalized_budgets (
    id INT PRIMARY KEY,
    sub_project_id INT,
    budget_year INT,
    budget_type VARCHAR(100),        -- '정부', '민간', '지방비'
    amount DECIMAL(15, 2),
    is_actual BOOLEAN                -- 실적/계획 구분
);
```

## 🚀 사용 방법

### 1. 기본 실행
```bash
# 전체 파이프라인 실행 (정규화 → DB 적재 → 시각화)
python main_government_standard.py
```

### 2. 테스트 실행 (DB 없이)
```bash
# 정규화 및 시각화만 테스트
python test_government_pipeline.py
```

### 3. 개별 모듈 사용
```python
# 정규화만 실행
from normalize_government_standard import GovernmentStandardNormalizer

normalizer = GovernmentStandardNormalizer(
    json_path="output/data.json",
    output_dir="normalized_output_government"
)
normalizer.process_document()
normalizer.save_to_csv()
```

## 📈 처리 결과

### 테스트 데이터 처리 결과
- **내역사업**: 3개
- **원본 데이터**: 15건
- **정규화 일정**: 21건 (병합 분기 분리됨)
- **정규화 성과**: 2건
- **정규화 예산**: 30건 (연도별/유형별 분리)
- **데이터 검증**: 100% 통과 ✅

### 생성된 시각화
1. **데이터 분포도**: 테이블별 레코드 수 및 비율
2. **분기별 일정**: 분기별 작업 분포
3. **성과 지표**: 카테고리별 성과 집계
4. **예산 분석**: 연도별/유형별 예산 분포
5. **사업별 커버리지**: 내역사업별 데이터 완성도

## 🔍 데이터 검증

### 자동 검증 항목
- ✅ 모든 내역사업에 대한 데이터 존재 여부
- ✅ 외래키 참조 무결성
- ✅ 고아 레코드 확인
- ✅ 데이터 누락 사업 검출
- ✅ 분기별 일정 완성도

### 검증 통계
```
📊 데이터 무결성 검증 결과:
- 총 내역사업: 3개
- 원본 데이터: 15건
- 정규화 데이터:
  • 일정: 21건
  • 성과: 2건
  • 예산: 30건
  • 개요: 6건
- 고아 레코드: 0개 테이블
- 누락 데이터 사업: 0개
```

## 🛠️ 기술 스택
- **언어**: Python 3.12
- **데이터베이스**: MySQL 8.0
- **시각화**: Matplotlib, Seaborn
- **데이터 처리**: Pandas, NumPy
- **정규화**: 3NF/BCNF, EAV 패턴

## 📝 주요 설계 결정

### 1. 원본 보존 + 정규화 분리
- **이유**: 정부 감사 대응 및 데이터 추적성 확보
- **구현**: raw_data 테이블에 JSON 형태로 원본 보관

### 2. 분기 데이터 개별 레코드화
- **이유**: 분석 및 집계 쿼리 최적화
- **구현**: "1/4분기~2/4분기" → 2개 개별 레코드

### 3. 성과 지표 세분화
- **이유**: 지표별 정확한 추적 및 비교 분석
- **구현**: 카테고리 × 타입 조합으로 개별 레코드

## 📌 참고 사항
- 모든 데이터는 UTF-8 인코딩 사용
- 날짜는 ISO 8601 형식 (YYYY-MM-DD)
- 금액 단위는 백만원
- 한글 처리 시 matplotlib 폰트 설정 필요

## 🔄 향후 개선 사항
- [ ] 실시간 데이터 업데이트 지원
- [ ] 다중 문서 배치 처리
- [ ] RESTful API 제공
- [ ] 대시보드 웹 인터페이스
- [ ] 엑셀 내보내기 기능

---
**마지막 업데이트**: 2025-10-28
**버전**: 1.0.0