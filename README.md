# PDF 데이터 변환 및 MySQL DB 적재 프로그램

## 📋 프로젝트 개요
PDF 문서에서 표 데이터를 추출하여 정규화된 관계형 데이터베이스 구조로 변환하고 MySQL에 적재하는 프로그램입니다.

## 🎯 주요 기능
- **JSON 정규화**: PDF에서 추출한 JSON 데이터를 정규화된 CSV로 변환
- **관계형 DB 구조**: Foreign Key 기반의 완전한 정규화 구조
- **MySQL 자동 적재**: 테이블 생성, 데이터 적재, 관계 설정 자동화
- **데이터 무결성**: CASCADE 옵션으로 참조 무결성 보장

## 🚀 빠른 시작

### 1. 환경 설정
```bash
# 가상환경 활성화
.venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt
```

### 2. MySQL 설정
`config.py` 파일에서 MySQL 비밀번호 설정:
```python
MYSQL_CONFIG = {
    "password": "your_password_here"  # 실제 비밀번호로 변경
}
```

### 3. 실행
```bash
# PDF는 이미 JSON으로 변환되어 output 폴더에 있다고 가정
python main.py
```

## 📊 데이터베이스 구조

### 테이블 관계도
```
document_metadata (문서 메타데이터)
    └─ 독립 테이블

detail_projects (세부사업)
    └─ sub_projects (내역사업) [FK]
        ├─ sub_project_programs (세부 프로그램) [FK]
        ├─ budgets (예산) [FK]
        ├─ performances (성과) [FK]
        ├─ schedules (일정) [FK]
        └─ raw_tables (원본 테이블) [FK]
```

### 주요 테이블 설명

#### 1. detail_projects (세부사업)
- `id`: 세부사업 ID (PK)
- `name`: 세부사업명
- `department`: 담당부처
- `page_number`: 페이지 번호

#### 2. sub_projects (내역사업)
- `id`: 내역사업 ID (PK)
- `detail_project_id`: 세부사업 ID (FK)
- `name`: 내역사업명
- `overview`: 사업개요
- `objectives`: 사업목표
- `content`: 사업내용
- `page_number`: 페이지 번호

#### 3. budgets (예산)
- `id`: 예산 ID (PK)
- `sub_project_id`: 내역사업 ID (FK)
- `year`: 연도
- `total_budget`: 총 예산
- `national_budget`: 국고
- `local_budget`: 지방비
- `other_budget`: 기타

## 🔍 쿼리 예시

### 1. 세부사업별 내역사업 조회
```sql
SELECT 
    d.name AS 세부사업명,
    s.name AS 내역사업명,
    s.overview AS 사업개요
FROM detail_projects d
JOIN sub_projects s ON d.id = s.detail_project_id
ORDER BY d.id, s.id;
```

### 2. 내역사업별 총 예산 조회
```sql
SELECT 
    s.name AS 내역사업명,
    SUM(b.total_budget) AS 총예산,
    SUM(b.national_budget) AS 국고,
    SUM(b.local_budget) AS 지방비
FROM sub_projects s
JOIN budgets b ON s.id = b.sub_project_id
GROUP BY s.id, s.name
ORDER BY 총예산 DESC;
```

### 3. 내역사업별 성과지표 조회
```sql
SELECT 
    s.name AS 내역사업명,
    p.indicator AS 성과지표,
    p.target_value AS 목표값,
    p.unit AS 단위
FROM sub_projects s
JOIN performances p ON s.id = p.sub_project_id
ORDER BY s.id;
```

### 4. 특정 세부사업의 전체 정보 조회
```sql
SELECT 
    d.name AS 세부사업명,
    s.name AS 내역사업명,
    COUNT(DISTINCT pr.id) AS 프로그램수,
    COUNT(DISTINCT b.id) AS 예산항목수,
    COUNT(DISTINCT pf.id) AS 성과지표수,
    COUNT(DISTINCT sc.id) AS 일정항목수
FROM detail_projects d
JOIN sub_projects s ON d.id = s.detail_project_id
LEFT JOIN sub_project_programs pr ON s.id = pr.sub_project_id
LEFT JOIN budgets b ON s.id = b.sub_project_id
LEFT JOIN performances pf ON s.id = pf.sub_project_id
LEFT JOIN schedules sc ON s.id = sc.sub_project_id
WHERE d.name LIKE '%생명공학%'
GROUP BY d.id, d.name, s.id, s.name;
```

## 📁 프로젝트 구조

```
PythonProject/
├── main.py                      # 메인 실행 파일
├── config.py                    # 설정 파일
├── normalize_proper.py          # JSON 정규화 모듈
├── load_proper_db.py           # MySQL 적재 모듈
├── visualize_normalization.py  # 시각화 (선택)
├── requirements.txt            # 필요 패키지
├── README.md                   # 문서 (이 파일)
├── input/                      # PDF 파일 입력 폴더
├── output/                     # JSON 변환 결과 폴더
└── normalized_output_proper/   # 정규화된 CSV 출력 폴더
    ├── document_metadata.csv
    ├── detail_projects.csv
    ├── sub_projects.csv
    ├── sub_project_programs.csv
    ├── budgets.csv
    ├── performances.csv
    ├── schedules.csv
    └── raw_tables.csv
```

## 🛠️ 트러블슈팅

### MySQL 연결 실패
```bash
# MySQL 서비스 상태 확인
net start | findstr MySQL

# MySQL 서비스 시작
net start MySQL80
```

### 데이터베이스가 없는 경우
```sql
-- MySQL 접속 후
CREATE DATABASE convert_pdf CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 기존 데이터 초기화
```bash
# 프로그램 실행 시 자동으로 기존 테이블 삭제 후 재생성됨
python main.py
```

## 📝 참고 사항

### Foreign Key CASCADE 옵션
- **ON DELETE CASCADE**: 부모 레코드 삭제 시 자식 레코드도 자동 삭제
- **ON UPDATE CASCADE**: 부모 레코드 ID 변경 시 자식 레코드도 자동 업데이트

### 인덱스 설정
- Foreign Key 컬럼에 자동 인덱스 생성
- 자주 조회되는 컬럼 (name, year 등)에 추가 인덱스 설정

## 📈 성능 최적화

### 배치 INSERT
- CSV 데이터를 100건씩 묶어서 INSERT (executemany 사용)
- 대량 데이터 처리 시 성능 향상

### 트랜잭션 관리
- 모든 테이블 적재를 하나의 트랜잭션으로 처리
- 오류 발생 시 자동 롤백

## 📞 문의 및 지원
프로젝트 관련 문의사항이나 버그 리포트는 이슈로 등록해주세요.

---

**마지막 업데이트**: 2025-10-28

