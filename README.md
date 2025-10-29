# PDF to Database - 정부/공공기관 표준 데이터 정규화 시스템

## 🎯 프로젝트 개요
PDF 문서에서 데이터를 추출하여 정부/공공기관 표준에 맞춰 정규화하고 MySQL 데이터베이스에 적재하는 완전한 End-to-End 시스템입니다.

### 📌 v1.1.0 업데이트
- ✅ **PDF → JSON 변환 모듈 추가** 
- ✅ **완전한 파이프라인 구성** (PDF → JSON → 정규화 → DB)
- ✅ **통합 테스트 시스템**
- ✅ **샘플 데이터 모드 지원** (PDF 없이도 테스트 가능)

## ✨ 주요 특징

### 1. 정부 표준 준수
- **원본 데이터 보존**: 감사 추적을 위한 완전한 원본 데이터 보관
- **정규화 데이터 분리**: 분석 최적화를 위한 별도 정규화 테이블
- **3NF/BCNF 준수**: 데이터 중복 최소화 및 무결성 보장

### 2. 핵심 기능
- **일정 정규화**: "1/4분기~2/4분기" → 개별 분기별 레코드 생성
- **성과 데이터 추출**: 특허, 논문, 인력양성 등 100% 정확도
- **예산 정규화**: 다년도 × 유형별 분리 저장
- **데이터 시각화**: 5종 분석 차트 자동 생성

## 🚀 빠른 시작

### 1. 환경 설정
```bash
# 패키지 설치
pip install -r requirements.txt
```

<<<<<<< HEAD
### 2. MySQL 설정 (선택사항)
=======
### 2. PDF 파일 준비
- PDF 파일을 `input/` 폴더에 넣기
- 또는 기존 JSON 파일을 `output/` 폴더에 넣기

### 3. MySQL 설정
>>>>>>> 32106076abc4cdf8ba59ca0773c34a44fb21a5ca
`config.py` 파일에서 MySQL 비밀번호 설정:
```python
MYSQL_CONFIG = {
    "password": "your_password_here"  # 실제 비밀번호로 변경
}
```

<<<<<<< HEAD
### 3. 실행 방법
=======
### 4. 실행
```bash
# PDF만 추출 (JSON 생성)
python extract_pdf_tables.py

# 전체 파이프라인 실행 (PDF 추출 → 정규화 → DB 적재 → 시각화)
python main_government_standard.py
>>>>>>> 32106076abc4cdf8ba59ca0773c34a44fb21a5ca

#### 완전한 파이프라인 (PDF → DB)
```bash
# PDF 파일 처리
python main_complete_pipeline.py your_document.pdf

# 샘플 데이터로 테스트 (PDF 없이)
python main_complete_pipeline.py

# DB 적재 건너뛰기
python main_complete_pipeline.py --skip-db
```

#### 시스템 검증
```bash
# 전체 시스템 테스트
python test_complete_system.py
```

## 📊 시스템 구조

```
<<<<<<< HEAD
정부 표준 데이터 처리 시스템 (v1.1.0)
│
├── 📄 PDF 처리
│   └── extract_pdf_to_json.py        # PDF → JSON 변환
│
├── 🔄 데이터 정규화
│   └── normalize_government_standard.py  # 정규화 엔진
│
├── 💾 데이터베이스
│   └── load_government_standard_db.py    # DB 적재 모듈
│
├── 🚀 실행 모듈
│   ├── main_complete_pipeline.py     # 완전한 파이프라인
│   ├── main_government_standard.py   # 정규화 중심 실행
│   └── test_government_pipeline.py   # 정규화 테스트
│
└── 🧪 테스트
    └── test_complete_system.py       # 통합 시스템 검증
=======
정부 표준 정규화 시스템
├── extract_pdf_tables.py             # PDF → JSON 추출
├── normalize_government_standard.py  # JSON → CSV 정규화
├── load_government_standard_db.py    # CSV → DB 적재
├── main_government_standard.py       # 통합 파이프라인
└── test_government_pipeline.py       # 테스트 및 검증
>>>>>>> 32106076abc4cdf8ba59ca0773c34a44fb21a5ca
```

## 📈 처리 결과 예시

### 입력 데이터
```json
["특허", "국내출원", "1,001"]
["1/4분기~2/4분기", "뇌지도 구축", "세부내용"]
```

### 출력 데이터
```sql
-- 성과 데이터
INSERT INTO normalized_performances VALUES 
  (1, 1, '특허', '국내출원', 1001, '건');

-- 일정 데이터 (분기별 분리)
INSERT INTO normalized_schedules VALUES 
  (1, 1, 2024, 1, '뇌지도 구축'),  -- 1분기
  (2, 1, 2024, 2, '뇌지도 구축');  -- 2분기
```

## 🔍 데이터 검증
- ✅ 모든 내역사업에 대한 데이터 존재 여부
- ✅ 외래키 참조 무결성
- ✅ 데이터 완전성 100% 달성

## 📁 프로젝트 구조

```
<<<<<<< HEAD
PDF_to_DB/
├── 📄 핵심 모듈
│   ├── extract_pdf_to_json.py         # PDF 추출 (NEW)
│   ├── normalize_government_standard.py   # 정규화 엔진
│   ├── load_government_standard_db.py     # DB 적재
│   └── config.py                          # 설정
│
├── 🚀 실행 파일
│   ├── main_complete_pipeline.py      # 완전한 파이프라인 (NEW)
│   ├── main_government_standard.py    # 정규화 파이프라인
│   └── test_government_pipeline.py    # 정규화 테스트
│
├── 🧪 테스트
│   └── test_complete_system.py        # 시스템 검증 (NEW)
│
├── 📚 문서
│   ├── README.md                      # 이 문서
│   └── GOVERNMENT_STANDARD_README.md  # 기술 상세
│
├── 📁 데이터 폴더
│   ├── input/                         # PDF 입력
│   ├── output/                        # JSON 출력
│   ├── normalized_output_government/  # CSV 출력
│   └── visualization_government/      # 보고서
│
└── 📦 기타
    └── requirements.txt               # 패키지 의존성
=======
/home/user/webapp/
├── config.py                          # 설정 파일
├── extract_pdf_tables.py              # PDF 추출 모듈 🆕
├── normalize_government_standard.py   # 정규화 엔진
├── load_government_standard_db.py     # DB 적재 모듈
├── main_government_standard.py        # 메인 파이프라인
├── test_government_pipeline.py        # 테스트 모듈
├── README.md                          # 이 문서
├── GOVERNMENT_STANDARD_README.md      # 상세 기술 문서
├── requirements.txt                   # 패키지 의존성
├── input/                             # PDF 입력 파일 🆕
├── output/                            # JSON 추출 결과 🆕
├── normalized_output_government/      # 정규화된 CSV 출력
└── visualization_government/          # 시각화 결과
>>>>>>> 32106076abc4cdf8ba59ca0773c34a44fb21a5ca
```

## 🛠️ 기술 스택
- **Python 3.13**
- **MySQL 8.0**
- **PDFPlumber**: PDF 테이블 추출
- **Pandas**: 데이터 처리
- **Matplotlib/Seaborn**: 시각화
- **PyMySQL**: 데이터베이스 연결

## 📝 상세 문서
자세한 기술 사양과 구현 세부사항은 [GOVERNMENT_STANDARD_README.md](GOVERNMENT_STANDARD_README.md)를 참조하세요.

## 📞 문의
프로젝트 관련 문의사항은 GitHub Issues를 통해 등록해주세요.

---
**최종 버전**: 1.0.0  
**마지막 업데이트**: 2025-10-29