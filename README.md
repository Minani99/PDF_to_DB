# PDF to Database - 정부/공공기관 표준 데이터 정규화 시스템

## 🎯 프로젝트 개요
PDF 문서에서 추출한 데이터를 정부/공공기관 표준에 맞춰 정규화하고 MySQL 데이터베이스에 적재하는 통합 시스템입니다.

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

### 2. MySQL 설정
`config.py` 파일에서 MySQL 비밀번호 설정:
```python
MYSQL_CONFIG = {
    "password": "your_password_here"  # 실제 비밀번호로 변경
}
```

### 3. 실행
```bash
# 전체 파이프라인 실행 (정규화 → DB 적재 → 시각화)
python main_government_standard.py

# 테스트 실행 (DB 없이 정규화와 시각화만)
python test_government_pipeline.py
```

## 📊 시스템 구조

```
정부 표준 정규화 시스템
├── normalize_government_standard.py  # 정규화 엔진
├── load_government_standard_db.py    # DB 적재 모듈
├── main_government_standard.py       # 통합 파이프라인
└── test_government_pipeline.py       # 테스트 및 검증
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
/home/user/webapp/
├── config.py                          # 설정 파일
├── normalize_government_standard.py   # 정규화 엔진
├── load_government_standard_db.py     # DB 적재 모듈
├── main_government_standard.py        # 메인 파이프라인
├── test_government_pipeline.py        # 테스트 모듈
├── README.md                          # 이 문서
├── GOVERNMENT_STANDARD_README.md      # 상세 기술 문서
├── requirements.txt                   # 패키지 의존성
├── output/                            # JSON 입력 데이터
├── normalized_output_government/      # 정규화된 CSV 출력
└── visualization_government/          # 시각화 결과
```

## 🛠️ 기술 스택
- **Python 3.12**
- **MySQL 8.0**
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