# 엔터프라이즈 정부사업 데이터 관리 시스템 설계

## 1. 핵심 데이터 구조

### 1.1 계층 구조
```
부처 (Department)
  └─ 세부사업 (Main_Project)
      └─ 내역사업 (Sub_Project) ← 메인 엔티티
          ├─ 사업개요 (Project_Overview)
          ├─ 추진실적 (Project_Performance) - 연도별
          └─ 추진계획 (Project_Plan) - 연도별
```

### 1.2 연도 관리 전략
- **동적 연도 처리**: 파일마다 다른 연도 자동 감지
- **실적 연도**: 보통 작년 (문서 연도 - 1)
- **계획 연도**: 보통 현재년도 (문서 연도)
- **다년도 지원**: 2021, 2022, 2023 실적 + 2024 계획

## 2. 테이블 설계 (업계 표준)

### 2.1 마스터 테이블
```sql
-- 내역사업 마스터
CREATE TABLE sub_projects (
    id INT PRIMARY KEY AUTO_INCREMENT,
    project_code VARCHAR(100) UNIQUE,  -- 사업코드 (자동생성)
    department_name VARCHAR(200),       -- 부처명
    main_project_name VARCHAR(500),     -- 세부사업명
    sub_project_name VARCHAR(500),      -- 내역사업명
    document_year INT,                  -- 문서 기준년도
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (sub_project_name),
    INDEX idx_year (document_year)
);
```

### 2.2 사업개요 (Overview)
```sql
-- 사업개요 기본정보
CREATE TABLE project_overviews (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    managing_organization VARCHAR(500),    -- 주관기관
    supervising_organization VARCHAR(500),  -- 관리기관
    project_type VARCHAR(100),            -- 사업성격
    research_period VARCHAR(200),          -- 연구기간
    total_research_budget VARCHAR(200),    -- 총 연구비
    representative_field TEXT,             -- 대표분야 및 비중
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id),
    INDEX idx_sub_project (sub_project_id)
);

-- 사업목표/내용 (긴 텍스트)
CREATE TABLE project_objectives (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    objective_type ENUM('목표', '내용'),
    content TEXT,                         -- 전체 텍스트
    parsed_json JSON,                      -- 구조화된 JSON (선택적)
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id),
    INDEX idx_type (sub_project_id, objective_type)
);
```

### 2.3 추진실적 (Performance) - 연도별
```sql
-- 실적 마스터 (연도별)
CREATE TABLE performance_master (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    performance_year INT NOT NULL,         -- 실적 연도
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id),
    UNIQUE KEY uk_project_year (sub_project_id, performance_year),
    INDEX idx_year (performance_year)
);

-- 특허 성과
CREATE TABLE performance_patents (
    id INT PRIMARY KEY AUTO_INCREMENT,
    performance_id INT NOT NULL,
    domestic_application INT DEFAULT 0,    -- 국내출원
    domestic_registration INT DEFAULT 0,   -- 국내등록
    foreign_application INT DEFAULT 0,     -- 국외출원
    foreign_registration INT DEFAULT 0,    -- 국외등록
    FOREIGN KEY (performance_id) REFERENCES performance_master(id)
);

-- 논문 성과
CREATE TABLE performance_papers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    performance_id INT NOT NULL,
    scie_total INT DEFAULT 0,             -- SCIE 합계
    scie_if10_above INT DEFAULT 0,        -- IF 10 이상
    scie_if20_above INT DEFAULT 0,        -- IF 20 이상
    non_scie INT DEFAULT 0,               -- 비SCIE
    total_papers INT DEFAULT 0,           -- 전체 논문수
    FOREIGN KEY (performance_id) REFERENCES performance_master(id)
);

-- 기술이전/사업화
CREATE TABLE performance_technology (
    id INT PRIMARY KEY AUTO_INCREMENT,
    performance_id INT NOT NULL,
    tech_transfer_count INT DEFAULT 0,     -- 기술이전 건수
    tech_transfer_amount DECIMAL(20,2),    -- 기술료 (백만원)
    commercialization_count INT DEFAULT 0,  -- 사업화 건수
    commercialization_amount DECIMAL(20,2), -- 사업화 금액
    FOREIGN KEY (performance_id) REFERENCES performance_master(id)
);

-- 인력양성
CREATE TABLE performance_hr (
    id INT PRIMARY KEY AUTO_INCREMENT,
    performance_id INT NOT NULL,
    phd_graduates INT DEFAULT 0,           -- 박사 배출
    master_graduates INT DEFAULT 0,        -- 석사 배출
    short_term_training INT DEFAULT 0,     -- 단기연수 (3개월 이하)
    long_term_training INT DEFAULT 0,      -- 장기연수 (3개월 초과)
    total_participants INT DEFAULT 0,      -- 연구과제 참여인력
    FOREIGN KEY (performance_id) REFERENCES performance_master(id)
);

-- 대표성과 (텍스트)
CREATE TABLE performance_achievements (
    id INT PRIMARY KEY AUTO_INCREMENT,
    performance_id INT NOT NULL,
    achievement_title TEXT,                -- 성과 제목
    journal_info VARCHAR(500),            -- 저널 정보
    impact_factor DECIMAL(10,2),          -- Impact Factor
    publication_date VARCHAR(100),        -- 발표 시기
    description TEXT,                      -- 상세 설명
    FOREIGN KEY (performance_id) REFERENCES performance_master(id)
);
```

### 2.4 추진계획 (Plan) - 연도별
```sql
-- 계획 마스터
CREATE TABLE plan_master (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    plan_year INT NOT NULL,               -- 계획 연도
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id),
    UNIQUE KEY uk_project_year (sub_project_id, plan_year),
    INDEX idx_year (plan_year)
);

-- 예산 계획 (다년도)
CREATE TABLE plan_budgets (
    id INT PRIMARY KEY AUTO_INCREMENT,
    plan_id INT NOT NULL,
    budget_year INT NOT NULL,             -- 예산 연도
    budget_type VARCHAR(50),              -- 예산 구분 (정부/민간)
    planned_amount DECIMAL(20,2),         -- 계획 금액 (백만원)
    actual_amount DECIMAL(20,2),          -- 실적 금액 (백만원)
    FOREIGN KEY (plan_id) REFERENCES plan_master(id),
    INDEX idx_year_type (budget_year, budget_type)
);

-- 추진일정 (분기별/월별)
CREATE TABLE plan_schedules (
    id INT PRIMARY KEY AUTO_INCREMENT,
    plan_id INT NOT NULL,
    schedule_type ENUM('분기', '월', '연중'),
    schedule_period VARCHAR(50),          -- '1/4분기', '3월', '연중' 등
    task_category VARCHAR(200),           -- 작업 분류
    task_description TEXT,                -- 작업 내용
    start_date DATE,                      -- 시작일 (선택적)
    end_date DATE,                        -- 종료일 (선택적)
    status VARCHAR(50),                   -- 상태
    FOREIGN KEY (plan_id) REFERENCES plan_master(id),
    INDEX idx_period (schedule_period),
    INDEX idx_dates (start_date, end_date)
);

-- 주요 추진내용 (텍스트)
CREATE TABLE plan_contents (
    id INT PRIMARY KEY AUTO_INCREMENT,
    plan_id INT NOT NULL,
    content_order INT DEFAULT 0,          -- 순서
    content_title VARCHAR(500),           -- 내용 제목
    content_description TEXT,             -- 상세 설명
    budget_allocation DECIMAL(20,2),      -- 예산 배분 (선택적)
    FOREIGN KEY (plan_id) REFERENCES plan_master(id),
    INDEX idx_order (content_order)
);
```

## 3. 텍스트 데이터 처리 전략

### 3.1 구조화된 텍스트 (사업내용)
```json
// 예시: 사업내용을 JSON으로 구조화
{
  "programs": [
    {
      "name": "글로벌 리더연구",
      "type": "유형2(글로벌형)",
      "description": "세계적 수준에 도달한 연구자의 심화연구 집중 지원",
      "features": ["글로벌 협력 지향", "혁신적 성과 창출"]
    },
    {
      "name": "중견연구",
      "description": "창의성 높은 개인연구 지원",
      "budget_scale": "중규모"
    }
  ]
}
```

### 3.2 처리 방식
- **원본 보존**: 전체 텍스트를 그대로 저장
- **구조화**: 주요 항목을 JSON으로 파싱하여 별도 저장
- **검색 최적화**: 풀텍스트 인덱스 + 구조화 데이터

## 4. 날짜/일정 데이터 형식

### 4.1 업계 표준 형식
```sql
-- 1. ISO 8601 형식 (권장)
'2024-03-15'           -- 날짜
'2024-03-15 14:30:00'  -- 날짜+시간

-- 2. 분기 표현
'2024Q1'               -- 2024년 1분기
'2024-Q1'              -- 대시 포함
'1/4분기'              -- 한국식 표현 (별도 컬럼)

-- 3. 월 표현
'2024-03'              -- 연월
'202403'               -- 연월 (압축)

-- 4. 기간 표현
start_date: '2024-01-01'
end_date: '2024-03-31'
duration_months: 3     -- 기간 (개월)

-- 5. 상태 관리
status: ENUM('planned', 'in_progress', 'completed', 'delayed')
```

### 4.2 일정 저장 예시
```sql
INSERT INTO plan_schedules VALUES (
    NULL,                    -- id (auto)
    1,                      -- plan_id
    '분기',                 -- schedule_type
    '1/4분기',              -- schedule_period
    '신규과제 선정',        -- task_category
    '신규과제 선정평가 및 협약', -- task_description
    '2024-01-01',           -- start_date
    '2024-03-31',           -- end_date
    'completed'             -- status
);
```

## 5. 숫자 데이터 정확성 보장

### 5.1 데이터 타입 선택
```sql
-- 정수형 (건수, 개수)
INT                     -- -2,147,483,648 ~ 2,147,483,647
BIGINT                  -- 매우 큰 숫자

-- 실수형 (금액, 비율)
DECIMAL(20,2)          -- 금액 (소수점 2자리)
DECIMAL(5,2)           -- 비율 (999.99%)
FLOAT/DOUBLE           -- 과학적 계산 (피하는게 좋음)
```

### 5.2 검증 규칙
```python
# 숫자 추출 시 검증
def validate_number(value, field_type):
    if field_type == 'integer':
        assert isinstance(value, int)
        assert value >= 0  # 음수 불가
    elif field_type == 'decimal':
        assert isinstance(value, (int, float, Decimal))
        assert value >= 0
    elif field_type == 'percentage':
        assert 0 <= value <= 100
    return True
```

## 6. 데이터 무결성 규칙

### 6.1 비즈니스 규칙
```sql
-- 체크 제약
ALTER TABLE plan_budgets 
ADD CONSTRAINT chk_amount CHECK (planned_amount >= 0);

ALTER TABLE performance_patents 
ADD CONSTRAINT chk_patent CHECK (
    domestic_application >= 0 AND
    domestic_registration <= domestic_application
);

-- 트리거 예시
CREATE TRIGGER check_year_consistency
BEFORE INSERT ON plan_budgets
FOR EACH ROW
BEGIN
    IF NEW.budget_year < 2020 OR NEW.budget_year > 2030 THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Invalid budget year';
    END IF;
END;
```

## 7. 실제 구현 예시

### 7.1 내역사업 "차세대바이오" 저장
```sql
-- 1. 내역사업 등록
INSERT INTO sub_projects VALUES (
    NULL, 
    'SUB-2024-001',
    '과학기술정보통신부',
    '바이오·의료기술개발사업',
    '차세대바이오',
    2024
);

-- 2. 사업개요
INSERT INTO project_overviews VALUES (
    NULL,
    1,  -- sub_project_id
    '과학기술정보통신부',
    '한국연구재단',
    '연구개발',
    '계속 (과제별 상이)',
    '해당 없음',
    '생명과학(70%), Red(10%), Green(10%), White(10%)'
);

-- 3. 2023년 실적 - 특허
INSERT INTO performance_patents VALUES (
    NULL, 1, 87, 38, 57, 5
);

-- 4. 2024년 예산
INSERT INTO plan_budgets VALUES 
    (NULL, 1, 2021, '정부', 51300, 51300),
    (NULL, 1, 2022, '정부', 64600, 64600),
    (NULL, 1, 2023, '정부', 75946, 75946),
    (NULL, 1, 2024, '정부', 90880, NULL);
```