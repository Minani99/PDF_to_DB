# 개선된 데이터베이스 스키마 설계

## 1. 엔터프라이즈급 정규화 구조 (3NF + BCNF)

### 핵심 개선 사항
- **완전한 계층 구조**: 부처 → 세부사업 → 내역사업 → 카테고리별 데이터
- **유연한 카테고리 시스템**: 동적 카테고리 관리
- **버전 관리**: 변경 이력 추적
- **메타데이터 관리**: 문서별 추출 정보 관리

## 2. 테이블 구조

### 2.1 마스터 테이블
```sql
-- 1. 부처 (Departments)
CREATE TABLE departments (
    id INT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL,  -- 부처 코드
    name VARCHAR(200) NOT NULL,        -- 부처명
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_code (code),
    INDEX idx_name (name)
);

-- 2. 세부사업 (Main Projects)
CREATE TABLE main_projects (
    id INT PRIMARY KEY AUTO_INCREMENT,
    department_id INT NOT NULL,
    code VARCHAR(100) UNIQUE,           -- 사업 코드
    name VARCHAR(500) NOT NULL,         -- 세부사업명
    fiscal_year INT NOT NULL,           -- 회계연도
    status ENUM('active', 'completed', 'planned') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE CASCADE,
    INDEX idx_dept_year (department_id, fiscal_year),
    INDEX idx_name (name)
);

-- 3. 내역사업 (Sub Projects)
CREATE TABLE sub_projects (
    id INT PRIMARY KEY AUTO_INCREMENT,
    main_project_id INT NOT NULL,
    code VARCHAR(100),                  -- 내역사업 코드
    name VARCHAR(500) NOT NULL,         -- 내역사업명
    project_type VARCHAR(100),          -- 사업성격
    priority INT DEFAULT 0,              -- 우선순위
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (main_project_id) REFERENCES main_projects(id) ON DELETE CASCADE,
    UNIQUE KEY uk_main_sub (main_project_id, code),
    INDEX idx_main_project (main_project_id),
    INDEX idx_name (name)
);
```

### 2.2 카테고리 시스템 (동적 구조)
```sql
-- 4. 카테고리 정의
CREATE TABLE categories (
    id INT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL,   -- 카테고리 코드 (overview, performance, budget)
    name VARCHAR(200) NOT NULL,         -- 카테고리명
    parent_id INT,                      -- 상위 카테고리
    level INT DEFAULT 1,                -- 계층 레벨
    display_order INT DEFAULT 0,        -- 표시 순서
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL,
    INDEX idx_parent (parent_id),
    INDEX idx_level (level)
);

-- 5. 사업별 카테고리 데이터 (EAV 패턴 적용)
CREATE TABLE project_category_data (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    category_id INT NOT NULL,
    attribute_key VARCHAR(100) NOT NULL,    -- 속성 키
    attribute_value TEXT,                    -- 속성 값
    data_type VARCHAR(50),                   -- 데이터 타입
    sequence_order INT DEFAULT 0,            -- 순서
    version INT DEFAULT 1,                   -- 버전
    is_current BOOLEAN DEFAULT TRUE,         -- 현재 버전 여부
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id),
    INDEX idx_project_category (sub_project_id, category_id),
    INDEX idx_key (attribute_key),
    INDEX idx_current (is_current)
);
```

### 2.3 구조화된 데이터 테이블
```sql
-- 6. 사업개요 (정형화된 데이터)
CREATE TABLE project_overviews (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT UNIQUE NOT NULL,
    managing_org VARCHAR(500),          -- 주관기관
    supervising_org VARCHAR(500),       -- 관리기관
    research_period VARCHAR(200),        -- 연구기간
    total_budget DECIMAL(20, 2),        -- 총 연구비
    objectives TEXT,                     -- 사업목표
    content TEXT,                        -- 사업내용
    representative_field VARCHAR(500),   -- 대표분야
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
    FULLTEXT INDEX ft_objectives (objectives),
    FULLTEXT INDEX ft_content (content)
);

-- 7. 예산 정보 (정규화된 구조)
CREATE TABLE budgets (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    fiscal_year INT NOT NULL,
    budget_type VARCHAR(50),            -- 예산 유형 (정부, 민간 등)
    planned_amount DECIMAL(20, 2),      -- 계획 금액
    executed_amount DECIMAL(20, 2),     -- 집행 금액
    execution_rate DECIMAL(5, 2),       -- 집행률
    currency VARCHAR(10) DEFAULT 'KRW',
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
    INDEX idx_project_year (sub_project_id, fiscal_year),
    INDEX idx_year (fiscal_year)
);

-- 8. 성과 지표
CREATE TABLE performance_indicators (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    indicator_type VARCHAR(100),        -- 지표 유형 (특허, 논문 등)
    indicator_name VARCHAR(500),        -- 지표명
    target_value DECIMAL(20, 2),        -- 목표값
    achieved_value DECIMAL(20, 2),      -- 달성값
    achievement_rate DECIMAL(5, 2),     -- 달성률
    unit VARCHAR(50),                   -- 단위
    measurement_year INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
    INDEX idx_project_type (sub_project_id, indicator_type),
    INDEX idx_year (measurement_year)
);

-- 9. 추진 일정
CREATE TABLE project_schedules (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sub_project_id INT NOT NULL,
    phase VARCHAR(100),                 -- 단계 (1/4분기, 2/4분기 등)
    task_name VARCHAR(500),             -- 작업명
    start_date DATE,
    end_date DATE,
    status VARCHAR(50),                 -- 상태
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
    INDEX idx_project_phase (sub_project_id, phase),
    INDEX idx_dates (start_date, end_date)
);
```

### 2.4 메타데이터 및 감사 테이블
```sql
-- 10. 문서 메타데이터
CREATE TABLE document_metadata (
    id INT PRIMARY KEY AUTO_INCREMENT,
    file_name VARCHAR(500) NOT NULL,
    file_path VARCHAR(1000),
    file_size BIGINT,
    page_count INT,
    extraction_date TIMESTAMP,
    processing_status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 11. 페이지별 원본 데이터
CREATE TABLE raw_page_data (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    document_id INT NOT NULL,
    page_number INT NOT NULL,
    raw_text LONGTEXT,                  -- 원본 텍스트
    extracted_tables JSON,               -- 추출된 테이블 (JSON)
    processing_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES document_metadata(id) ON DELETE CASCADE,
    INDEX idx_doc_page (document_id, page_number),
    FULLTEXT INDEX ft_raw_text (raw_text)
);

-- 12. 변경 이력 (감사 로그)
CREATE TABLE audit_logs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    table_name VARCHAR(100) NOT NULL,
    record_id BIGINT NOT NULL,
    action VARCHAR(50) NOT NULL,        -- INSERT, UPDATE, DELETE
    old_value JSON,
    new_value JSON,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_table_record (table_name, record_id),
    INDEX idx_changed_at (changed_at)
);
```

## 3. 주요 개선 사항

### 3.1 정규화 수준
- **3NF (Third Normal Form)**: 모든 속성이 기본키에만 종속
- **BCNF (Boyce-Codd Normal Form)**: 모든 결정자가 후보키

### 3.2 유연성
- **EAV 패턴**: 동적 속성 관리 가능
- **카테고리 시스템**: 새로운 카테고리 추가 용이
- **계층 구조**: 무제한 계층 지원

### 3.3 성능 최적화
- **인덱스 전략**: 자주 조회되는 컬럼에 인덱스
- **풀텍스트 인덱스**: 텍스트 검색 최적화
- **파티셔닝 고려**: 대용량 데이터 처리

### 3.4 데이터 무결성
- **외래키 제약**: 참조 무결성 보장
- **유니크 제약**: 중복 방지
- **CHECK 제약**: 데이터 유효성 검증

## 4. 구현 예제 쿼리

### 4.1 데이터 조회
```sql
-- 부처별 세부사업 조회
SELECT 
    d.name AS 부처명,
    mp.name AS 세부사업명,
    sp.name AS 내역사업명,
    po.objectives AS 사업목표
FROM departments d
JOIN main_projects mp ON d.id = mp.department_id
JOIN sub_projects sp ON mp.id = sp.main_project_id
LEFT JOIN project_overviews po ON sp.id = po.sub_project_id
WHERE d.code = 'MSIT'
  AND mp.fiscal_year = 2024;

-- 예산 집행 현황
SELECT 
    sp.name AS 내역사업명,
    b.fiscal_year AS 연도,
    b.budget_type AS 예산유형,
    b.planned_amount AS 계획금액,
    b.executed_amount AS 집행금액,
    b.execution_rate AS 집행률
FROM sub_projects sp
JOIN budgets b ON sp.id = b.sub_project_id
WHERE b.fiscal_year = 2024
ORDER BY b.execution_rate DESC;
```

### 4.2 데이터 삽입
```sql
-- 트랜잭션으로 데이터 삽입
START TRANSACTION;

-- 부처 등록
INSERT INTO departments (code, name, description)
VALUES ('MSIT', '과학기술정보통신부', '국가 과학기술 정책 총괄');

-- 세부사업 등록
INSERT INTO main_projects (department_id, code, name, fiscal_year)
VALUES (LAST_INSERT_ID(), 'BIO2024-001', '바이오·의료기술개발사업', 2024);

-- 내역사업 등록
INSERT INTO sub_projects (main_project_id, code, name, project_type)
VALUES (LAST_INSERT_ID(), 'SUB2024-001', '차세대바이오', '연구개발');

COMMIT;
```

## 5. 마이그레이션 전략

### 5.1 단계별 마이그레이션
1. **Phase 1**: 새 스키마 생성
2. **Phase 2**: 데이터 변환 및 이관
3. **Phase 3**: 검증 및 테스트
4. **Phase 4**: 전환 및 구 시스템 폐기

### 5.2 데이터 변환 규칙
- NULL 값 처리
- 데이터 타입 변환
- 중복 데이터 제거
- 참조 무결성 확인

## 6. 모니터링 및 유지보수

### 6.1 성능 모니터링
- 쿼리 실행 시간
- 인덱스 사용률
- 테이블 크기 관리

### 6.2 백업 전략
- 일일 증분 백업
- 주간 전체 백업
- 시점 복구 지원