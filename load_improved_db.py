"""
개선된 MySQL 데이터베이스 적재 모듈
엔터프라이즈급 트랜잭션 관리 및 성능 최적화 구현
"""
import csv
import json
import mysql.connector
from mysql.connector import Error
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from contextlib import contextmanager
from config import MYSQL_CONFIG

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ImprovedDBLoader:
    """개선된 데이터베이스 적재 클래스"""
    
    # 테이블 생성 순서 (외래키 종속성 고려)
    TABLE_ORDER = [
        "departments",
        "main_projects", 
        "sub_projects",
        "categories",
        "project_category_data",
        "project_overviews",
        "budgets",
        "performance_indicators",
        "project_schedules",
        "document_metadata",
        "raw_page_data",
        "audit_logs"
    ]
    
    # 배치 크기 설정
    BATCH_SIZE = 100
    
    def __init__(self, csv_dir: str):
        self.csv_dir = Path(csv_dir)
        self.connection = None
        self.cursor = None
        
        # 성능 통계
        self.stats = {
            "tables_created": 0,
            "records_inserted": 0,
            "batch_count": 0,
            "errors": []
        }
    
    @contextmanager
    def db_connection(self):
        """데이터베이스 연결 컨텍스트 매니저"""
        try:
            self.connection = mysql.connector.connect(
                host=MYSQL_CONFIG.get("host", "localhost"),
                user=MYSQL_CONFIG.get("user", "root"),
                password=MYSQL_CONFIG["password"],
                database=MYSQL_CONFIG.get("database", "convert_pdf"),
                charset='utf8mb4',
                use_unicode=True,
                autocommit=False,  # 트랜잭션 수동 관리
                connection_timeout=30
            )
            self.cursor = self.connection.cursor(dictionary=True)
            
            # 성능 최적화 설정
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            self.cursor.execute("SET UNIQUE_CHECKS = 0")
            self.cursor.execute("SET AUTOCOMMIT = 0")
            
            yield
            
            # 설정 복원
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.cursor.execute("SET UNIQUE_CHECKS = 1")
            self.cursor.execute("SET AUTOCOMMIT = 1")
            
        except Error as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            raise
        finally:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
    
    def create_tables(self):
        """테이블 생성"""
        try:
            # 기존 테이블 삭제 (역순)
            for table_name in reversed(self.TABLE_ORDER):
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # 테이블 생성 SQL
            create_sqls = self._get_create_table_sqls()
            
            for sql in create_sqls:
                self.cursor.execute(sql)
                self.stats["tables_created"] += 1
            
            self.connection.commit()
            logger.info(f"✅ {self.stats['tables_created']}개 테이블 생성 완료")
            
        except Error as e:
            self.connection.rollback()
            logger.error(f"테이블 생성 실패: {e}")
            raise
    
    def _get_create_table_sqls(self) -> List[str]:
        """테이블 생성 SQL 반환"""
        return [
            # 부처
            """
            CREATE TABLE departments (
                id INT PRIMARY KEY,
                code VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(200) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_code (code),
                INDEX idx_name (name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 세부사업
            """
            CREATE TABLE main_projects (
                id INT PRIMARY KEY,
                department_id INT NOT NULL,
                code VARCHAR(100) UNIQUE,
                name VARCHAR(500) NOT NULL,
                fiscal_year INT NOT NULL,
                status VARCHAR(20) DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_dept_year (department_id, fiscal_year),
                INDEX idx_name (name),
                INDEX idx_fiscal_year (fiscal_year)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 내역사업
            """
            CREATE TABLE sub_projects (
                id INT PRIMARY KEY,
                main_project_id INT NOT NULL,
                code VARCHAR(100),
                name VARCHAR(500) NOT NULL,
                project_type VARCHAR(100),
                priority INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (main_project_id) REFERENCES main_projects(id) ON DELETE CASCADE ON UPDATE CASCADE,
                UNIQUE KEY uk_main_sub (main_project_id, code),
                INDEX idx_main_project (main_project_id),
                INDEX idx_name (name),
                INDEX idx_type (project_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 카테고리
            """
            CREATE TABLE categories (
                id INT PRIMARY KEY,
                code VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(200) NOT NULL,
                parent_id INT,
                level INT DEFAULT 1,
                display_order INT DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL ON UPDATE CASCADE,
                INDEX idx_parent (parent_id),
                INDEX idx_level (level),
                INDEX idx_code (code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 프로젝트 카테고리 데이터 (EAV)
            """
            CREATE TABLE project_category_data (
                id BIGINT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                category_id INT NOT NULL,
                attribute_key VARCHAR(100) NOT NULL,
                attribute_value TEXT,
                data_type VARCHAR(50),
                sequence_order INT DEFAULT 0,
                version INT DEFAULT 1,
                is_current BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (category_id) REFERENCES categories(id) ON UPDATE CASCADE,
                INDEX idx_project_category (sub_project_id, category_id),
                INDEX idx_key (attribute_key),
                INDEX idx_current (is_current),
                INDEX idx_version (version)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 사업개요
            """
            CREATE TABLE project_overviews (
                id INT PRIMARY KEY,
                sub_project_id INT UNIQUE NOT NULL,
                managing_org VARCHAR(500),
                supervising_org VARCHAR(500),
                research_period VARCHAR(200),
                total_budget DECIMAL(20, 2),
                objectives TEXT,
                content TEXT,
                representative_field VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE ON UPDATE CASCADE,
                FULLTEXT INDEX ft_objectives (objectives),
                FULLTEXT INDEX ft_content (content)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 예산
            """
            CREATE TABLE budgets (
                id BIGINT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                fiscal_year INT NOT NULL,
                budget_type VARCHAR(50),
                planned_amount DECIMAL(20, 2),
                executed_amount DECIMAL(20, 2),
                execution_rate DECIMAL(5, 2),
                currency VARCHAR(10) DEFAULT 'KRW',
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_project_year (sub_project_id, fiscal_year),
                INDEX idx_year (fiscal_year),
                INDEX idx_type (budget_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 성과지표
            """
            CREATE TABLE performance_indicators (
                id BIGINT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                indicator_type VARCHAR(100),
                indicator_name VARCHAR(500),
                target_value DECIMAL(20, 2),
                achieved_value DECIMAL(20, 2),
                achievement_rate DECIMAL(5, 2),
                unit VARCHAR(50),
                measurement_year INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_project_type (sub_project_id, indicator_type),
                INDEX idx_year (measurement_year),
                INDEX idx_type (indicator_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 일정
            """
            CREATE TABLE project_schedules (
                id BIGINT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                phase VARCHAR(100),
                task_name VARCHAR(500),
                start_date DATE,
                end_date DATE,
                status VARCHAR(50),
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_project_phase (sub_project_id, phase),
                INDEX idx_dates (start_date, end_date),
                INDEX idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 문서 메타데이터
            """
            CREATE TABLE document_metadata (
                id INT PRIMARY KEY,
                file_name VARCHAR(500) NOT NULL,
                file_path VARCHAR(1000),
                file_size BIGINT,
                page_count INT,
                extraction_date TIMESTAMP,
                processing_status VARCHAR(50),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_status (processing_status),
                INDEX idx_file_name (file_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 원본 페이지 데이터
            """
            CREATE TABLE raw_page_data (
                id BIGINT PRIMARY KEY,
                document_id INT NOT NULL,
                page_number INT NOT NULL,
                raw_text LONGTEXT,
                extracted_tables JSON,
                processing_notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES document_metadata(id) ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_doc_page (document_id, page_number),
                FULLTEXT INDEX ft_raw_text (raw_text)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 감사 로그
            """
            CREATE TABLE audit_logs (
                id BIGINT PRIMARY KEY,
                table_name VARCHAR(100) NOT NULL,
                record_id BIGINT NOT NULL,
                action VARCHAR(50) NOT NULL,
                old_value JSON,
                new_value JSON,
                changed_by VARCHAR(100),
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_table_record (table_name, record_id),
                INDEX idx_changed_at (changed_at),
                INDEX idx_action (action)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ]
    
    def load_csv_data(self):
        """CSV 데이터 적재"""
        try:
            for table_name in self.TABLE_ORDER:
                csv_path = self.csv_dir / f"{table_name}.csv"
                
                if not csv_path.exists():
                    logger.warning(f"⚠️ CSV 파일 없음: {csv_path}")
                    continue
                
                self._load_single_csv(table_name, csv_path)
            
            self.connection.commit()
            logger.info(f"✅ 총 {self.stats['records_inserted']}개 레코드 적재 완료")
            
        except Error as e:
            self.connection.rollback()
            logger.error(f"데이터 적재 실패: {e}")
            raise
    
    def _load_single_csv(self, table_name: str, csv_path: Path):
        """단일 CSV 파일 적재"""
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                logger.warning(f"⚠️ 빈 CSV 파일: {table_name}")
                return
            
            # 배치 처리
            batch_count = 0
            for i in range(0, len(rows), self.BATCH_SIZE):
                batch = rows[i:i + self.BATCH_SIZE]
                self._insert_batch(table_name, batch)
                batch_count += 1
                self.stats["batch_count"] += 1
                
                # 진행 상황 로깅
                if batch_count % 10 == 0:
                    logger.info(f"  - {table_name}: {i + len(batch)}/{len(rows)} 처리 중...")
            
            self.stats["records_inserted"] += len(rows)
            logger.info(f"✅ {table_name}: {len(rows)}개 레코드 적재 완료")
            
        except Exception as e:
            logger.error(f"CSV 적재 실패 ({table_name}): {e}")
            self.stats["errors"].append(f"{table_name}: {str(e)}")
            raise
    
    def _insert_batch(self, table_name: str, batch: List[Dict[str, Any]]):
        """배치 INSERT 실행"""
        if not batch:
            return
        
        # 컬럼명과 플레이스홀더 생성
        columns = list(batch[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f"`{col}`" for col in columns])
        
        # INSERT SQL
        sql = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
        """
        
        # 데이터 준비
        data = []
        for row in batch:
            values = []
            for col in columns:
                value = row[col]
                
                # NULL 처리
                if value == '' or value == 'None':
                    value = None
                # Boolean 처리
                elif col in ['is_active', 'is_current'] and value is not None:
                    value = value.lower() in ['true', '1', 'yes']
                # 숫자 처리
                elif col in ['fiscal_year', 'measurement_year', 'page_number', 
                           'level', 'display_order', 'priority', 'version']:
                    value = int(value) if value else None
                elif col in ['total_budget', 'planned_amount', 'executed_amount',
                           'target_value', 'achieved_value', 'execution_rate', 
                           'achievement_rate']:
                    value = float(value) if value else None
                # 날짜 처리
                elif col in ['start_date', 'end_date']:
                    if value and value != 'None':
                        try:
                            # YYYY-MM-DD 형식 확인
                            datetime.strptime(value, '%Y-%m-%d')
                        except:
                            value = None
                # JSON 처리
                elif col in ['old_value', 'new_value', 'extracted_tables']:
                    if value and value != 'None':
                        try:
                            # 이미 JSON 문자열인지 확인
                            json.loads(value)
                        except:
                            value = json.dumps(value, ensure_ascii=False)
                    else:
                        value = None
                
                values.append(value)
            
            data.append(tuple(values))
        
        # 배치 실행
        self.cursor.executemany(sql, data)
    
    def verify_data(self):
        """데이터 검증"""
        logger.info("\n데이터 검증 시작...")
        
        verification_queries = [
            ("부처별 세부사업 수", """
                SELECT d.name AS 부처명, COUNT(mp.id) AS 세부사업수
                FROM departments d
                LEFT JOIN main_projects mp ON d.id = mp.department_id
                GROUP BY d.id, d.name
            """),
            
            ("세부사업별 내역사업 수", """
                SELECT mp.name AS 세부사업명, COUNT(sp.id) AS 내역사업수
                FROM main_projects mp
                LEFT JOIN sub_projects sp ON mp.id = sp.main_project_id
                GROUP BY mp.id, mp.name
                LIMIT 5
            """),
            
            ("카테고리별 데이터 수", """
                SELECT c.name AS 카테고리명, COUNT(pcd.id) AS 데이터수
                FROM categories c
                LEFT JOIN project_category_data pcd ON c.id = pcd.category_id
                GROUP BY c.id, c.name
            """),
            
            ("연도별 예산 합계", """
                SELECT fiscal_year AS 연도, 
                       COUNT(*) AS 예산항목수,
                       SUM(planned_amount) AS 총예산
                FROM budgets
                GROUP BY fiscal_year
                ORDER BY fiscal_year
            """)
        ]
        
        for title, query in verification_queries:
            try:
                self.cursor.execute(query)
                results = self.cursor.fetchall()
                
                logger.info(f"\n📊 {title}:")
                for row in results[:10]:  # 최대 10개만 표시
                    logger.info(f"  {row}")
                    
            except Error as e:
                logger.error(f"검증 쿼리 실패 ({title}): {e}")
    
    def print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("데이터베이스 적재 통계")
        print("="*80)
        print(f"✅ 생성된 테이블: {self.stats['tables_created']}개")
        print(f"✅ 적재된 레코드: {self.stats['records_inserted']:,}개")
        print(f"✅ 실행된 배치: {self.stats['batch_count']}개")
        
        if self.stats["errors"]:
            print(f"\n⚠️ 오류 발생:")
            for error in self.stats["errors"]:
                print(f"  - {error}")
        
        print("="*80 + "\n")
    
    def load(self):
        """전체 적재 프로세스 실행"""
        try:
            with self.db_connection():
                logger.info("🚀 개선된 DB 적재 시작")
                
                # 1. 테이블 생성
                logger.info("\n1. 테이블 생성 중...")
                self.create_tables()
                
                # 2. 데이터 적재
                logger.info("\n2. CSV 데이터 적재 중...")
                self.load_csv_data()
                
                # 3. 데이터 검증
                logger.info("\n3. 데이터 검증 중...")
                self.verify_data()
                
                # 4. 통계 출력
                self.print_statistics()
                
                logger.info("✅ DB 적재 완료!")
                return True
                
        except Exception as e:
            logger.error(f"❌ DB 적재 실패: {e}")
            self.print_statistics()
            return False


def load_to_mysql_improved(csv_dir: str) -> bool:
    """개선된 MySQL 적재 실행 함수"""
    loader = ImprovedDBLoader(csv_dir)
    return loader.load()


if __name__ == "__main__":
    # 테스트 실행
    csv_folder = "normalized_output_improved"
    
    if Path(csv_folder).exists():
        success = load_to_mysql_improved(csv_folder)
        if success:
            print("✅ DB 적재 성공!")
        else:
            print("❌ DB 적재 실패!")
    else:
        print(f"❌ CSV 폴더를 찾을 수 없습니다: {csv_folder}")