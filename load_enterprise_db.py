"""
엔터프라이즈 정부사업 데이터 DB 적재 시스템
트랜잭션 관리 및 데이터 무결성 보장
"""
import csv
import json
import mysql.connector
from mysql.connector import Error
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal
import logging
from contextlib import contextmanager
from config import MYSQL_CONFIG

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnterpriseDBLoader:
    """엔터프라이즈 DB 적재 클래스"""
    
    # 테이블 생성 순서 (외래키 종속성)
    TABLE_ORDER = [
        'sub_projects',
        'project_overviews',
        'project_objectives',
        'performance_master',
        'performance_patents',
        'performance_papers', 
        'performance_technology',
        'performance_hr',
        'performance_achievements',
        'plan_master',
        'plan_budgets',
        'plan_schedules',
        'plan_contents'
    ]
    
    BATCH_SIZE = 100
    
    def __init__(self, csv_dir: str):
        self.csv_dir = Path(csv_dir)
        self.connection = None
        self.cursor = None
        self.stats = {
            'tables_created': 0,
            'records_inserted': 0,
            'errors': []
        }
    
    @contextmanager
    def db_connection(self):
        """데이터베이스 연결 컨텍스트 매니저"""
        try:
            self.connection = mysql.connector.connect(
                host=MYSQL_CONFIG.get('host', 'localhost'),
                user=MYSQL_CONFIG.get('user', 'root'),
                password=MYSQL_CONFIG['password'],
                database=MYSQL_CONFIG.get('database', 'convert_pdf'),
                charset='utf8mb4',
                use_unicode=True,
                autocommit=False,
                connection_timeout=30
            )
            self.cursor = self.connection.cursor(dictionary=True)
            
            # 성능 설정
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            self.cursor.execute("SET AUTOCOMMIT = 0")
            
            yield
            
            # 복원
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.cursor.execute("SET AUTOCOMMIT = 1")
            
        except Error as e:
            logger.error(f"DB 연결 실패: {e}")
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
            for table in reversed(self.TABLE_ORDER):
                self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
            
            # 테이블 생성
            sqls = self._get_create_sqls()
            for sql in sqls:
                self.cursor.execute(sql)
                self.stats['tables_created'] += 1
            
            self.connection.commit()
            logger.info(f"✅ {self.stats['tables_created']}개 테이블 생성 완료")
            
        except Error as e:
            self.connection.rollback()
            logger.error(f"테이블 생성 실패: {e}")
            raise
    
    def _get_create_sqls(self) -> List[str]:
        """테이블 생성 SQL"""
        return [
            # 내역사업 마스터
            """
            CREATE TABLE sub_projects (
                id INT PRIMARY KEY,
                project_code VARCHAR(100) UNIQUE,
                department_name VARCHAR(200),
                main_project_name VARCHAR(500),
                sub_project_name VARCHAR(500) NOT NULL,
                document_year INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_name (sub_project_name),
                INDEX idx_year (document_year),
                INDEX idx_main (main_project_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 사업개요
            """
            CREATE TABLE project_overviews (
                id INT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                managing_organization VARCHAR(500),
                supervising_organization VARCHAR(500),
                project_type VARCHAR(100),
                research_period VARCHAR(200),
                total_research_budget VARCHAR(200),
                representative_field TEXT,
                objectives TEXT,
                content TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
                INDEX idx_sub_project (sub_project_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 사업목표/내용
            """
            CREATE TABLE project_objectives (
                id INT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                objective_type ENUM('목표', '내용'),
                content TEXT,
                parsed_json JSON,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
                INDEX idx_type (sub_project_id, objective_type),
                FULLTEXT INDEX ft_content (content)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 실적 마스터
            """
            CREATE TABLE performance_master (
                id INT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                performance_year INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
                UNIQUE KEY uk_project_year (sub_project_id, performance_year),
                INDEX idx_year (performance_year)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 특허 성과
            """
            CREATE TABLE performance_patents (
                id INT PRIMARY KEY,
                performance_id INT NOT NULL,
                domestic_application INT DEFAULT 0,
                domestic_registration INT DEFAULT 0,
                foreign_application INT DEFAULT 0,
                foreign_registration INT DEFAULT 0,
                FOREIGN KEY (performance_id) REFERENCES performance_master(id) ON DELETE CASCADE,
                INDEX idx_performance (performance_id),
                CHECK (domestic_registration <= domestic_application),
                CHECK (foreign_registration <= foreign_application)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 논문 성과
            """
            CREATE TABLE performance_papers (
                id INT PRIMARY KEY,
                performance_id INT NOT NULL,
                scie_total INT DEFAULT 0,
                scie_if10_above INT DEFAULT 0,
                scie_if20_above INT DEFAULT 0,
                non_scie INT DEFAULT 0,
                total_papers INT DEFAULT 0,
                FOREIGN KEY (performance_id) REFERENCES performance_master(id) ON DELETE CASCADE,
                INDEX idx_performance (performance_id),
                CHECK (scie_if20_above <= scie_if10_above),
                CHECK (scie_if10_above <= scie_total)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 기술이전
            """
            CREATE TABLE performance_technology (
                id INT PRIMARY KEY,
                performance_id INT NOT NULL,
                tech_transfer_count INT DEFAULT 0,
                tech_transfer_amount DECIMAL(20,2) DEFAULT 0,
                commercialization_count INT DEFAULT 0,
                commercialization_amount DECIMAL(20,2) DEFAULT 0,
                FOREIGN KEY (performance_id) REFERENCES performance_master(id) ON DELETE CASCADE,
                INDEX idx_performance (performance_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 인력양성
            """
            CREATE TABLE performance_hr (
                id INT PRIMARY KEY,
                performance_id INT NOT NULL,
                phd_graduates INT DEFAULT 0,
                master_graduates INT DEFAULT 0,
                short_term_training INT DEFAULT 0,
                long_term_training INT DEFAULT 0,
                total_participants INT DEFAULT 0,
                FOREIGN KEY (performance_id) REFERENCES performance_master(id) ON DELETE CASCADE,
                INDEX idx_performance (performance_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 대표성과
            """
            CREATE TABLE performance_achievements (
                id INT PRIMARY KEY AUTO_INCREMENT,
                performance_id INT NOT NULL,
                achievement_title TEXT,
                journal_info VARCHAR(500),
                impact_factor DECIMAL(10,2),
                publication_date VARCHAR(100),
                description TEXT,
                FOREIGN KEY (performance_id) REFERENCES performance_master(id) ON DELETE CASCADE,
                INDEX idx_performance (performance_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 계획 마스터
            """
            CREATE TABLE plan_master (
                id INT PRIMARY KEY,
                sub_project_id INT NOT NULL,
                plan_year INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) ON DELETE CASCADE,
                UNIQUE KEY uk_project_year (sub_project_id, plan_year),
                INDEX idx_year (plan_year)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 예산 계획
            """
            CREATE TABLE plan_budgets (
                id INT PRIMARY KEY,
                plan_id INT NOT NULL,
                budget_year INT NOT NULL,
                budget_type VARCHAR(50),
                planned_amount DECIMAL(20,2),
                actual_amount DECIMAL(20,2),
                FOREIGN KEY (plan_id) REFERENCES plan_master(id) ON DELETE CASCADE,
                INDEX idx_year_type (budget_year, budget_type),
                INDEX idx_plan (plan_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 추진일정
            """
            CREATE TABLE plan_schedules (
                id INT PRIMARY KEY,
                plan_id INT NOT NULL,
                schedule_type ENUM('분기', '월', '연중'),
                schedule_period VARCHAR(50),
                task_category VARCHAR(200),
                task_description TEXT,
                start_date DATE,
                end_date DATE,
                status VARCHAR(50),
                FOREIGN KEY (plan_id) REFERENCES plan_master(id) ON DELETE CASCADE,
                INDEX idx_period (schedule_period),
                INDEX idx_dates (start_date, end_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # 추진내용
            """
            CREATE TABLE plan_contents (
                id INT PRIMARY KEY AUTO_INCREMENT,
                plan_id INT NOT NULL,
                content_order INT DEFAULT 0,
                content_title VARCHAR(500),
                content_description TEXT,
                budget_allocation DECIMAL(20,2),
                FOREIGN KEY (plan_id) REFERENCES plan_master(id) ON DELETE CASCADE,
                INDEX idx_order (content_order)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ]
    
    def load_csv_data(self):
        """CSV 데이터 적재"""
        try:
            for table_name in self.TABLE_ORDER:
                csv_path = self.csv_dir / f"{table_name}.csv"
                
                if not csv_path.exists():
                    logger.debug(f"CSV 파일 없음: {csv_path}")
                    continue
                
                self._load_single_csv(table_name, csv_path)
            
            self.connection.commit()
            logger.info(f"✅ 총 {self.stats['records_inserted']}개 레코드 적재 완료")
            
        except Error as e:
            self.connection.rollback()
            logger.error(f"데이터 적재 실패: {e}")
            raise
    
    def _load_single_csv(self, table_name: str, csv_path: Path):
        """단일 CSV 적재"""
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                return
            
            # 배치 처리
            for i in range(0, len(rows), self.BATCH_SIZE):
                batch = rows[i:i + self.BATCH_SIZE]
                self._insert_batch(table_name, batch)
            
            self.stats['records_inserted'] += len(rows)
            logger.info(f"  {table_name}: {len(rows)}개 레코드 적재")
            
        except Exception as e:
            logger.error(f"CSV 적재 실패 ({table_name}): {e}")
            self.stats['errors'].append(f"{table_name}: {str(e)}")
            raise
    
    def _insert_batch(self, table_name: str, batch: List[Dict]):
        """배치 INSERT"""
        if not batch:
            return
        
        columns = list(batch[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f"`{col}`" for col in columns])
        
        sql = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
        """
        
        data = []
        for row in batch:
            values = []
            for col in columns:
                value = row[col]
                
                # NULL 처리
                if value == '' or value == 'None' or value == 'null':
                    value = None
                # JSON 처리
                elif col == 'parsed_json' and value:
                    try:
                        json.loads(value)  # 검증
                    except:
                        value = None
                # 날짜 처리
                elif col in ['start_date', 'end_date'] and value:
                    try:
                        datetime.strptime(value, '%Y-%m-%d')
                    except:
                        value = None
                # 숫자 처리
                elif any(col.endswith(suffix) for suffix in 
                        ['_count', '_amount', '_year', '_id', 'application', 
                         'registration', 'graduates', 'participants', 'total',
                         'above', 'scie', 'training']):
                    try:
                        value = int(value) if value else 0
                    except:
                        try:
                            value = float(value) if value else 0
                        except:
                            value = 0
                
                values.append(value)
            
            data.append(tuple(values))
        
        self.cursor.executemany(sql, data)
    
    def verify_data(self):
        """데이터 검증"""
        logger.info("\n📊 데이터 검증...")
        
        queries = [
            ("내역사업 목록", """
                SELECT sub_project_name, project_code, document_year
                FROM sub_projects
                ORDER BY id
            """),
            
            ("실적 데이터 요약", """
                SELECT 
                    sp.sub_project_name,
                    pm.performance_year,
                    pp.domestic_application AS 특허_국내출원,
                    pp.domestic_registration AS 특허_국내등록,
                    pap.scie_total AS 논문_SCIE,
                    pap.scie_if10_above AS 논문_IF10이상
                FROM sub_projects sp
                LEFT JOIN performance_master pm ON sp.id = pm.sub_project_id
                LEFT JOIN performance_patents pp ON pm.id = pp.performance_id
                LEFT JOIN performance_papers pap ON pm.id = pap.performance_id
                WHERE pp.domestic_application > 0 OR pap.scie_total > 0
            """),
            
            ("예산 데이터 요약", """
                SELECT 
                    sp.sub_project_name,
                    pb.budget_year,
                    pb.budget_type,
                    pb.planned_amount,
                    pb.actual_amount
                FROM sub_projects sp
                JOIN plan_master pm ON sp.id = pm.sub_project_id
                JOIN plan_budgets pb ON pm.id = pb.plan_id
                ORDER BY sp.id, pb.budget_year
                LIMIT 10
            """),
            
            ("데이터 통계", """
                SELECT 
                    '내역사업' AS 구분, COUNT(*) AS 건수 FROM sub_projects
                UNION ALL
                SELECT '사업개요', COUNT(*) FROM project_overviews
                UNION ALL
                SELECT '실적(특허)', COUNT(*) FROM performance_patents
                UNION ALL
                SELECT '실적(논문)', COUNT(*) FROM performance_papers
                UNION ALL
                SELECT '예산계획', COUNT(*) FROM plan_budgets
                UNION ALL
                SELECT '추진일정', COUNT(*) FROM plan_schedules
            """)
        ]
        
        for title, query in queries:
            try:
                self.cursor.execute(query)
                results = self.cursor.fetchall()
                
                print(f"\n{title}:")
                for row in results[:10]:
                    print(f"  {row}")
                    
            except Error as e:
                logger.error(f"검증 실패 ({title}): {e}")
    
    def print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("📊 DB 적재 통계")
        print("="*80)
        print(f"✅ 생성된 테이블: {self.stats['tables_created']}개")
        print(f"✅ 적재된 레코드: {self.stats['records_inserted']:,}개")
        
        if self.stats['errors']:
            print(f"\n⚠️ 오류:")
            for error in self.stats['errors']:
                print(f"  - {error}")
        
        print("="*80)
    
    def load(self):
        """전체 적재 프로세스"""
        try:
            with self.db_connection():
                logger.info("🚀 엔터프라이즈 DB 적재 시작")
                
                # 1. 테이블 생성
                logger.info("\n1. 테이블 생성...")
                self.create_tables()
                
                # 2. 데이터 적재
                logger.info("\n2. CSV 데이터 적재...")
                self.load_csv_data()
                
                # 3. 데이터 검증
                logger.info("\n3. 데이터 검증...")
                self.verify_data()
                
                # 4. 통계 출력
                self.print_statistics()
                
                logger.info("\n✅ DB 적재 완료!")
                return True
                
        except Exception as e:
            logger.error(f"❌ DB 적재 실패: {e}")
            self.print_statistics()
            return False


def load_enterprise_db(csv_dir: str) -> bool:
    """엔터프라이즈 DB 적재 실행"""
    loader = EnterpriseDBLoader(csv_dir)
    return loader.load()


if __name__ == "__main__":
    # 테스트
    csv_folder = "normalized_enterprise"
    
    if Path(csv_folder).exists():
        success = load_enterprise_db(csv_folder)
        print("✅ 성공!" if success else "❌ 실패!")
    else:
        print(f"❌ CSV 폴더를 찾을 수 없습니다: {csv_folder}")