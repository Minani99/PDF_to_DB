"""
정부/공공기관 표준 데이터베이스 적재 모듈
원본 데이터 + 정규화 데이터 분리 저장
"""
import pymysql
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any
import logging
from datetime import datetime
from decimal import Decimal

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GovernmentStandardDBLoader:
    """정부 표준 DB 적재 클래스"""
    
    def __init__(self, db_config: Dict[str, Any], csv_dir: str):
        """
        Args:
            db_config: MySQL 연결 설정
            csv_dir: 정규화된 CSV 파일 디렉토리
        """
        self.db_config = db_config
        self.csv_dir = Path(csv_dir)
        self.connection = None
        self.cursor = None
        
        # 테이블 생성 순서 (외래키 의존성 고려)
        self.tables = [
            'sub_projects',
            'raw_data',
            'normalized_schedules',
            'normalized_performances',
            'normalized_budgets',
            'normalized_overviews',
            'key_achievements',
            'plan_details',
            'data_statistics'
        ]
        
        # 적재 통계
        self.load_stats = {
            'tables_created': 0,
            'total_records': 0,
            'records_by_table': {},
            'errors': []
        }
    
    def connect(self):
        """데이터베이스 연결"""
        try:
            db_name = self.db_config.get('database', 'government_standard')

            # 먼저 데이터베이스 생성 (없으면)
            logger.info(f"🔌 데이터베이스 '{db_name}' 확인 중...")
            temp_conn = pymysql.connect(
                host=self.db_config.get('host', 'localhost'),
                user=self.db_config.get('user', 'root'),
                password=self.db_config['password'],
                charset='utf8mb4'
            )

            with temp_conn.cursor() as cursor:
                cursor.execute(f"""
                    CREATE DATABASE IF NOT EXISTS {db_name}
                    CHARACTER SET utf8mb4 
                    COLLATE utf8mb4_unicode_ci
                """)
                temp_conn.commit()
                logger.info(f"✅ 데이터베이스 '{db_name}' 준비 완료")

            temp_conn.close()

            # 이제 데이터베이스에 연결
            self.connection = pymysql.connect(
                host=self.db_config.get('host', 'localhost'),
                user=self.db_config.get('user', 'root'),
                password=self.db_config['password'],
                database=db_name,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()
            logger.info("✅ 데이터베이스 연결 성공")

        except Exception as e:
            logger.error(f"❌ 데이터베이스 연결 실패: {e}")
            raise

    def _create_database_if_not_exists(self):
        """데이터베이스 생성 (더 이상 사용하지 않음 - connect()에서 처리)"""
        pass

    def drop_existing_tables(self):
        """기존 테이블 삭제"""
        logger.info("🗑️ 기존 테이블 삭제 중...")
        
        # 외래키 제약 임시 해제
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # 역순으로 삭제
        for table in reversed(self.tables):
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"  ✓ {table} 테이블 삭제")
            except Exception as e:
                logger.warning(f"  ! {table} 테이블 삭제 실패: {e}")
        
        # 외래키 제약 재설정
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        self.connection.commit()
    
    def create_tables(self):
        """테이블 생성"""
        logger.info("📊 테이블 생성 중...")
        
        # 1. 내역사업 마스터
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sub_projects (
                id INT PRIMARY KEY,
                project_code VARCHAR(100),
                department_name VARCHAR(200),
                main_project_name VARCHAR(500),
                sub_project_name VARCHAR(500),
                document_year INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_project_code (project_code),
                INDEX idx_sub_project_name (sub_project_name),
                INDEX idx_document_year (document_year)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        
        # 2. 원본 데이터 (감사 추적용)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                id INT PRIMARY KEY,
                sub_project_id INT,
                data_type VARCHAR(50),
                data_year INT,
                raw_content JSON,
                page_number INT,
                table_index INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_data_type (data_type),
                INDEX idx_data_year (data_year),
                INDEX idx_page (page_number)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        
        # 3. 정규화된 일정
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS normalized_schedules (
                id INT PRIMARY KEY,
                sub_project_id INT,
                raw_data_id INT,
                year INT,
                quarter INT,
                month_start INT,
                month_end INT,
                start_date DATE,
                end_date DATE,
                task_category VARCHAR(200),
                task_description TEXT,
                original_period VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) 
                    ON DELETE SET NULL ON UPDATE CASCADE,
                INDEX idx_year_quarter (year, quarter),
                INDEX idx_dates (start_date, end_date),
                INDEX idx_category (task_category)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        
        # 4. 정규화된 성과
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS normalized_performances (
                id INT PRIMARY KEY,
                sub_project_id INT,
                raw_data_id INT,
                performance_year INT,
                indicator_category VARCHAR(100),
                indicator_type VARCHAR(200),
                value INT,
                unit VARCHAR(50),
                original_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) 
                    ON DELETE SET NULL ON UPDATE CASCADE,
                INDEX idx_year (performance_year),
                INDEX idx_category (indicator_category),
                INDEX idx_type (indicator_type)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        
        # 5. 정규화된 예산
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS normalized_budgets (
                id INT PRIMARY KEY,
                sub_project_id INT,
                raw_data_id INT,
                budget_year INT,
                budget_category VARCHAR(100),
                budget_type VARCHAR(100),
                amount DECIMAL(15, 2),
                currency VARCHAR(10) DEFAULT 'KRW',
                is_actual BOOLEAN DEFAULT FALSE,
                original_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) 
                    ON DELETE SET NULL ON UPDATE CASCADE,
                INDEX idx_year (budget_year),
                INDEX idx_category (budget_category),
                INDEX idx_type (budget_type)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        
        # 6. 정규화된 사업개요
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS normalized_overviews (
                id INT PRIMARY KEY,
                sub_project_id INT,
                raw_data_id INT,
                overview_type VARCHAR(100),
                main_project TEXT,
                sub_project TEXT,
                field TEXT,
                project_type TEXT,
                objective TEXT,
                content TEXT,
                managing_dept TEXT,
                managing_org TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) 
                    ON DELETE SET NULL ON UPDATE CASCADE,
                INDEX idx_type (overview_type)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        
        # 7. 대표성과 (key_achievements)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS key_achievements (
                id INT PRIMARY KEY,
                sub_project_id INT,
                achievement_year INT,
                achievement_order INT,
                description TEXT,
                page_number INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_year (achievement_year)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)

        # 8. 주요 추진계획 내용 (plan_details)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS plan_details (
                id INT PRIMARY KEY,
                sub_project_id INT,
                plan_year INT,
                plan_order INT,
                description TEXT,
                page_number INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_year (plan_year)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)

        # 9. 데이터 통계 (검증용)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_statistics (
                id INT PRIMARY KEY AUTO_INCREMENT,
                sub_project_id INT,
                table_name VARCHAR(100),
                record_count INT,
                data_year INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sub_project_id) REFERENCES sub_projects(id) 
                    ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_table (table_name),
                INDEX idx_year (data_year)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        
        self.connection.commit()
        logger.info("✅ 모든 테이블 생성 완료")
        self.load_stats['tables_created'] = len(self.tables)

    def load_csv_to_table(self, table_name: str) -> int:
        """CSV 파일을 테이블로 적재"""
        csv_file = self.csv_dir / f"{table_name}.csv"
        
        if not csv_file.exists():
            logger.warning(f"⚠️ {csv_file} 파일이 없습니다.")
            return 0
        
        try:
            # CSV 읽기
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            
            if df.empty:
                logger.warning(f"⚠️ {table_name}에 데이터가 없습니다.")
                return 0

            # NULL 값 처리 (NaN을 None으로 변환)
            df = df.replace({pd.NA: None, pd.NaT: None})
            df = df.where(pd.notna(df), None)
            
            # 빈 문자열을 None으로 처리
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: None if x == '' or (isinstance(x, str) and x.strip() == '') else x)

            # 날짜 컬럼 처리
            date_columns = ['start_date', 'end_date', 'created_at']
            for col in date_columns:
                if col in df.columns:
                    # 날짜 형식 파싱
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # NaT는 None으로 변환
                    df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

            # JSON 컬럼 처리
            if 'raw_content' in df.columns:
                df['raw_content'] = df['raw_content'].apply(
                    lambda x: json.dumps(json.loads(x), ensure_ascii=False) if x and pd.notna(x) else None
                )
            
            # 데이터 적재
            records = df.to_dict('records')
            
            if records:
                # 컬럼명 가져오기
                columns = list(records[0].keys())
                
                # INSERT 쿼리 생성
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join([f"`{col}`" for col in columns])
                
                query = f"""
                    INSERT INTO {table_name} ({columns_str})
                    VALUES ({placeholders})
                """
                
                # 배치로 삽입
                batch_size = 100
                total_inserted = 0
                
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    values = []
                    
                    for record in batch:
                        # 각 레코드를 튜플로 변환
                        row_values = []
                        for col in columns:
                            val = record.get(col)
                            # NaN 체크 및 처리
                            if pd.isna(val):
                                row_values.append(None)
                            elif isinstance(val, (int, float)):
                                # float NaN 체크
                                if val != val:  # NaN은 자기 자신과 같지 않음
                                    row_values.append(None)
                                else:
                                    row_values.append(val)
                            else:
                                row_values.append(val)
                        values.append(tuple(row_values))
                    
                    self.cursor.executemany(query, values)
                    total_inserted += len(batch)
                    
                    if total_inserted % 1000 == 0:
                        logger.info(f"  {table_name}: {total_inserted}건 적재 중...")
                
                self.connection.commit()
                logger.info(f"✅ {table_name}: {total_inserted}건 적재 완료")
                
                # 통계 업데이트
                self.load_stats['records_by_table'][table_name] = total_inserted
                self.load_stats['total_records'] += total_inserted
                
                return total_inserted
                
        except Exception as e:
            logger.error(f"❌ {table_name} 적재 실패: {e}")
            self.load_stats['errors'].append(f"{table_name}: {str(e)}")
            self.connection.rollback()
            return 0
    
    def load_all_tables(self):
        """모든 테이블 적재"""
        logger.info("📥 데이터 적재 시작...")
        
        for table_name in self.tables:
            record_count = self.load_csv_to_table(table_name)
            
            # 통계 테이블 업데이트
            if record_count > 0 and table_name != 'data_statistics':
                self._update_statistics(table_name, record_count)
        
        logger.info("✅ 모든 데이터 적재 완료")
        self._print_load_summary()
    
    def _update_statistics(self, table_name: str, record_count: int):
        """통계 테이블 업데이트"""
        try:
            # 각 내역사업별 통계
            query = f"""
                INSERT INTO data_statistics (sub_project_id, table_name, record_count, data_year)
                SELECT sub_project_id, %s, COUNT(*), YEAR(CURRENT_DATE())
                FROM {table_name}
                WHERE sub_project_id IS NOT NULL
                GROUP BY sub_project_id
            """
            
            if 'sub_project_id' in self._get_table_columns(table_name):
                self.cursor.execute(query, (table_name,))
                self.connection.commit()
                
        except Exception as e:
            logger.warning(f"통계 업데이트 실패: {e}")
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """테이블 컬럼 목록 조회"""
        self.cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """, (self.db_config.get('database', 'government_standard'), table_name))
        
        return [row['COLUMN_NAME'] for row in self.cursor.fetchall()]
    
    def verify_data_integrity(self) -> Dict[str, Any]:
        """데이터 무결성 검증"""
        logger.info("🔍 데이터 무결성 검증 중...")
        
        verification = {
            'total_sub_projects': 0,
            'raw_data_count': 0,
            'normalized_counts': {},
            'orphan_records': {},
            'missing_data': []
        }
        
        # 1. 내역사업 수
        self.cursor.execute("SELECT COUNT(*) as cnt FROM sub_projects")
        verification['total_sub_projects'] = self.cursor.fetchone()['cnt']
        
        # 2. 원본 데이터 수
        self.cursor.execute("SELECT COUNT(*) as cnt FROM raw_data")
        verification['raw_data_count'] = self.cursor.fetchone()['cnt']
        
        # 3. 정규화 데이터 수
        normalized_tables = ['normalized_schedules', 'normalized_performances', 
                           'normalized_budgets', 'normalized_overviews']
        
        for table in normalized_tables:
            self.cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            verification['normalized_counts'][table] = self.cursor.fetchone()['cnt']
        
        # 4. 고아 레코드 확인
        for table in normalized_tables:
            self.cursor.execute(f"""
                SELECT COUNT(*) as cnt 
                FROM {table} t
                LEFT JOIN sub_projects s ON t.sub_project_id = s.id
                WHERE s.id IS NULL
            """)
            orphan_count = self.cursor.fetchone()['cnt']
            if orphan_count > 0:
                verification['orphan_records'][table] = orphan_count
        
        # 5. 누락 데이터 확인
        self.cursor.execute("""
            SELECT s.sub_project_name, 
                   COUNT(DISTINCT ns.id) as schedules,
                   COUNT(DISTINCT np.id) as performances,
                   COUNT(DISTINCT nb.id) as budgets
            FROM sub_projects s
            LEFT JOIN normalized_schedules ns ON s.id = ns.sub_project_id
            LEFT JOIN normalized_performances np ON s.id = np.sub_project_id
            LEFT JOIN normalized_budgets nb ON s.id = nb.sub_project_id
            GROUP BY s.id, s.sub_project_name
            HAVING schedules = 0 OR performances = 0 OR budgets = 0
        """)
        
        missing_data = self.cursor.fetchall()
        if missing_data:
            verification['missing_data'] = missing_data
        
        # 검증 결과 출력
        logger.info(f"""
📊 데이터 무결성 검증 결과:
- 총 내역사업: {verification['total_sub_projects']}개
- 원본 데이터: {verification['raw_data_count']}건
- 정규화 데이터:
  • 일정: {verification['normalized_counts'].get('normalized_schedules', 0)}건
  • 성과: {verification['normalized_counts'].get('normalized_performances', 0)}건
  • 예산: {verification['normalized_counts'].get('normalized_budgets', 0)}건
  • 개요: {verification['normalized_counts'].get('normalized_overviews', 0)}건
- 고아 레코드: {len(verification['orphan_records'])}개 테이블
- 누락 데이터 사업: {len(verification['missing_data'])}개
        """)
        
        return verification
    
    def _print_load_summary(self):
        """적재 요약 출력"""
        print("\n" + "="*60)
        print("📊 데이터 적재 요약")
        print("="*60)
        print(f"✅ 생성된 테이블: {self.load_stats['tables_created']}개")
        print(f"✅ 총 적재 레코드: {self.load_stats['total_records']:,}건")
        print("\n테이블별 적재 현황:")
        
        for table, count in self.load_stats['records_by_table'].items():
            print(f"  • {table}: {count:,}건")
        
        if self.load_stats['errors']:
            print(f"\n⚠️ 오류 발생: {len(self.load_stats['errors'])}건")
            for error in self.load_stats['errors']:
                print(f"  - {error}")
        
        print("="*60)
    
    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔌 데이터베이스 연결 종료")


def main():
    """메인 실행"""
    from config import MYSQL_CONFIG
    
    # 정부 표준 DB 설정
    db_config = MYSQL_CONFIG.copy()
    db_config['database'] = 'government_standard'
    
    # CSV 디렉토리
    csv_dir = "normalized_output_government"
    
    # 적재 실행
    loader = GovernmentStandardDBLoader(db_config, csv_dir)
    
    try:
        # 연결
        loader.connect()
        
        # 기존 테이블 삭제
        loader.drop_existing_tables()
        
        # 테이블 생성
        loader.create_tables()
        
        # 데이터 적재
        loader.load_all_tables()
        
        # 데이터 검증
        verification = loader.verify_data_integrity()
        
        return verification
        
    except Exception as e:
        logger.error(f"❌ 적재 실패: {e}")
        raise
        
    finally:
        loader.close()


if __name__ == "__main__":
    verification_result = main()
    print(f"\n✅ 정부 표준 데이터베이스 적재 완료!")
    print(f"검증 결과: {json.dumps(verification_result, ensure_ascii=False, indent=2)}")