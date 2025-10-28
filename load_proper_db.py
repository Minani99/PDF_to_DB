"""
MySQL 데이터베이스 적재 모듈
정규화된 CSV 데이터를 MySQL DB에 적재하고 Foreign Key 관계 설정
"""
import pymysql
import csv
from pathlib import Path
from typing import Dict, List, Any
from config import MYSQL_CONFIG, TABLE_ORDER, CSV_TABLE_MAPPING


class MySQLLoader:
    """MySQL DB에 정규화된 데이터를 적재하는 클래스"""

    def __init__(self, csv_dir: str):
        """
        Args:
            csv_dir: CSV 파일이 있는 디렉토리 경로
        """
        self.csv_dir = Path(csv_dir)
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """MySQL DB 연결"""
        try:
            self.connection = pymysql.connect(
                host=MYSQL_CONFIG['host'],
                user=MYSQL_CONFIG['user'],
                password=MYSQL_CONFIG['password'],
                database=MYSQL_CONFIG['database'],
                port=MYSQL_CONFIG['port'],
                charset=MYSQL_CONFIG['charset'],
                autocommit=False
            )
            self.cursor = self.connection.cursor()
            print(f"✅ MySQL 연결 성공: {MYSQL_CONFIG['database']}")
            return True
        except Exception as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def disconnect(self):
        """MySQL DB 연결 해제"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("✅ MySQL 연결 해제 완료")

    def load_all_data(self) -> bool:
        """모든 CSV 데이터를 DB에 적재"""
        try:
            print(f"\n{'='*80}")
            print("MySQL DB 데이터 적재 시작")
            print(f"{'='*80}\n")

            # 기존 테이블 삭제 (역순)
            self._drop_all_tables()

            # 테이블 생성 및 데이터 적재 (순서대로)
            for table_name in TABLE_ORDER:
                csv_file = f"{table_name}.csv"
                csv_path = self.csv_dir / csv_file

                if not csv_path.exists():
                    print(f"⚠️  {csv_file} 파일이 없습니다. 스킵합니다.")
                    continue

                # 테이블 생성
                self._create_table(table_name)

                # 데이터 적재
                self._load_csv_to_table(csv_path, table_name)

            # Foreign Key 생성
            self._create_foreign_keys()

            # 커밋
            self.connection.commit()

            # 통계 출력
            self._print_statistics()

            print(f"\n{'='*80}")
            print("✅ 모든 데이터 적재 완료!")
            print(f"{'='*80}\n")

            return True

        except Exception as e:
            print(f"❌ 데이터 적재 오류: {e}")
            if self.connection:
                self.connection.rollback()
            import traceback
            traceback.print_exc()
            return False

    def _drop_all_tables(self):
        """모든 테이블 삭제 (Foreign Key 때문에 역순)"""
        print("[1단계] 기존 테이블 삭제 중...")

        # Foreign Key 제약조건 비활성화
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        for table_name in reversed(TABLE_ORDER):
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                print(f"  🗑��  {table_name} 삭제")
            except Exception as e:
                print(f"  ⚠️  {table_name} 삭제 실패: {e}")

        # Foreign Key 제약조건 활성화
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("✅ 기존 테이블 삭제 완료\n")

    def _create_table(self, table_name: str):
        """테이블 생성"""
        table_schemas = {
            "document_metadata": """
                CREATE TABLE `document_metadata` (
                    `id` INT PRIMARY KEY,
                    `source_file` VARCHAR(255),
                    `total_pages` INT,
                    `total_tables` INT,
                    `extraction_date` DATETIME,
                    `file_size` BIGINT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            "detail_projects": """
                CREATE TABLE `detail_projects` (
                    `id` INT PRIMARY KEY,
                    `name` VARCHAR(500),
                    `department` VARCHAR(255),
                    `project_type` VARCHAR(100),
                    `representative_field` TEXT,
                    `managing_org` VARCHAR(255),
                    `supervising_org` VARCHAR(255),
                    `page_number` INT,
                    INDEX `idx_name` (`name`(255)),
                    FULLTEXT INDEX `idx_fulltext_name` (`name`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            "sub_projects": """
                CREATE TABLE `sub_projects` (
                    `id` INT PRIMARY KEY,
                    `detail_project_id` INT,
                    `name` VARCHAR(500),
                    `overview` TEXT,
                    `objectives` TEXT,
                    `content` TEXT,
                    `project_type` VARCHAR(100),
                    `representative_field` TEXT,
                    `research_period` VARCHAR(255),
                    `total_budget_text` VARCHAR(255),
                    `managing_org` VARCHAR(255),
                    `supervising_org` VARCHAR(255),
                    `full_page_text` LONGTEXT,
                    `page_number` INT,
                    INDEX `idx_detail_project` (`detail_project_id`),
                    INDEX `idx_name` (`name`(255)),
                    FULLTEXT INDEX `idx_fulltext_search` (`name`, `overview`, `objectives`, `content`, `full_page_text`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            "sub_project_programs": """
                CREATE TABLE `sub_project_programs` (
                    `id` INT PRIMARY KEY,
                    `sub_project_id` INT,
                    `program_name` VARCHAR(500),
                    `description` TEXT,
                    `page_number` INT,
                    INDEX `idx_sub_project` (`sub_project_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            "budgets": """
                CREATE TABLE `budgets` (
                    `id` INT PRIMARY KEY,
                    `sub_project_id` INT,
                    `year` VARCHAR(10),
                    `total_budget` DECIMAL(20, 2),
                    `national_budget` DECIMAL(20, 2),
                    `local_budget` DECIMAL(20, 2),
                    `other_budget` DECIMAL(20, 2),
                    `page_number` INT,
                    INDEX `idx_sub_project` (`sub_project_id`),
                    INDEX `idx_year` (`year`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            "performances": """
                CREATE TABLE `performances` (
                    `id` INT PRIMARY KEY,
                    `sub_project_id` INT,
                    `indicator` VARCHAR(500),
                    `target_value` VARCHAR(255),
                    `unit` VARCHAR(50),
                    `page_number` INT,
                    INDEX `idx_sub_project` (`sub_project_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            "schedules": """
                CREATE TABLE `schedules` (
                    `id` INT PRIMARY KEY,
                    `sub_project_id` INT,
                    `task` VARCHAR(500),
                    `schedule` VARCHAR(255),
                    `page_number` INT,
                    INDEX `idx_sub_project` (`sub_project_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            "raw_tables": """
                CREATE TABLE `raw_tables` (
                    `id` INT PRIMARY KEY,
                    `sub_project_id` INT,
                    `page_number` INT,
                    `table_type` VARCHAR(50),
                    `table_data` LONGTEXT,
                    `full_page_text` LONGTEXT,
                    INDEX `idx_sub_project` (`sub_project_id`),
                    INDEX `idx_page` (`page_number`),
                    FULLTEXT INDEX `idx_fulltext_page` (`full_page_text`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        }

        if table_name in table_schemas:
            self.cursor.execute(table_schemas[table_name])
            print(f"  ✅ {table_name} 테이블 생성")

    def _load_csv_to_table(self, csv_path: Path, table_name: str):
        """CSV 파일을 테이블에 적재"""
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            if not rows:
                print(f"  ⚠️  {csv_path.name}: 데이터 없음")
                return

            # 첫 번째 행으로 컬럼 확인
            columns = list(rows[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join([f'`{col}`' for col in columns])

            sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

            # 배치 INSERT
            batch_size = 100
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                values = [tuple(row.values()) for row in batch]
                self.cursor.executemany(sql, values)

            print(f"  ✅ {csv_path.name}: {len(rows)}건 적재 완료")

    def _create_foreign_keys(self):
        """Foreign Key 제약조건 생성"""
        print("\n[3단계] Foreign Key 생성 중...")

        fk_constraints = [
            ("sub_projects", "fk_sub_detail", "detail_project_id", "detail_projects", "id"),
            ("sub_project_programs", "fk_program_sub", "sub_project_id", "sub_projects", "id"),
            ("budgets", "fk_budget_sub", "sub_project_id", "sub_projects", "id"),
            ("performances", "fk_performance_sub", "sub_project_id", "sub_projects", "id"),
            ("schedules", "fk_schedule_sub", "sub_project_id", "sub_projects", "id"),
            ("raw_tables", "fk_raw_sub", "sub_project_id", "sub_projects", "id"),
        ]

        for table, fk_name, column, ref_table, ref_column in fk_constraints:
            try:
                sql = f"""
                    ALTER TABLE `{table}`
                    ADD CONSTRAINT `{fk_name}`
                    FOREIGN KEY (`{column}`)
                    REFERENCES `{ref_table}`(`{ref_column}`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                """
                self.cursor.execute(sql)
                print(f"  🔗 {table}.{column} -> {ref_table}.{ref_column}")
            except Exception as e:
                print(f"  ⚠️  FK 생성 실패 ({table}): {e}")

        print("✅ Foreign Key 생성 완료\n")

    def _print_statistics(self):
        """DB 통계 출력"""
        print(f"\n{'='*80}")
        print("데이터베이스 통계")
        print(f"{'='*80}")

        for table_name in TABLE_ORDER:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                count = self.cursor.fetchone()[0]
                print(f"📊 {table_name}: {count}건")
            except:
                pass

        print(f"{'='*80}")


def load_to_mysql(csv_dir: str) -> bool:
    """
    CSV 파일들을 MySQL DB에 적재하는 메인 함수

    Args:
        csv_dir: CSV 파일 디렉토리 경로

    Returns:
        성공 여부
    """
    loader = MySQLLoader(csv_dir)

    if not loader.connect():
        return False

    try:
        result = loader.load_all_data()
        return result
    finally:
        loader.disconnect()

