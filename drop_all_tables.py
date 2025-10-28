"""
MySQL DB 스키마의 모든 테이블 삭제 스크립트
"""
import pymysql
from config import MYSQL_CONFIG

def drop_all_tables():
    """convert_pdf 스키마의 모든 테이블 삭제"""
    try:
        # MySQL 연결
        connection = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            port=MYSQL_CONFIG['port'],
            charset=MYSQL_CONFIG['charset']
        )
        cursor = connection.cursor()

        print("\n" + "="*80)
        print(f"MySQL DB '{MYSQL_CONFIG['database']}' 스키마의 모든 테이블 삭제")
        print("="*80 + "\n")

        # Foreign Key 제약조건 비활성화
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        # 현재 테이블 목록 조회
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        if not tables:
            print("⚠️  삭제할 테이블이 없습니다.\n")
            return

        print(f"📋 발견된 테이블: {len(tables)}개\n")

        # 각 테이블 삭제
        for (table_name,) in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                print(f"  🗑️  {table_name} 삭제 완료")
            except Exception as e:
                print(f"  ❌ {table_name} 삭제 실패: {e}")

        # Foreign Key 제약조건 활성화
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        connection.commit()
        cursor.close()
        connection.close()

        print("\n" + "="*80)
        print("✅ 모든 테이블 삭제 완료!")
        print("="*80 + "\n")

        return True

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    drop_all_tables()

