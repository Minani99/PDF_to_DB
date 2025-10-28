"""
정규화 결과 시각화 도구
CSV 데이터의 통계 및 관계를 시각화합니다.
"""
import csv
from pathlib import Path
from collections import defaultdict
from config import NORMALIZED_OUTPUT_DIR


def visualize_normalization_stats():
    """정규화된 데이터의 통계를 콘솔에 출력"""

    print("\n" + "="*80)
    print("정규화 데이터 통계")
    print("="*80 + "\n")

    csv_dir = Path(NORMALIZED_OUTPUT_DIR)

    # 각 CSV 파일의 레코드 수 계산
    stats = {}

    csv_files = [
        "document_metadata.csv",
        "detail_projects.csv",
        "sub_projects.csv",
        "sub_project_programs.csv",
        "budgets.csv",
        "performances.csv",
        "schedules.csv",
        "raw_tables.csv"
    ]

    for csv_file in csv_files:
        csv_path = csv_dir / csv_file
        if csv_path.exists():
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                row_count = sum(1 for row in reader) - 1  # 헤더 제외
                stats[csv_file] = row_count
                print(f"📊 {csv_file:<30} {row_count:>5}건")
        else:
            print(f"⚠️  {csv_file:<30} 파일 없음")

    print("\n" + "="*80)
    print(f"총 레코드 수: {sum(stats.values())}건")
    print("="*80 + "\n")

    # 관계 분석
    print("📈 데이터 관계 분석")
    print("-" * 80)

    # 세부사업당 내역사업 수
    detail_project_counts = defaultdict(int)
    sub_project_path = csv_dir / "sub_projects.csv"

    if sub_project_path.exists():
        with open(sub_project_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                detail_id = row.get('detail_project_id', '')
                if detail_id:
                    detail_project_counts[detail_id] += 1

        print(f"세부사업 수: {len(detail_project_counts)}개")
        print(f"내역사업 수: {sum(detail_project_counts.values())}개")
        print(f"세부사업당 평균 내역사업: {sum(detail_project_counts.values()) / len(detail_project_counts):.1f}개")

    # 내역사업당 하위 항목 통계
    sub_project_stats = defaultdict(lambda: {'programs': 0, 'budgets': 0, 'performances': 0, 'schedules': 0})

    tables = [
        ('sub_project_programs.csv', 'programs'),
        ('budgets.csv', 'budgets'),
        ('performances.csv', 'performances'),
        ('schedules.csv', 'schedules')
    ]

    for csv_file, key in tables:
        csv_path = csv_dir / csv_file
        if csv_path.exists():
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sub_id = row.get('sub_project_id', '')
                    if sub_id:
                        sub_project_stats[sub_id][key] += 1

    if sub_project_stats:
        total_subs = len(sub_project_stats)
        avg_programs = sum(s['programs'] for s in sub_project_stats.values()) / total_subs
        avg_budgets = sum(s['budgets'] for s in sub_project_stats.values()) / total_subs
        avg_performances = sum(s['performances'] for s in sub_project_stats.values()) / total_subs
        avg_schedules = sum(s['schedules'] for s in sub_project_stats.values()) / total_subs

        print(f"\n내역사업당 평균:")
        print(f"  - 프로그램: {avg_programs:.1f}개")
        print(f"  - 예산항목: {avg_budgets:.1f}건")
        print(f"  - 성과지표: {avg_performances:.1f}건")
        print(f"  - 일정항목: {avg_schedules:.1f}건")

    print("\n" + "="*80 + "\n")


def show_sample_data():
    """샘플 데이터 출력"""

    print("📋 샘플 데이터 미리보기")
    print("="*80 + "\n")

    csv_dir = Path(NORMALIZED_OUTPUT_DIR)

    # 세부사업 샘플
    detail_path = csv_dir / "detail_projects.csv"
    if detail_path.exists():
        print("🔹 세부사업 (detail_projects)")
        print("-" * 80)
        with open(detail_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 3:
                    break
                print(f"  ID {row.get('id')}: {row.get('name', '')[:50]}")
        print()

    # 내역사업 샘플
    sub_path = csv_dir / "sub_projects.csv"
    if sub_path.exists():
        print("🔹 내역사업 (sub_projects)")
        print("-" * 80)
        with open(sub_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 3:
                    break
                print(f"  ID {row.get('id')} (세부사업 ID: {row.get('detail_project_id')})")
                print(f"    이름: {row.get('name', '')[:50]}")
                print(f"    개요: {row.get('overview', '')[:50]}...")
                print()

    print("="*80 + "\n")


if __name__ == "__main__":
    visualize_normalization_stats()
    show_sample_data()
# Python 필수 패키지
# PDF 데이터 처리 및 MySQL DB 적재 프로그램

# MySQL 데이터베이스 드라이버
pymysql>=1.1.0

# CSV 및 데이터 처리 (기본 라이브러리에 포함되어 있으나 명시)
# csv, json, pathlib는 Python 표준 라이브러리

# 시각화 (선택 사항 - visualize_normalization.py 사용 시)
# matplotlib>=3.7.0
# pandas>=2.0.0

