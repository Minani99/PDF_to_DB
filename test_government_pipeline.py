"""
정부 표준 데이터 처리 파이프라인 테스트
DB 없이 정규화 및 시각화 테스트
"""
import json
from pathlib import Path
import logging
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

from normalize_government_standard import GovernmentStandardNormalizer

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 한글 폰트 설정
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False


def test_normalization():
    """정규화 테스트"""
    logger.info("🚀 정규화 테스트 시작...")
    
    # 파일 경로 설정
    json_file = Path("output/sample_government_data.json")
    output_dir = Path("normalized_output_government")
    visualization_dir = Path("visualization_government")
    
    output_dir.mkdir(exist_ok=True)
    visualization_dir.mkdir(exist_ok=True)
    
    if not json_file.exists():
        logger.error(f"❌ JSON 파일이 없습니다: {json_file}")
        return None
    
    # 정규화 실행
    normalizer = GovernmentStandardNormalizer(str(json_file), str(output_dir))
    
    # JSON 로드
    with open(json_file, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    # 정규화 처리
    success = normalizer.normalize(json_data)
    
    if success:
        # CSV 저장
        normalizer.save_to_csv()
        
        # 통계 출력
        normalizer.print_statistics()
        
        # 검증
        validation_result = normalizer.validate_data()
        
        # 시각화
        visualize_normalized_data(normalizer.data, visualization_dir)
        
        return normalizer.data, validation_result
    
    return None, None


def visualize_normalized_data(data, vis_dir):
    """정규화 데이터 시각화"""
    logger.info("📊 시각화 생성 중...")
    
    # 1. 데이터 타입별 레코드 수
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # 데이터 수집
    table_names = []
    record_counts = []
    
    for table_name, records in data.items():
        if table_name != 'data_statistics' and records:
            clean_name = table_name.replace('normalized_', '').replace('_', ' ').title()
            table_names.append(clean_name)
            record_counts.append(len(records))
    
    if table_names:
        # 바 차트
        colors = plt.cm.Set3(range(len(table_names)))
        bars = ax1.bar(range(len(table_names)), record_counts, color=colors)
        ax1.set_xticks(range(len(table_names)))
        ax1.set_xticklabels(table_names, rotation=45, ha='right')
        ax1.set_title('Normalized Records by Type', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Record Count')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # 값 표시
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}', ha='center', va='bottom')
        
        # 파이 차트
        ax2.pie(record_counts, labels=table_names, autopct='%1.1f%%', colors=colors)
        ax2.set_title('Data Distribution', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(vis_dir / 'normalized_data_overview.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. 분기별 일정 분포
    if data.get('normalized_schedules'):
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        
        # 분기별 집계
        quarters = {}
        for schedule in data['normalized_schedules']:
            q = schedule.get('quarter', 0)
            if q not in quarters:
                quarters[q] = 0
            quarters[q] += 1
        
        # 정렬 및 시각화
        sorted_quarters = sorted(quarters.items())
        q_labels = [f"Q{q}" if q > 0 else "All Year" for q, _ in sorted_quarters]
        q_values = [count for _, count in sorted_quarters]
        
        bars = ax.bar(q_labels, q_values, color='steelblue')
        ax.set_title('Schedule Distribution by Quarter', fontsize=12, fontweight='bold')
        ax.set_xlabel('Quarter')
        ax.set_ylabel('Number of Tasks')
        ax.grid(True, alpha=0.3, axis='y')
        
        # 값 표시
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height)}', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig(vis_dir / 'schedule_by_quarter.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    # 3. 성과 지표 시각화
    if data.get('normalized_performances'):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # 카테고리별 집계
        categories = {}
        types = {}
        
        for perf in data['normalized_performances']:
            cat = perf.get('indicator_category', 'Unknown')
            typ = perf.get('indicator_type', 'Unknown')
            val = perf.get('value', 0)
            
            if cat not in categories:
                categories[cat] = 0
            categories[cat] += val
            
            key = f"{cat}-{typ}"
            if key not in types:
                types[key] = 0
            types[key] += val
        
        # 카테고리별 차트
        if categories:
            cat_names = list(categories.keys())
            cat_values = list(categories.values())
            
            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
            bars = ax1.bar(cat_names, cat_values, color=colors[:len(cat_names)])
            ax1.set_title('Performance by Category', fontsize=12, fontweight='bold')
            ax1.set_ylabel('Total Value')
            ax1.grid(True, alpha=0.3, axis='y')
            
            for bar in bars:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height):,}', ha='center', va='bottom')
        
        # 상위 지표 타입
        if types:
            sorted_types = sorted(types.items(), key=lambda x: x[1], reverse=True)[:10]
            type_names = [t[0] for t in sorted_types]
            type_values = [t[1] for t in sorted_types]
            
            y_pos = range(len(type_names))
            bars = ax2.barh(y_pos, type_values, color='coral')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels(type_names, fontsize=9)
            ax2.set_xlabel('Value')
            ax2.set_title('Top 10 Performance Indicators', fontsize=12, fontweight='bold')
            ax2.grid(True, alpha=0.3, axis='x')
            
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax2.text(width, bar.get_y() + bar.get_height()/2.,
                        f'{int(width):,}', ha='left', va='center')
        
        plt.tight_layout()
        plt.savefig(vis_dir / 'performance_indicators.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    # 4. 예산 분석
    if data.get('normalized_budgets'):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # 연도별 예산
        year_budgets = {}
        type_budgets = {}
        
        for budget in data['normalized_budgets']:
            year = budget.get('budget_year', 0)
            btype = budget.get('budget_type', 'Unknown')
            amount = float(budget.get('amount', 0))
            
            if year not in year_budgets:
                year_budgets[year] = 0
            year_budgets[year] += amount
            
            if btype not in type_budgets:
                type_budgets[btype] = 0
            type_budgets[btype] += amount
        
        # 연도별 차트
        if year_budgets:
            years = sorted(year_budgets.keys())
            amounts = [year_budgets[y] for y in years]
            
            bars = ax1.bar([str(y) for y in years], amounts, color='green', alpha=0.7)
            ax1.set_title('Budget by Year', fontsize=12, fontweight='bold')
            ax1.set_xlabel('Year')
            ax1.set_ylabel('Amount (Million KRW)')
            ax1.grid(True, alpha=0.3, axis='y')
            
            for bar in bars:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height):,}', ha='center', va='bottom')
        
        # 예산 타입별 파이 차트
        if type_budgets:
            colors = ['#3498DB', '#E74C3C', '#F39C12', '#2ECC71']
            ax2.pie(type_budgets.values(), labels=type_budgets.keys(), 
                   autopct='%1.1f%%', colors=colors[:len(type_budgets)])
            ax2.set_title('Budget Distribution by Type', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(vis_dir / 'budget_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    # 5. 내역사업 요약
    if data.get('sub_projects'):
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        
        # 내역사업별 데이터 카운트
        project_stats = []
        
        for project in data['sub_projects']:
            proj_id = project['id']
            proj_name = project['sub_project_name']
            
            # 각 데이터 타입별 카운트
            schedule_count = sum(1 for s in data.get('normalized_schedules', []) 
                               if s['sub_project_id'] == proj_id)
            perf_count = sum(1 for p in data.get('normalized_performances', []) 
                           if p['sub_project_id'] == proj_id)
            budget_count = sum(1 for b in data.get('normalized_budgets', []) 
                             if b['sub_project_id'] == proj_id)
            
            project_stats.append({
                'name': proj_name[:30] + '...' if len(proj_name) > 30 else proj_name,
                'schedules': schedule_count,
                'performances': perf_count,
                'budgets': budget_count
            })
        
        if project_stats:
            # 스택 바 차트
            x = range(len(project_stats))
            names = [p['name'] for p in project_stats]
            schedules = [p['schedules'] for p in project_stats]
            performances = [p['performances'] for p in project_stats]
            budgets = [p['budgets'] for p in project_stats]
            
            width = 0.25
            ax.bar([i - width for i in x], schedules, width, label='Schedules', color='#3498DB')
            ax.bar(x, performances, width, label='Performances', color='#E74C3C')
            ax.bar([i + width for i in x], budgets, width, label='Budgets', color='#2ECC71')
            
            ax.set_xlabel('Sub Projects')
            ax.set_ylabel('Record Count')
            ax.set_title('Data Coverage by Sub Project', fontsize=12, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(names, rotation=45, ha='right')
            ax.legend()
            ax.grid(True, alpha=0.3, axis='y')
            
            plt.tight_layout()
            plt.savefig(vis_dir / 'subproject_coverage.png', dpi=300, bbox_inches='tight')
            plt.close()
    
    logger.info(f"✅ 시각화 완료 - {vis_dir}에 저장됨")


def generate_summary_report(data, validation, vis_dir):
    """요약 보고서 생성"""
    report = []
    report.append("="*80)
    report.append("📊 정부/공공기관 표준 데이터 정규화 테스트 보고서")
    report.append("="*80)
    report.append(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    if data:
        report.append("📁 정규화 결과:")
        for table_name, records in data.items():
            if records and table_name != 'data_statistics':
                report.append(f"  • {table_name}: {len(records)}건")
        report.append("")
        
        # 내역사업 목록
        if data.get('sub_projects'):
            report.append("🏢 내역사업 목록:")
            for project in data['sub_projects']:
                report.append(f"  • {project['sub_project_name']} (ID: {project['id']})")
            report.append("")
        
        # 분기별 일정
        if data.get('normalized_schedules'):
            quarters = {}
            for schedule in data['normalized_schedules']:
                q = schedule.get('quarter', 0)
                if q not in quarters:
                    quarters[q] = 0
                quarters[q] += 1
            
            report.append("📅 분기별 일정 분포:")
            for q, count in sorted(quarters.items()):
                q_label = f"{q}/4분기" if q > 0 else "연중"
                report.append(f"  • {q_label}: {count}건")
            report.append("")
        
        # 성과 지표
        if data.get('normalized_performances'):
            categories = {}
            for perf in data['normalized_performances']:
                cat = perf.get('indicator_category', 'Unknown')
                val = perf.get('value', 0)
                if cat not in categories:
                    categories[cat] = 0
                categories[cat] += val
            
            report.append("📊 성과 지표 요약:")
            for cat, val in categories.items():
                report.append(f"  • {cat}: {val:,}")
            report.append("")
        
        # 예산 요약
        if data.get('normalized_budgets'):
            total = sum(float(b.get('amount', 0)) for b in data['normalized_budgets'])
            report.append(f"💰 총 예산: {total:,.0f} 백만원")
            report.append("")
    
    if validation:
        report.append("✅ 데이터 검증:")
        report.append(f"  • 처리율: {validation.get('process_rate', 'N/A')}")
        report.append(f"  • 통계: {validation.get('statistics', {})}")
        
        if validation.get('issues'):
            report.append("  ⚠️ 발견된 문제:")
            for issue in validation['issues']:
                report.append(f"    - {issue}")
        else:
            report.append("  • 모든 검증 통과 ✓")
    
    report.append("")
    report.append("📊 생성된 시각화 파일:")
    report.append(f"  • {vis_dir}/normalized_data_overview.png")
    report.append(f"  • {vis_dir}/schedule_by_quarter.png")
    report.append(f"  • {vis_dir}/performance_indicators.png")
    report.append(f"  • {vis_dir}/budget_analysis.png")
    report.append(f"  • {vis_dir}/subproject_coverage.png")
    
    report.append("")
    report.append("="*80)
    
    # 보고서 저장
    report_text = "\n".join(report)
    report_file = vis_dir / "test_report.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    # 콘솔 출력
    print(report_text)
    
    return report_text


def main():
    """메인 실행"""
    logger.info("🚀 정부 표준 데이터 정규화 테스트 시작")
    
    # 테스트 실행
    data, validation = test_normalization()
    
    if data:
        # 보고서 생성
        vis_dir = Path("visualization_government")
        generate_summary_report(data, validation, vis_dir)
        
        print("\n✅ 테스트 완료!")
        print(f"📁 정규화 CSV: normalized_output_government/")
        print(f"📊 시각화 파일: visualization_government/")
        print(f"📄 보고서: visualization_government/test_report.txt")
        
        return True
    else:
        print("\n❌ 테스트 실패!")
        return False


if __name__ == "__main__":
    success = main()