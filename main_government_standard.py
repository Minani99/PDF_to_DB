"""
정부/공공기관 표준 데이터 처리 메인 스크립트
PDF → JSON → 정규화 → DB 적재 → 시각화 → 검증
"""
import os
import json
from pathlib import Path
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from typing import Dict, Any

# 모듈 임포트
from normalize_government_standard import GovernmentStandardNormalizer
from load_government_standard_db import GovernmentStandardDBLoader
from config import MYSQL_CONFIG

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 한글 폰트 설정
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False


class GovernmentDataPipeline:
    """정부 표준 데이터 처리 파이프라인"""
    
    def __init__(self):
        self.input_dir = Path("output")  # JSON 파일 위치
        self.output_dir = Path("normalized_output_government")
        self.visualization_dir = Path("visualization_government")
        self.visualization_dir.mkdir(exist_ok=True)
        
        # 통계
        self.pipeline_stats = {
            'start_time': datetime.now(),
            'json_files_processed': 0,
            'total_tables_extracted': 0,
            'normalized_records': 0,
            'db_records_loaded': 0,
            'verification_passed': False,
            'errors': []
        }
    
    def run_normalization(self) -> Dict[str, int]:
        """정규화 실행"""
        logger.info("🔄 정규화 프로세스 시작...")
        
        normalized_stats = {
            'files_processed': 0,
            'total_records': 0,
            'records_by_type': {}
        }
        
        # JSON 파일 찾기
        json_files = list(self.input_dir.glob("*.json"))
        
        if not json_files:
            logger.error("❌ JSON 파일을 찾을 수 없습니다.")
            return normalized_stats
        
        for json_file in json_files:
            try:
                logger.info(f"📄 처리 중: {json_file.name}")
                
                # 정규화 실행
                normalizer = GovernmentStandardNormalizer(
                    str(json_file),
                    str(self.output_dir)
                )
                
                # JSON 데이터 로드 및 처리
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # 정규화 처리
                normalizer.normalize(data)
                
                # CSV 저장
                normalizer.save_to_csv()
                
                # 통계 업데이트
                normalized_stats['files_processed'] += 1
                
                # 레코드 수 카운트
                for table_name in normalizer.data.keys():
                    count = len(normalizer.data[table_name])
                    if table_name not in normalized_stats['records_by_type']:
                        normalized_stats['records_by_type'][table_name] = 0
                    normalized_stats['records_by_type'][table_name] += count
                    normalized_stats['total_records'] += count
                
                logger.info(f"✅ {json_file.name} 정규화 완료")
                
            except Exception as e:
                logger.error(f"❌ {json_file.name} 정규화 실패: {e}")
                self.pipeline_stats['errors'].append(f"정규화 실패: {json_file.name}")
        
        self.pipeline_stats['json_files_processed'] = normalized_stats['files_processed']
        self.pipeline_stats['normalized_records'] = normalized_stats['total_records']
        
        return normalized_stats
    
    def load_to_database(self) -> Dict[str, Any]:
        """데이터베이스 적재"""
        logger.info("💾 데이터베이스 적재 시작...")
        
        # DB 설정
        db_config = MYSQL_CONFIG.copy()
        db_config['database'] = 'government_standard'
        
        # 적재 실행
        loader = GovernmentStandardDBLoader(db_config, str(self.output_dir))
        
        try:
            loader.connect()
            loader.drop_existing_tables()
            loader.create_tables()
            loader.load_all_tables()
            
            # 검증
            verification = loader.verify_data_integrity()
            
            self.pipeline_stats['db_records_loaded'] = loader.load_stats['total_records']
            
            return verification
            
        except Exception as e:
            logger.error(f"❌ DB 적재 실패: {e}")
            self.pipeline_stats['errors'].append(f"DB 적재 실패: {str(e)}")
            return {}
            
        finally:
            loader.close()
    
    def visualize_data(self, normalized_stats: Dict, db_verification: Dict):
        """데이터 시각화"""
        logger.info("📊 데이터 시각화 생성 중...")
        
        # 1. 정규화 통계 시각화
        self._plot_normalization_stats(normalized_stats)
        
        # 2. DB 적재 통계 시각화
        self._plot_db_stats(db_verification)
        
        # 3. 데이터 완전성 시각화
        self._plot_data_completeness(db_verification)
        
        # 4. 파이프라인 요약 시각화
        self._plot_pipeline_summary()
        
        logger.info(f"✅ 시각화 완료 - {self.visualization_dir}에 저장됨")
    
    def _plot_normalization_stats(self, stats: Dict):
        """정규화 통계 시각화"""
        if not stats.get('records_by_type'):
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # 테이블별 레코드 수
        tables = list(stats['records_by_type'].keys())
        counts = list(stats['records_by_type'].values())
        
        ax1.bar(range(len(tables)), counts, color='steelblue')
        ax1.set_xticks(range(len(tables)))
        ax1.set_xticklabels(tables, rotation=45, ha='right')
        ax1.set_title('Records by Table Type')
        ax1.set_ylabel('Record Count')
        ax1.grid(True, alpha=0.3)
        
        # 파이 차트
        colors = plt.cm.Set3(range(len(tables)))
        ax2.pie(counts, labels=tables, autopct='%1.1f%%', colors=colors)
        ax2.set_title('Data Distribution')
        
        plt.tight_layout()
        plt.savefig(self.visualization_dir / 'normalization_stats.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _plot_db_stats(self, verification: Dict):
        """DB 적재 통계 시각화"""
        if not verification.get('normalized_counts'):
            return
        
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        
        # 정규화 테이블 데이터
        tables = list(verification['normalized_counts'].keys())
        counts = list(verification['normalized_counts'].values())
        
        # 바 차트
        bars = ax.bar(tables, counts, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'])
        
        # 값 표시
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}',
                   ha='center', va='bottom')
        
        ax.set_title('Database Load Statistics', fontsize=14, fontweight='bold')
        ax.set_ylabel('Record Count')
        ax.set_xlabel('Table Name')
        ax.grid(True, alpha=0.3, axis='y')
        
        # 원본 데이터 수 추가
        ax.axhline(y=verification.get('raw_data_count', 0), 
                  color='red', linestyle='--', alpha=0.5, 
                  label=f"Raw Data: {verification.get('raw_data_count', 0):,}")
        ax.legend()
        
        plt.tight_layout()
        plt.savefig(self.visualization_dir / 'db_load_stats.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _plot_data_completeness(self, verification: Dict):
        """데이터 완전성 시각화"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # 1. 데이터 완전성 지표
        total_projects = verification.get('total_sub_projects', 0)
        missing_data = len(verification.get('missing_data', []))
        complete_projects = total_projects - missing_data
        
        if total_projects > 0:
            completeness = (complete_projects / total_projects) * 100
            
            # 도넛 차트
            sizes = [complete_projects, missing_data]
            colors = ['#2ECC71', '#E74C3C']
            explode = (0.05, 0.05)
            
            ax1.pie(sizes, explode=explode, labels=['Complete', 'Incomplete'],
                   colors=colors, autopct='%1.1f%%', startangle=90)
            ax1.set_title(f'Data Completeness\n({completeness:.1f}% Complete)')
        
        # 2. 데이터 유형별 커버리지
        if verification.get('normalized_counts'):
            data_types = ['Schedules', 'Performances', 'Budgets', 'Overviews']
            coverage = []
            
            for table_key, display_name in zip(
                ['normalized_schedules', 'normalized_performances', 
                 'normalized_budgets', 'normalized_overviews'],
                data_types
            ):
                count = verification['normalized_counts'].get(table_key, 0)
                if total_projects > 0:
                    avg_per_project = count / total_projects
                    coverage.append(avg_per_project)
                else:
                    coverage.append(0)
            
            # 수평 바 차트
            y_pos = range(len(data_types))
            bars = ax2.barh(y_pos, coverage, color=['#3498DB', '#9B59B6', '#F39C12', '#1ABC9C'])
            
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels(data_types)
            ax2.set_xlabel('Average Records per Project')
            ax2.set_title('Data Coverage by Type')
            ax2.grid(True, alpha=0.3, axis='x')
            
            # 값 표시
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax2.text(width, bar.get_y() + bar.get_height()/2.,
                        f'{width:.1f}',
                        ha='left', va='center')
        
        plt.tight_layout()
        plt.savefig(self.visualization_dir / 'data_completeness.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _plot_pipeline_summary(self):
        """파이프라인 요약 시각화"""
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        
        # 파이프라인 단계별 통계
        stages = ['JSON Files', 'Normalized\nRecords', 'DB Records\nLoaded']
        values = [
            self.pipeline_stats['json_files_processed'],
            self.pipeline_stats['normalized_records'],
            self.pipeline_stats['db_records_loaded']
        ]
        
        # 색상 그라데이션
        colors = ['#3498DB', '#2ECC71', '#F39C12']
        
        # 바 차트
        bars = ax.bar(stages, values, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
        
        # 값 표시
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}',
                   ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        # 제목 및 레이블
        ax.set_title('Government Standard Data Pipeline Summary', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_ylabel('Count', fontsize=12)
        ax.set_xlabel('Pipeline Stage', fontsize=12)
        
        # 실행 시간 표시
        execution_time = (datetime.now() - self.pipeline_stats['start_time']).total_seconds()
        ax.text(0.02, 0.98, f'Execution Time: {execution_time:.1f} seconds',
               transform=ax.transAxes, fontsize=10,
               verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # 오류 표시
        if self.pipeline_stats['errors']:
            error_text = f"Errors: {len(self.pipeline_stats['errors'])}"
            ax.text(0.98, 0.98, error_text,
                   transform=ax.transAxes, fontsize=10,
                   verticalalignment='top', horizontalalignment='right',
                   color='red', fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='#FFE5E5', alpha=0.5))
        
        ax.grid(True, alpha=0.3, axis='y')
        ax.set_ylim(0, max(values) * 1.15)
        
        plt.tight_layout()
        plt.savefig(self.visualization_dir / 'pipeline_summary.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def generate_report(self, normalized_stats: Dict, db_verification: Dict):
        """최종 보고서 생성"""
        report = []
        report.append("=" * 80)
        report.append("📊 정부/공공기관 표준 데이터 처리 보고서")
        report.append("=" * 80)
        report.append(f"실행 시간: {self.pipeline_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"소요 시간: {(datetime.now() - self.pipeline_stats['start_time']).total_seconds():.1f}초")
        report.append("")
        
        report.append("📁 정규화 결과:")
        report.append(f"  • 처리된 JSON 파일: {normalized_stats.get('files_processed', 0)}개")
        report.append(f"  • 총 정규화 레코드: {normalized_stats.get('total_records', 0):,}건")
        
        if normalized_stats.get('records_by_type'):
            report.append("  • 테이블별 레코드:")
            for table, count in normalized_stats['records_by_type'].items():
                report.append(f"    - {table}: {count:,}건")
        report.append("")
        
        report.append("💾 데이터베이스 적재:")
        report.append(f"  • 총 내역사업: {db_verification.get('total_sub_projects', 0)}개")
        report.append(f"  • 원본 데이터: {db_verification.get('raw_data_count', 0)}건")
        
        if db_verification.get('normalized_counts'):
            report.append("  • 정규화 테이블:")
            for table, count in db_verification['normalized_counts'].items():
                report.append(f"    - {table}: {count:,}건")
        report.append("")
        
        report.append("✅ 데이터 검증:")
        missing_count = len(db_verification.get('missing_data', []))
        orphan_count = len(db_verification.get('orphan_records', {}))
        
        if missing_count == 0 and orphan_count == 0:
            report.append("  • 모든 데이터 무결성 검증 통과 ✓")
            self.pipeline_stats['verification_passed'] = True
        else:
            if missing_count > 0:
                report.append(f"  ⚠️ 데이터 누락 사업: {missing_count}개")
            if orphan_count > 0:
                report.append(f"  ⚠️ 고아 레코드 테이블: {orphan_count}개")
        
        if self.pipeline_stats['errors']:
            report.append("")
            report.append("❌ 오류 발생:")
            for error in self.pipeline_stats['errors']:
                report.append(f"  - {error}")
        
        report.append("")
        report.append("📊 시각화 파일:")
        report.append(f"  • 저장 위치: {self.visualization_dir}")
        report.append("  • 생성된 차트:")
        report.append("    - normalization_stats.png")
        report.append("    - db_load_stats.png")
        report.append("    - data_completeness.png")
        report.append("    - pipeline_summary.png")
        
        report.append("")
        report.append("=" * 80)
        
        # 보고서 저장
        report_text = "\n".join(report)
        report_file = self.visualization_dir / "processing_report.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        # 콘솔 출력
        print(report_text)
        
        return report_text
    
    def run(self):
        """전체 파이프라인 실행"""
        logger.info("🚀 정부 표준 데이터 처리 파이프라인 시작")
        
        try:
            # 1. 정규화
            normalized_stats = self.run_normalization()
            
            if normalized_stats['files_processed'] == 0:
                logger.error("❌ 정규화할 파일이 없습니다.")
                return False
            
            # 2. 데이터베이스 적재
            db_verification = self.load_to_database()
            
            # 3. 시각화
            self.visualize_data(normalized_stats, db_verification)
            
            # 4. 보고서 생성
            self.generate_report(normalized_stats, db_verification)
            
            logger.info("✅ 파이프라인 완료!")
            
            return self.pipeline_stats['verification_passed']
            
        except Exception as e:
            logger.error(f"❌ 파이프라인 실행 실패: {e}")
            self.pipeline_stats['errors'].append(str(e))
            return False


def main():
    """메인 실행"""
    pipeline = GovernmentDataPipeline()
    success = pipeline.run()
    
    if success:
        print("\n✅ 정부/공공기관 표준 데이터 처리 완료!")
        print("✅ 모든 데이터 검증 통과!")
    else:
        print("\n⚠️ 처리는 완료되었으나 일부 문제가 발견되었습니다.")
        print("visualization_government/processing_report.txt 파일을 확인하세요.")
    
    return success


if __name__ == "__main__":
    success = main()