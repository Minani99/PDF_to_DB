"""
완전한 PDF → JSON → 정규화 → DB 파이프라인
정부/공공기관 표준 데이터 처리 시스템
"""
import os
import sys
import json
from pathlib import Path
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# 모듈 임포트
from extract_pdf_to_json import extract_pdf_to_json
from normalize_government_standard import GovernmentStandardNormalizer
from load_government_standard_db import GovernmentStandardDBLoader
from config import MYSQL_CONFIG

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CompletePipeline:
    """PDF부터 DB까지 완전한 파이프라인"""
    
    def __init__(self, pdf_path: str = None, skip_db: bool = False):
        """
        Args:
            pdf_path: PDF 파일 경로 (None이면 샘플 데이터)
            skip_db: DB 적재 건너뛰기 (테스트용)
        """
        self.pdf_path = Path(pdf_path) if pdf_path else None
        self.skip_db = skip_db
        
        # 디렉토리 설정
        self.input_dir = Path("input")
        self.json_dir = Path("output")
        self.csv_dir = Path("normalized_output_government")
        self.viz_dir = Path("visualization_government")
        
        # 디렉토리 생성
        for dir_path in [self.input_dir, self.json_dir, self.csv_dir, self.viz_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # 파이프라인 통계
        self.stats = {
            'start_time': datetime.now(),
            'pdf_extracted': False,
            'json_created': False,
            'normalized': False,
            'db_loaded': False,
            'total_records': 0,
            'errors': []
        }
    
    def run(self) -> bool:
        """전체 파이프라인 실행"""
        logger.info("="*80)
        logger.info("🚀 완전한 데이터 처리 파이프라인 시작")
        logger.info("="*80)
        
        try:
            # 1단계: PDF → JSON
            json_data = self._extract_pdf()
            if not json_data:
                return False
            
            # 2단계: JSON → 정규화
            normalized = self._normalize_data(json_data)
            if not normalized:
                return False
            
            # 3단계: 정규화 데이터 → DB (옵션)
            if not self.skip_db:
                db_loaded = self._load_to_database()
                if not db_loaded:
                    logger.warning("DB 적재 실패했지만 정규화는 완료됨")
            
            # 4단계: 검증 및 보고서
            self._generate_report()
            
            logger.info("✅ 파이프라인 완료!")
            return True
            
        except Exception as e:
            logger.error(f"파이프라인 실패: {e}")
            self.stats['errors'].append(str(e))
            return False
        
        finally:
            self._print_summary()
    
    def _extract_pdf(self) -> Optional[Dict[str, Any]]:
        """1단계: PDF 추출"""
        logger.info("\n" + "="*60)
        logger.info("📄 1단계: PDF → JSON 변환")
        logger.info("="*60)
        
        try:
            # PDF 추출
            if self.pdf_path and self.pdf_path.exists():
                logger.info(f"PDF 파일: {self.pdf_path}")
                json_data = extract_pdf_to_json(str(self.pdf_path), str(self.json_dir))
            else:
                logger.info("샘플 데이터 모드 사용")
                json_data = extract_pdf_to_json(None, str(self.json_dir))
            
            if json_data:
                self.stats['pdf_extracted'] = True
                self.stats['json_created'] = True
                
                # JSON 파일 저장
                json_file = self.json_dir / "extracted_data.json"
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"✅ JSON 생성: {json_file}")
                logger.info(f"   - 페이지: {len(json_data.get('pages', []))}개")
                
                # 테이블 수 계산
                total_tables = sum(
                    len(page.get('tables', [])) 
                    for page in json_data.get('pages', [])
                )
                logger.info(f"   - 테이블: {total_tables}개")
                
                return json_data
                
        except Exception as e:
            logger.error(f"PDF 추출 실패: {e}")
            self.stats['errors'].append(f"PDF 추출: {str(e)}")
            
        return None
    
    def _normalize_data(self, json_data: Dict[str, Any]) -> bool:
        """2단계: 데이터 정규화"""
        logger.info("\n" + "="*60)
        logger.info("🔄 2단계: 데이터 정규화")
        logger.info("="*60)
        
        try:
            # JSON 파일 경로
            json_file = self.json_dir / "extracted_data.json"
            
            # 정규화 실행
            normalizer = GovernmentStandardNormalizer(
                str(json_file),
                str(self.csv_dir)
            )
            
            # 정규화 처리
            success = normalizer.normalize(json_data)
            
            if success:
                # CSV 저장
                normalizer.save_to_csv()
                
                # 통계 출력
                normalizer.print_statistics()
                
                # 검증
                validation = normalizer.validate_data()
                
                self.stats['normalized'] = True
                self.stats['total_records'] = sum(
                    len(records) for records in normalizer.data.values()
                    if isinstance(records, list)
                )
                
                logger.info(f"✅ 정규화 완료:")
                logger.info(f"   - 내역사업: {len(normalizer.data.get('sub_projects', []))}개")
                logger.info(f"   - 일정: {len(normalizer.data.get('normalized_schedules', []))}건")
                logger.info(f"   - 성과: {len(normalizer.data.get('normalized_performances', []))}건")
                logger.info(f"   - 예산: {len(normalizer.data.get('normalized_budgets', []))}건")
                
                if validation.get('issues'):
                    logger.warning(f"   ⚠️ 검증 이슈: {len(validation['issues'])}개")
                else:
                    logger.info(f"   ✓ 데이터 검증 통과")
                
                return True
                
        except Exception as e:
            logger.error(f"정규화 실패: {e}")
            self.stats['errors'].append(f"정규화: {str(e)}")
            
        return False
    
    def _load_to_database(self) -> bool:
        """3단계: DB 적재"""
        logger.info("\n" + "="*60)
        logger.info("💾 3단계: 데이터베이스 적재")
        logger.info("="*60)
        
        try:
            # DB 설정
            db_config = MYSQL_CONFIG.copy()
            db_config['database'] = 'government_standard'
            
            # 적재 실행
            loader = GovernmentStandardDBLoader(db_config, str(self.csv_dir))
            
            # 연결
            loader.connect()
            
            # 기존 테이블 삭제
            loader.drop_existing_tables()
            
            # 테이블 생성
            loader.create_tables()
            
            # 데이터 적재
            loader.load_all_tables()
            
            # 검증
            verification = loader.verify_data_integrity()
            
            loader.close()
            
            self.stats['db_loaded'] = True
            
            logger.info(f"✅ DB 적재 완료:")
            logger.info(f"   - 테이블: {loader.load_stats['tables_created']}개")
            logger.info(f"   - 레코드: {loader.load_stats['total_records']:,}건")
            
            if verification.get('orphan_records'):
                logger.warning(f"   ⚠️ 고아 레코드: {len(verification['orphan_records'])}개")
            else:
                logger.info(f"   ✓ 무결성 검증 통과")
            
            return True
            
        except Exception as e:
            logger.error(f"DB 적재 실패: {e}")
            self.stats['errors'].append(f"DB 적재: {str(e)}")
            
        return False
    
    def _generate_report(self):
        """보고서 생성"""
        logger.info("\n" + "="*60)
        logger.info("📊 4단계: 보고서 생성")
        logger.info("="*60)
        
        report = []
        report.append("="*80)
        report.append("📊 데이터 처리 파이프라인 실행 보고서")
        report.append("="*80)
        report.append(f"실행 시간: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"소요 시간: {(datetime.now() - self.stats['start_time']).total_seconds():.1f}초")
        report.append("")
        
        # 단계별 상태
        report.append("📋 처리 단계:")
        report.append(f"  1. PDF 추출: {'✅' if self.stats['pdf_extracted'] else '❌'}")
        report.append(f"  2. JSON 생성: {'✅' if self.stats['json_created'] else '❌'}")
        report.append(f"  3. 정규화: {'✅' if self.stats['normalized'] else '❌'}")
        report.append(f"  4. DB 적재: {'✅' if self.stats['db_loaded'] else '⏭️ 건너뜀'}")
        report.append("")
        
        # 데이터 통계
        report.append("📊 데이터 통계:")
        report.append(f"  총 레코드: {self.stats['total_records']:,}건")
        report.append("")
        
        # 오류
        if self.stats['errors']:
            report.append("❌ 오류:")
            for error in self.stats['errors']:
                report.append(f"  - {error}")
            report.append("")
        
        # 생성된 파일
        report.append("📁 생성된 파일:")
        report.append(f"  - JSON: output/extracted_data.json")
        report.append(f"  - CSV: normalized_output_government/*.csv")
        if self.stats['db_loaded']:
            report.append(f"  - DB: government_standard 데이터베이스")
        report.append("")
        
        report.append("="*80)
        
        # 보고서 저장
        report_text = "\n".join(report)
        report_file = self.viz_dir / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        logger.info(f"✅ 보고서 생성: {report_file}")
    
    def _print_summary(self):
        """요약 출력"""
        print("\n" + "="*80)
        print("📊 파이프라인 실행 요약")
        print("="*80)
        
        # 성공/실패 상태
        success_count = sum([
            self.stats['pdf_extracted'],
            self.stats['json_created'],
            self.stats['normalized'],
            self.stats['db_loaded']
        ])
        
        total_steps = 4 if not self.skip_db else 3
        
        print(f"✅ 성공: {success_count}/{total_steps} 단계")
        print(f"📊 처리 레코드: {self.stats['total_records']:,}건")
        print(f"⏱️ 소요 시간: {(datetime.now() - self.stats['start_time']).total_seconds():.1f}초")
        
        if self.stats['errors']:
            print(f"❌ 오류 발생: {len(self.stats['errors'])}개")
        
        print("="*80)


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="PDF → JSON → 정규화 → DB 완전한 파이프라인"
    )
    parser.add_argument(
        'pdf_file',
        nargs='?',
        help='처리할 PDF 파일 경로 (생략하면 샘플 데이터 사용)'
    )
    parser.add_argument(
        '--skip-db',
        action='store_true',
        help='DB 적재 건너뛰기 (테스트용)'
    )
    
    args = parser.parse_args()
    
    # 파이프라인 실행
    pipeline = CompletePipeline(
        pdf_path=args.pdf_file,
        skip_db=args.skip_db
    )
    
    success = pipeline.run()
    
    if success:
        print("\n✅ 파이프라인 성공적으로 완료!")
        print("📁 결과 확인:")
        print("  - JSON: output/extracted_data.json")
        print("  - CSV: normalized_output_government/*.csv")
        if not args.skip_db:
            print("  - DB: government_standard 데이터베이스")
    else:
        print("\n⚠️ 파이프라인 일부 실패. 보고서를 확인하세요.")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())