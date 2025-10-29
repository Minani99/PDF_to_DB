#!/usr/bin/env python3
"""
PDF to Database - 최종 통합 메인 프로그램
정부/공공기관 표준 데이터 처리 시스템

사용법:
    python main.py                    # input 폴더의 모든 PDF 처리
    python main.py document.pdf       # 특정 PDF 파일 처리
    python main.py --sample           # 샘플 데이터로 테스트
    python main.py --skip-db          # DB 적재 건너뛰기
"""

import os
import sys
import glob
import json
from pathlib import Path
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import argparse

# 모듈 임포트
from extract_pdf_to_json import extract_pdf_to_json
from normalize_government_standard import GovernmentStandardNormalizer
from load_government_standard_db import GovernmentStandardDBLoader
from config import MYSQL_CONFIG

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PDFtoDBPipeline:
    """PDF to Database 완전한 파이프라인"""
    
    def __init__(self, skip_db: bool = False, use_sample: bool = False):
        """
        Args:
            skip_db: DB 적재 건너뛰기
            use_sample: 샘플 데이터 사용
        """
        self.skip_db = skip_db
        self.use_sample = use_sample
        
        # 디렉토리 설정
        self.input_dir = Path("input")
        self.output_dir = Path("output")
        self.normalized_dir = Path("normalized_output_government")
        self.report_dir = Path("reports")
        
        # 디렉토리 생성
        for dir_path in [self.input_dir, self.output_dir, self.normalized_dir, self.report_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # 통계
        self.stats = {
            'start_time': datetime.now(),
            'pdf_files': [],
            'processed': 0,
            'failed': 0,
            'total_records': 0,
            'db_loaded': False
        }
    
    def process_pdf(self, pdf_path: Path) -> bool:
        """단일 PDF 처리"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"📄 처리 중: {pdf_path.name}")
            logger.info(f"{'='*60}")
            
            # 1. PDF → JSON
            logger.info("1️⃣ PDF → JSON 변환")
            json_data = extract_pdf_to_json(str(pdf_path), str(self.output_dir))
            
            if not json_data:
                logger.error("JSON 변환 실패")
                return False
            
            # JSON 파일 저장
            json_file = self.output_dir / f"{pdf_path.stem}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"   ✅ JSON 생성: {json_file.name}")
            
            # 2. JSON → 정규화
            logger.info("2️⃣ 데이터 정규화")
            normalizer = GovernmentStandardNormalizer(str(json_file), str(self.normalized_dir))
            
            if not normalizer.normalize(json_data):
                logger.error("정규화 실패")
                return False
            
            normalizer.save_to_csv()
            normalizer.print_statistics()
            
            # 통계 업데이트
            for table_name, records in normalizer.data.items():
                if isinstance(records, list):
                    self.stats['total_records'] += len(records)
            
            logger.info(f"   ✅ 정규화 완료")
            
            return True
            
        except Exception as e:
            logger.error(f"처리 실패: {e}")
            return False
    
    def process_sample(self) -> bool:
        """샘플 데이터 처리"""
        try:
            logger.info("\n" + "="*60)
            logger.info("🧪 샘플 데이터 모드")
            logger.info("="*60)
            
            # 1. 샘플 JSON 생성
            logger.info("1️⃣ 샘플 데이터 생성")
            json_data = extract_pdf_to_json(None, str(self.output_dir))
            
            # JSON 저장
            json_file = self.output_dir / "sample_data.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            # 2. 정규화
            logger.info("2️⃣ 데이터 정규화")
            normalizer = GovernmentStandardNormalizer(str(json_file), str(self.normalized_dir))
            normalizer.normalize(json_data)
            normalizer.save_to_csv()
            normalizer.print_statistics()
            
            # 통계
            for table_name, records in normalizer.data.items():
                if isinstance(records, list):
                    self.stats['total_records'] += len(records)
            
            return True
            
        except Exception as e:
            logger.error(f"샘플 처리 실패: {e}")
            return False
    
    def load_to_database(self) -> bool:
        """3단계: 데이터베이스 적재"""
        if self.skip_db:
            logger.info("\n⏭️ DB 적재 건너뜀")
            return True
        
        try:
            logger.info("\n" + "="*60)
            logger.info("3️⃣ 데이터베이스 적재")
            logger.info("="*60)
            
            # DB 설정
            db_config = MYSQL_CONFIG.copy()
            db_config['database'] = 'government_standard'
            
            # 적재
            loader = GovernmentStandardDBLoader(db_config, str(self.normalized_dir))
            loader.connect()
            loader.drop_existing_tables()
            loader.create_tables()
            loader.load_all_tables()
            
            # 검증
            verification = loader.verify_data_integrity()
            loader.close()
            
            self.stats['db_loaded'] = True
            logger.info(f"   ✅ DB 적재 완료: {loader.load_stats['total_records']:,}건")
            
            return True
            
        except Exception as e:
            logger.error(f"DB 적재 실패: {e}")
            return False
    
    def generate_report(self):
        """최종 보고서 생성"""
        report = []
        report.append("="*80)
        report.append("📊 PDF to Database 처리 보고서")
        report.append("="*80)
        report.append(f"실행 시간: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"소요 시간: {(datetime.now() - self.stats['start_time']).total_seconds():.1f}초")
        report.append("")
        
        if self.stats['pdf_files']:
            report.append("📄 처리된 파일:")
            for pdf in self.stats['pdf_files']:
                report.append(f"  - {pdf}")
        
        report.append("")
        report.append("📊 처리 결과:")
        report.append(f"  • 성공: {self.stats['processed']}개")
        report.append(f"  • 실패: {self.stats['failed']}개")
        report.append(f"  • 총 레코드: {self.stats['total_records']:,}건")
        report.append(f"  • DB 적재: {'✅' if self.stats['db_loaded'] else '⏭️ 건너뜀'}")
        report.append("")
        
        # 생성된 파일
        report.append("📁 생성된 파일:")
        report.append(f"  • JSON: {self.output_dir}/*.json")
        report.append(f"  • CSV: {self.normalized_dir}/*.csv")
        if self.stats['db_loaded']:
            report.append(f"  • DB: government_standard database")
        
        report.append("")
        report.append("="*80)
        
        # 보고서 저장
        report_text = "\n".join(report)
        report_file = self.report_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        # 콘솔 출력
        print("\n" + report_text)
        
        return report_file
    
    def run(self, pdf_files: List[str] = None):
        """파이프라인 실행"""
        logger.info("\n" + "="*80)
        logger.info("🚀 PDF to Database 파이프라인 시작")
        logger.info("="*80)
        
        success = False
        
        try:
            # 샘플 모드
            if self.use_sample:
                success = self.process_sample()
                self.stats['processed'] = 1 if success else 0
                self.stats['failed'] = 0 if success else 1
            
            # PDF 처리 모드
            else:
                # PDF 파일 찾기
                if pdf_files:
                    pdf_list = [Path(f) for f in pdf_files if Path(f).exists()]
                else:
                    # input 폴더에서 모든 PDF 찾기
                    pdf_list = list(self.input_dir.glob("*.pdf"))
                
                if not pdf_list:
                    logger.warning("PDF 파일이 없습니다. 샘플 데이터 모드로 전환...")
                    success = self.process_sample()
                    self.stats['processed'] = 1 if success else 0
                else:
                    # 각 PDF 처리
                    for pdf_path in pdf_list:
                        self.stats['pdf_files'].append(pdf_path.name)
                        
                        if self.process_pdf(pdf_path):
                            self.stats['processed'] += 1
                        else:
                            self.stats['failed'] += 1
                    
                    success = self.stats['processed'] > 0
            
            # DB 적재
            if success and not self.skip_db:
                self.load_to_database()
            
            # 보고서 생성
            report_file = self.generate_report()
            logger.info(f"\n📄 보고서 생성: {report_file}")
            
        except Exception as e:
            logger.error(f"파이프라인 오류: {e}")
            success = False
        
        # 완료 메시지
        if success:
            print("\n" + "="*80)
            print("✅ 파이프라인 성공적으로 완료!")
            print("="*80)
        else:
            print("\n" + "="*80)
            print("⚠️ 파이프라인 일부 실패")
            print("="*80)
        
        return success


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description="PDF to Database - 정부/공공기관 표준 데이터 처리",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python main.py                    # input 폴더의 모든 PDF 처리
  python main.py doc1.pdf doc2.pdf  # 특정 PDF 파일들 처리
  python main.py --sample           # 샘플 데이터로 테스트
  python main.py --skip-db          # DB 적재 건너뛰기
        """
    )
    
    parser.add_argument(
        'pdf_files',
        nargs='*',
        help='처리할 PDF 파일 경로 (생략하면 input 폴더 검색)'
    )
    
    parser.add_argument(
        '--sample',
        action='store_true',
        help='샘플 데이터 모드로 실행'
    )
    
    parser.add_argument(
        '--skip-db',
        action='store_true',
        help='데이터베이스 적재 건너뛰기'
    )
    
    args = parser.parse_args()
    
    # 파이프라인 실행
    pipeline = PDFtoDBPipeline(
        skip_db=args.skip_db,
        use_sample=args.sample
    )
    
    success = pipeline.run(args.pdf_files)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())