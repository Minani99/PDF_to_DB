"""
전체 시스템 통합 테스트
PDF → JSON → 정규화 → DB 전 과정 검증
"""
import os
import sys
import json
from pathlib import Path
import logging
from typing import Dict, Any, List

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SystemValidator:
    """시스템 검증 클래스"""
    
    def __init__(self):
        self.test_results = {
            'pdf_extraction': False,
            'json_structure': False,
            'normalization': False,
            'data_completeness': False,
            'quarter_split': False,
            'performance_extraction': False,
            'budget_separation': False,
            'db_ready': False,
            'errors': []
        }
    
    def run_all_tests(self) -> bool:
        """모든 테스트 실행"""
        logger.info("="*80)
        logger.info("🧪 전체 시스템 통합 테스트 시작")
        logger.info("="*80)
        
        # 1. PDF 추출 테스트
        self._test_pdf_extraction()
        
        # 2. JSON 구조 검증
        self._test_json_structure()
        
        # 3. 정규화 테스트
        self._test_normalization()
        
        # 4. 데이터 완전성 검증
        self._test_data_completeness()
        
        # 5. 결과 출력
        self._print_results()
        
        # 모든 테스트 통과 여부
        return all([
            self.test_results['pdf_extraction'],
            self.test_results['json_structure'],
            self.test_results['normalization'],
            self.test_results['data_completeness']
        ])
    
    def _test_pdf_extraction(self):
        """PDF 추출 테스트"""
        logger.info("\n📄 TEST 1: PDF 추출 기능")
        
        try:
            from extract_pdf_to_json import extract_pdf_to_json
            
            # 샘플 데이터로 테스트
            result = extract_pdf_to_json(None, "output")
            
            if result and 'pages' in result:
                self.test_results['pdf_extraction'] = True
                logger.info("  ✅ PDF 추출 성공")
                logger.info(f"     - 페이지: {len(result['pages'])}개")
                
                # 테이블 수 확인
                total_tables = sum(
                    len(page.get('tables', [])) 
                    for page in result['pages']
                )
                logger.info(f"     - 테이블: {total_tables}개")
            else:
                logger.error("  ❌ PDF 추출 실패")
                self.test_results['errors'].append("PDF 추출 실패")
                
        except Exception as e:
            logger.error(f"  ❌ PDF 추출 오류: {e}")
            self.test_results['errors'].append(f"PDF 추출: {str(e)}")
    
    def _test_json_structure(self):
        """JSON 구조 검증"""
        logger.info("\n📋 TEST 2: JSON 구조 검증")
        
        try:
            json_file = Path("output/extracted_data.json")
            
            if json_file.exists():
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # 필수 필드 확인
                required_fields = ['metadata', 'pages']
                if all(field in data for field in required_fields):
                    self.test_results['json_structure'] = True
                    logger.info("  ✅ JSON 구조 정상")
                    
                    # 카테고리 확인
                    categories = set()
                    for page in data.get('pages', []):
                        if page.get('category'):
                            categories.add(page['category'])
                    
                    logger.info(f"     - 카테고리: {categories}")
                    
                    # 내역사업 확인
                    sub_projects = set()
                    for page in data.get('pages', []):
                        if page.get('sub_project'):
                            sub_projects.add(page['sub_project'])
                    
                    if sub_projects:
                        logger.info(f"     - 내역사업: {sub_projects}")
                else:
                    logger.error("  ❌ JSON 필수 필드 누락")
                    self.test_results['errors'].append("JSON 구조 불완전")
            else:
                logger.warning("  ⚠️ JSON 파일 없음 - 샘플 데이터 생성 필요")
                
        except Exception as e:
            logger.error(f"  ❌ JSON 검증 오류: {e}")
            self.test_results['errors'].append(f"JSON 검증: {str(e)}")
    
    def _test_normalization(self):
        """정규화 테스트"""
        logger.info("\n🔄 TEST 3: 데이터 정규화")
        
        try:
            from normalize_government_standard import GovernmentStandardNormalizer
            
            # JSON 로드
            json_file = Path("output/extracted_data.json")
            if json_file.exists():
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
            else:
                # 샘플 데이터 사용
                from extract_pdf_to_json import extract_pdf_to_json
                json_data = extract_pdf_to_json(None, "output")
            
            # 정규화 실행
            normalizer = GovernmentStandardNormalizer(
                str(json_file),
                "normalized_output_government"
            )
            
            success = normalizer.normalize(json_data)
            
            if success:
                normalizer.save_to_csv()
                self.test_results['normalization'] = True
                logger.info("  ✅ 정규화 성공")
                
                # 분기 분리 확인
                schedules = normalizer.data.get('normalized_schedules', [])
                quarter_split_count = 0
                for schedule in schedules:
                    if '~' in schedule.get('original_period', ''):
                        quarter_split_count += 1
                
                if quarter_split_count > 0:
                    self.test_results['quarter_split'] = True
                    logger.info(f"  ✅ 분기 분리: {quarter_split_count}개 일정 분리됨")
                
                # 성과 데이터 확인
                performances = normalizer.data.get('normalized_performances', [])
                if len(performances) > 10:  # 충분한 성과 데이터
                    self.test_results['performance_extraction'] = True
                    logger.info(f"  ✅ 성과 추출: {len(performances)}개 지표")
                    
                    # 카테고리별 집계
                    categories = {}
                    for perf in performances:
                        cat = perf.get('indicator_category', 'Unknown')
                        if cat not in categories:
                            categories[cat] = 0
                        categories[cat] += 1
                    logger.info(f"     - 카테고리: {categories}")
                
                # 예산 데이터 확인
                budgets = normalizer.data.get('normalized_budgets', [])
                if len(budgets) > 10:  # 충분한 예산 데이터
                    self.test_results['budget_separation'] = True
                    logger.info(f"  ✅ 예산 분리: {len(budgets)}개 레코드")
                    
                    # 연도별 집계
                    years = set(b.get('budget_year') for b in budgets)
                    logger.info(f"     - 연도: {sorted(years)}")
                    
                    # 유형별 집계
                    types = set(b.get('budget_type') for b in budgets)
                    logger.info(f"     - 유형: {types}")
            else:
                logger.error("  ❌ 정규화 실패")
                self.test_results['errors'].append("정규화 실패")
                
        except Exception as e:
            logger.error(f"  ❌ 정규화 오류: {e}")
            self.test_results['errors'].append(f"정규화: {str(e)}")
    
    def _test_data_completeness(self):
        """데이터 완전성 테스트"""
        logger.info("\n✅ TEST 4: 데이터 완전성 검증")
        
        try:
            csv_dir = Path("normalized_output_government")
            
            required_files = [
                'sub_projects.csv',
                'normalized_schedules.csv',
                'normalized_performances.csv',
                'normalized_budgets.csv',
                'raw_data.csv'
            ]
            
            missing_files = []
            for file_name in required_files:
                file_path = csv_dir / file_name
                if not file_path.exists():
                    missing_files.append(file_name)
                else:
                    # 파일 크기 확인
                    size = file_path.stat().st_size
                    if size > 100:  # 최소 100 바이트
                        logger.info(f"  ✅ {file_name}: {size:,} bytes")
                    else:
                        logger.warning(f"  ⚠️ {file_name}: 데이터 부족 ({size} bytes)")
            
            if not missing_files:
                self.test_results['data_completeness'] = True
                logger.info("  ✅ 모든 필수 파일 생성됨")
            else:
                logger.error(f"  ❌ 누락된 파일: {missing_files}")
                self.test_results['errors'].append(f"누락 파일: {missing_files}")
            
            # DB 준비 상태 확인
            try:
                import pymysql
                self.test_results['db_ready'] = True
                logger.info("  ✅ DB 모듈 준비됨")
            except ImportError:
                logger.warning("  ⚠️ pymysql 미설치 - DB 적재 불가")
                
        except Exception as e:
            logger.error(f"  ❌ 완전성 검증 오류: {e}")
            self.test_results['errors'].append(f"완전성 검증: {str(e)}")
    
    def _print_results(self):
        """테스트 결과 출력"""
        print("\n" + "="*80)
        print("📊 시스템 테스트 결과")
        print("="*80)
        
        # 점수 계산
        total_tests = 8
        passed_tests = sum([
            self.test_results['pdf_extraction'],
            self.test_results['json_structure'],
            self.test_results['normalization'],
            self.test_results['data_completeness'],
            self.test_results['quarter_split'],
            self.test_results['performance_extraction'],
            self.test_results['budget_separation'],
            self.test_results['db_ready']
        ])
        
        score = (passed_tests / total_tests) * 100
        
        print(f"\n📈 점수: {score:.1f}% ({passed_tests}/{total_tests})")
        print("\n📋 세부 결과:")
        
        results = [
            ('PDF 추출', self.test_results['pdf_extraction']),
            ('JSON 구조', self.test_results['json_structure']),
            ('데이터 정규화', self.test_results['normalization']),
            ('데이터 완전성', self.test_results['data_completeness']),
            ('분기 분리', self.test_results['quarter_split']),
            ('성과 추출', self.test_results['performance_extraction']),
            ('예산 분리', self.test_results['budget_separation']),
            ('DB 준비', self.test_results['db_ready'])
        ]
        
        for name, passed in results:
            status = '✅' if passed else '❌'
            print(f"  {status} {name}")
        
        if self.test_results['errors']:
            print("\n❌ 오류 목록:")
            for error in self.test_results['errors']:
                print(f"  - {error}")
        
        print("\n" + "="*80)
        
        if score >= 90:
            print("🎉 시스템 준비 완료! 프로덕션 사용 가능")
        elif score >= 70:
            print("⚠️ 일부 기능 개선 필요")
        else:
            print("❌ 주요 문제 해결 필요")
        
        print("="*80)


def main():
    """메인 실행"""
    validator = SystemValidator()
    success = validator.run_all_tests()
    
    # 빠른 테스트 실행
    if success:
        print("\n🚀 빠른 파이프라인 테스트:")
        print("python main_complete_pipeline.py --skip-db")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())