"""
엔터프라이즈 정부사업 데이터 처리 시스템
메인 실행 파일
"""
import sys
from pathlib import Path
from normalize_enterprise import normalize_enterprise
from load_enterprise_db import load_enterprise_db
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """메인 실행 함수"""
    print("\n" + "="*80)
    print("🏢 엔터프라이즈 정부사업 데이터 처리 시스템")
    print("="*80)
    
    # 경로 설정
    json_path = "output/extracted_data.json"  # JSON 파일이 이미 추출되어 있다고 가정
    csv_output_dir = "normalized_enterprise"
    
    # JSON 파일 확인
    if not Path(json_path).exists():
        logger.error(f"❌ JSON 파일을 찾을 수 없습니다: {json_path}")
        logger.info("PDF를 먼저 JSON으로 변환해주세요.")
        return False
    
    try:
        # 1단계: 정규화
        print("\n📊 1단계: 데이터 정규화")
        print("-" * 40)
        
        if not normalize_enterprise(json_path, csv_output_dir):
            logger.error("❌ 정규화 실패")
            return False
        
        print("✅ 정규화 완료!")
        
        # 2단계: DB 적재
        print("\n💾 2단계: 데이터베이스 적재")
        print("-" * 40)
        
        if not load_enterprise_db(csv_output_dir):
            logger.error("❌ DB 적재 실패")
            return False
        
        print("✅ DB 적재 완료!")
        
        # 완료
        print("\n" + "="*80)
        print("🎉 모든 처리가 완료되었습니다!")
        print("="*80)
        
        print("\n📋 다음 SQL로 데이터를 확인할 수 있습니다:")
        print("""
-- 내역사업 목록 조회
SELECT * FROM sub_projects;

-- 특정 내역사업의 2023년 실적 조회
SELECT 
    sp.sub_project_name AS 내역사업명,
    pp.domestic_application AS 특허_국내출원,
    pp.domestic_registration AS 특허_국내등록,
    pap.scie_total AS 논문_SCIE
FROM sub_projects sp
JOIN performance_master pm ON sp.id = pm.sub_project_id
LEFT JOIN performance_patents pp ON pm.id = pp.performance_id
LEFT JOIN performance_papers pap ON pm.id = pap.performance_id
WHERE pm.performance_year = 2023;

-- 2024년 예산 계획 조회
SELECT 
    sp.sub_project_name AS 내역사업명,
    pb.budget_type AS 예산구분,
    pb.planned_amount AS 계획금액
FROM sub_projects sp
JOIN plan_master pm ON sp.id = pm.sub_project_id
JOIN plan_budgets pb ON pm.id = pb.plan_id
WHERE pb.budget_year = 2024;
        """)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 처리 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)