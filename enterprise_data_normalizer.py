"""
엔터프라이즈 데이터 정규화 전략
회사 표준에 맞는 데이터 분리 및 중복 저장
"""
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class EnterpriseDataNormalizer:
    """회사 표준 데이터 정규화"""
    
    def normalize_schedule(self, raw_schedule: Dict) -> List[Dict]:
        """
        일정 데이터 정규화
        1/4분기~2/4분기 → 1/4분기, 2/4분기로 분리
        """
        normalized = []
        
        period = raw_schedule.get('period', '')
        task = raw_schedule.get('task', '')
        
        # Case 1: 병합된 분기 (1/4분기 ~ 2/4분기)
        if '~' in period and '분기' in period:
            # "1/4분기 ~ 2/4분기" → ["1/4분기", "2/4분기"]
            import re
            match = re.search(r'(\d)/4\s*분기\s*~\s*(\d)/4\s*분기', period)
            if match:
                start_q = int(match.group(1))
                end_q = int(match.group(2))
                
                for quarter in range(start_q, end_q + 1):
                    normalized.append({
                        'schedule_type': '분기',
                        'schedule_period': f'{quarter}/4분기',
                        'quarter_num': quarter,
                        'year': 2024,
                        'start_date': f'2024-{(quarter-1)*3+1:02d}-01',
                        'end_date': f'2024-{quarter*3:02d}-31',
                        'task_description': task,
                        'original_period': period  # 원본 보존
                    })
        
        # Case 2: 연중
        elif '연중' in period:
            # 연중 → 4개 분기로 분리
            for quarter in range(1, 5):
                normalized.append({
                    'schedule_type': '분기',
                    'schedule_period': f'{quarter}/4분기',
                    'quarter_num': quarter,
                    'year': 2024,
                    'start_date': f'2024-{(quarter-1)*3+1:02d}-01',
                    'end_date': f'2024-{quarter*3:02d}-31',
                    'task_description': task,
                    'original_period': '연중'
                })
        
        # Case 3: 단일 분기
        else:
            normalized.append(raw_schedule)
        
        return normalized
    
    def normalize_budget(self, raw_budget: Dict) -> List[Dict]:
        """
        예산 데이터 정규화
        다년도 예산을 연도별로 분리
        """
        normalized = []
        
        # 예: "2021-2024년 총예산 300억" → 연도별 분리
        if 'multi_year' in raw_budget:
            total = raw_budget['total_amount']
            years = raw_budget['years']
            per_year = total / len(years)
            
            for year in years:
                normalized.append({
                    'budget_year': year,
                    'budget_type': raw_budget['budget_type'],
                    'amount': per_year,
                    'is_estimated': True,  # 추정치 표시
                    'original_total': total
                })
        else:
            normalized.append(raw_budget)
        
        return normalized
    
    def normalize_performance(self, raw_performance: Dict) -> List[Dict]:
        """
        성과 데이터 정규화
        통합 지표를 세부 지표로 분리
        """
        normalized = []
        
        # 예: "특허 100건(국내 70, 해외 30)" → 분리
        if 'combined_patents' in raw_performance:
            total = raw_performance['total_patents']
            
            # 국내 특허
            normalized.append({
                'indicator_type': '특허',
                'indicator_subtype': '국내',
                'value': raw_performance.get('domestic', 0),
                'year': raw_performance['year']
            })
            
            # 해외 특허
            normalized.append({
                'indicator_type': '특허',
                'indicator_subtype': '해외',
                'value': raw_performance.get('foreign', 0),
                'year': raw_performance['year']
            })
        
        return normalized if normalized else [raw_performance]


class DataWarehouseFormatter:
    """데이터 웨어하우스용 포맷터"""
    
    @staticmethod
    def to_fact_table(normalized_data: List[Dict]) -> List[Dict]:
        """
        팩트 테이블 형식으로 변환
        스타 스키마 구조
        """
        facts = []
        
        for record in normalized_data:
            fact = {
                # 차원 키
                'date_key': record.get('start_date', '').replace('-', ''),
                'project_key': record.get('project_id', 0),
                'category_key': record.get('category_id', 0),
                
                # 측정값
                'planned_amount': record.get('amount', 0),
                'actual_amount': 0,
                'variance': 0,
                
                # 메타데이터
                'created_at': datetime.now().isoformat(),
                'is_normalized': True
            }
            facts.append(fact)
        
        return facts
    
    @staticmethod
    def to_dimension_table(data: Dict, dim_type: str) -> Dict:
        """차원 테이블 생성"""
        if dim_type == 'date':
            return {
                'date_key': data['date_key'],
                'full_date': data['date'],
                'year': data['year'],
                'quarter': data['quarter'],
                'month': data['month'],
                'week': data['week'],
                'day_of_week': data['day_of_week'],
                'is_holiday': data.get('is_holiday', False)
            }
        elif dim_type == 'project':
            return {
                'project_key': data['project_key'],
                'project_code': data['code'],
                'project_name': data['name'],
                'department': data['department'],
                'category': data['category'],
                'status': data['status']
            }
        
        return data


# 실제 사용 예시
def example_usage():
    """회사 표준 정규화 예시"""
    
    normalizer = EnterpriseDataNormalizer()
    
    # 1. 일정 정규화
    raw_schedule = {
        'period': '1/4분기 ~ 2/4분기',
        'task': '신규과제 선정평가'
    }
    
    normalized_schedules = normalizer.normalize_schedule(raw_schedule)
    
    print("📅 일정 정규화:")
    print(f"원본: {raw_schedule['period']}")
    print(f"정규화 후: {len(normalized_schedules)}개 레코드")
    for s in normalized_schedules:
        print(f"  - {s['schedule_period']}: {s['start_date']} ~ {s['end_date']}")
    
    # 2. SQL 생성
    print("\n💾 SQL INSERT:")
    for s in normalized_schedules:
        sql = f"""
        INSERT INTO plan_schedules 
        (schedule_type, schedule_period, quarter_num, year, start_date, end_date, task_description)
        VALUES 
        ('{s['schedule_type']}', '{s['schedule_period']}', {s['quarter_num']}, 
         {s['year']}, '{s['start_date']}', '{s['end_date']}', '{s['task_description']}');
        """
        print(sql.strip())
    
    # 3. 데이터 웨어하우스 형식
    warehouse = DataWarehouseFormatter()
    facts = warehouse.to_fact_table(normalized_schedules)
    
    print("\n📊 팩트 테이블:")
    for fact in facts:
        print(f"  Date: {fact['date_key']}, Normalized: {fact['is_normalized']}")


# 회사별 정규화 정책
class NormalizationPolicy:
    """회사별 정규화 정책"""
    
    # 대기업 A사: 완전 정규화
    ENTERPRISE_A = {
        'split_periods': True,      # 기간 분리
        'split_multi_year': True,   # 다년도 분리
        'duplicate_for_bi': True,   # BI용 중복 허용
        'keep_original': True,       # 원본 보존
    }
    
    # 정부기관 B: 원본 유지 + 정규화 병행
    GOVERNMENT_B = {
        'split_periods': False,     # 원본 기간 유지
        'create_view': True,        # VIEW로 정규화
        'audit_required': True,     # 감사 추적
    }
    
    # 스타트업 C: 유연한 구조
    STARTUP_C = {
        'use_json': True,          # JSON 필드 활용
        'normalize_on_demand': True,  # 필요시 정규화
    }


if __name__ == "__main__":
    example_usage()
    
    print("\n" + "="*80)
    print("🏢 회사 정규화 정책 예시:")
    print("="*80)
    
    print("\n1. 대기업: 모든 데이터 정규화 + 중복 저장")
    print("   - 1/4~2/4분기 → 1/4분기, 2/4분기 (2개 레코드)")
    print("   - 연중 → 1,2,3,4분기 (4개 레코드)")
    print("   - 장점: 쿼리 단순, BI 툴 호환")
    print("   - 단점: 저장공간 증가")
    
    print("\n2. 정부기관: 원본 보존 + VIEW")
    print("   - 원본 테이블 + 정규화 VIEW")
    print("   - 장점: 원본 보존, 감사 추적")
    print("   - 단점: 관리 복잡도")
    
    print("\n3. 스타트업: JSON 활용")
    print("   - PostgreSQL JSON 필드")
    print("   - 장점: 유연성")
    print("   - 단점: 쿼리 복잡")