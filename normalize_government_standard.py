"""
정부/공공기관 표준 데이터 정규화 시스템
원본 보존 + 정규화 분리 저장
"""
import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from decimal import Decimal
import logging
from dataclasses import dataclass, field, asdict
from enum import Enum

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class SubProject:
    """내역사업"""
    id: int
    project_code: str
    department_name: str
    main_project_name: str
    sub_project_name: str
    document_year: int


@dataclass
class RawData:
    """원본 데이터 (감사 추적용)"""
    id: int
    sub_project_id: int
    data_type: str  # 'overview', 'performance', 'plan'
    data_year: int
    raw_content: str  # JSON 형태로 원본 저장
    page_number: int
    table_index: int


@dataclass 
class NormalizedSchedule:
    """정규화된 일정"""
    id: int
    sub_project_id: int
    raw_data_id: int
    year: int
    quarter: int
    month_start: int
    month_end: int
    start_date: str
    end_date: str
    task_category: str
    task_description: str
    original_period: str  # 원본 기간 표현


@dataclass
class NormalizedPerformance:
    """정규화된 성과"""
    id: int
    sub_project_id: int
    raw_data_id: int
    performance_year: int
    indicator_category: str  # '특허', '논문', '기술이전', '인력양성'
    indicator_type: str      # '국내출원', '국내등록', 'SCIE', 'IF10이상' 등
    value: int
    unit: str
    original_text: str


@dataclass
class NormalizedBudget:
    """정규화된 예산"""
    id: int
    sub_project_id: int
    raw_data_id: int
    budget_year: int
    budget_category: str  # '계획', '실적'
    budget_type: str      # '정부', '민간', '지방비', '기타'
    amount: Decimal
    currency: str
    is_actual: bool      # 실적 여부
    original_text: str


class GovernmentStandardNormalizer:
    """정부 표준 정규화 클래스"""
    
    def __init__(self, json_path: str, output_dir: str):
        self.json_path = Path(json_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # ID 카운터
        self.id_counters = {
            'sub_project': 1,
            'raw_data': 1,
            'schedule': 1,
            'performance': 1,
            'budget': 1,
            'overview': 1
        }
        
        # 데이터 저장소
        self.data = {
            # 마스터
            'sub_projects': [],
            
            # 원본 데이터 (감사용)
            'raw_data': [],
            'raw_overviews': [],
            'raw_performances': [],
            'raw_plans': [],
            
            # 정규화 데이터 (분석용)
            'normalized_schedules': [],
            'normalized_performances': [],
            'normalized_budgets': [],
            'normalized_overviews': [],
            
            # 통계 (검증용)
            'data_statistics': []
        }
        
        # 컨텍스트
        self.current_context = {
            'sub_project_id': None,
            'document_year': 2024,
            'performance_year': 2023,
            'plan_year': 2024
        }
        
        # 캐시
        self.project_cache = {}
        
        # 검증 통계
        self.validation_stats = {
            'total_tables': 0,
            'processed_tables': 0,
            'normalized_records': 0,
            'errors': []
        }
    
    def _get_next_id(self, entity_type: str) -> int:
        """ID 생성"""
        current = self.id_counters[entity_type]
        self.id_counters[entity_type] += 1
        return current
    
    def _save_raw_data(self, data_type: str, content: Any, 
                      page_number: int, table_index: int) -> int:
        """원본 데이터 저장"""
        raw_id = self._get_next_id('raw_data')
        
        self.data['raw_data'].append({
            'id': raw_id,
            'sub_project_id': self.current_context['sub_project_id'],
            'data_type': data_type,
            'data_year': self.current_context.get(f'{data_type}_year', 
                                                 self.current_context['document_year']),
            'raw_content': json.dumps(content, ensure_ascii=False),
            'page_number': page_number,
            'table_index': table_index,
            'created_at': datetime.now().isoformat()
        })
        
        return raw_id
    
    def _normalize_schedule_data(self, period: str, task: str, detail: str, 
                                raw_data_id: int) -> List[Dict]:
        """일정 데이터 정규화 - 분기별로 분리"""
        normalized = []
        year = self.current_context['plan_year']
        
        # 작업 카테고리 추출
        task_category = ""
        if '-' in task:
            parts = task.split('-', 1)
            task_category = parts[0].strip()
        
        # Case 1: 병합된 분기 (1/4분기 ~ 3/4분기)
        if '~' in period and '분기' in period:
            quarter_match = re.search(r'(\d)/4\s*분기\s*~\s*(\d)/4\s*분기', period)
            if quarter_match:
                start_q = int(quarter_match.group(1))
                end_q = int(quarter_match.group(2))
                
                for quarter in range(start_q, end_q + 1):
                    record = {
                        'id': self._get_next_id('schedule'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'year': year,
                        'quarter': quarter,
                        'month_start': (quarter - 1) * 3 + 1,
                        'month_end': quarter * 3,
                        'start_date': f"{year}-{(quarter-1)*3+1:02d}-01",
                        'end_date': f"{year}-{quarter*3:02d}-31" if quarter*3 != 6 else f"{year}-06-30",
                        'task_category': task_category,
                        'task_description': task,
                        'original_period': period
                    }
                    normalized.append(record)
                    
        # Case 2: 연중
        elif '연중' in period:
            for quarter in range(1, 5):
                record = {
                    'id': self._get_next_id('schedule'),
                    'sub_project_id': self.current_context['sub_project_id'],
                    'raw_data_id': raw_data_id,
                    'year': year,
                    'quarter': quarter,
                    'month_start': (quarter - 1) * 3 + 1,
                    'month_end': quarter * 3,
                    'start_date': f"{year}-{(quarter-1)*3+1:02d}-01",
                    'end_date': f"{year}-{quarter*3:02d}-31" if quarter*3 != 6 else f"{year}-06-30",
                    'task_category': task_category,
                    'task_description': task,
                    'original_period': '연중'
                }
                normalized.append(record)
                
        # Case 3: 단일 분기
        elif '분기' in period:
            quarter_match = re.search(r'(\d)/4\s*분기', period)
            if quarter_match:
                quarter = int(quarter_match.group(1))
                record = {
                    'id': self._get_next_id('schedule'),
                    'sub_project_id': self.current_context['sub_project_id'],
                    'raw_data_id': raw_data_id,
                    'year': year,
                    'quarter': quarter,
                    'month_start': (quarter - 1) * 3 + 1,
                    'month_end': quarter * 3,
                    'start_date': f"{year}-{(quarter-1)*3+1:02d}-01",
                    'end_date': f"{year}-{quarter*3:02d}-31" if quarter*3 != 6 else f"{year}-06-30",
                    'task_category': task_category,
                    'task_description': task,
                    'original_period': period
                }
                normalized.append(record)
        
        # Case 4: 월 단위
        else:
            # 기본값으로 하나의 레코드 생성
            record = {
                'id': self._get_next_id('schedule'),
                'sub_project_id': self.current_context['sub_project_id'],
                'raw_data_id': raw_data_id,
                'year': year,
                'quarter': 0,  # 분기 미정
                'month_start': 1,
                'month_end': 12,
                'start_date': f"{year}-01-01",
                'end_date': f"{year}-12-31",
                'task_category': task_category,
                'task_description': task,
                'original_period': period
            }
            normalized.append(record)
        
        return normalized
    
    def _normalize_performance_data(self, rows: List[List], raw_data_id: int) -> List[Dict]:
        """성과 데이터 정규화 - 지표별로 분리"""
        normalized = []
        year = self.current_context['performance_year']
        
        # 특허 데이터 찾기
        for row in rows:
            row_text = ' '.join(str(cell) for cell in row).lower()
            
            # 특허 데이터 패턴: "1,001 125 74 10"
            if any(keyword in row_text for keyword in ['특허', '출원', '등록']):
                numbers = []
                for cell in row:
                    try:
                        num = int(str(cell).replace(',', '').strip())
                        if num > 0:
                            numbers.append(num)
                    except:
                        pass
                
                if len(numbers) >= 4:
                    # 국내출원
                    normalized.append({
                        'id': self._get_next_id('performance'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'performance_year': year,
                        'indicator_category': '특허',
                        'indicator_type': '국내출원',
                        'value': numbers[0],
                        'unit': '건',
                        'original_text': str(row)
                    })
                    
                    # 국내등록
                    normalized.append({
                        'id': self._get_next_id('performance'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'performance_year': year,
                        'indicator_category': '특허',
                        'indicator_type': '국내등록',
                        'value': numbers[1],
                        'unit': '건',
                        'original_text': str(row)
                    })
                    
                    # 국외출원
                    normalized.append({
                        'id': self._get_next_id('performance'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'performance_year': year,
                        'indicator_category': '특허',
                        'indicator_type': '국외출원',
                        'value': numbers[2],
                        'unit': '건',
                        'original_text': str(row)
                    })
                    
                    # 국외등록
                    normalized.append({
                        'id': self._get_next_id('performance'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'performance_year': year,
                        'indicator_category': '특허',
                        'indicator_type': '국외등록',
                        'value': numbers[3],
                        'unit': '건',
                        'original_text': str(row)
                    })
            
            # 논문 데이터
            elif any(keyword in row_text for keyword in ['논문', 'scie', 'if']):
                numbers = []
                for cell in row:
                    try:
                        num = int(str(cell).replace(',', '').strip())
                        if num > 0:
                            numbers.append(num)
                    except:
                        pass
                
                if numbers:
                    # SCIE 논문
                    if len(numbers) > 2:
                        normalized.append({
                            'id': self._get_next_id('performance'),
                            'sub_project_id': self.current_context['sub_project_id'],
                            'raw_data_id': raw_data_id,
                            'performance_year': year,
                            'indicator_category': '논문',
                            'indicator_type': 'SCIE',
                            'value': max(numbers[2:4]) if len(numbers) > 3 else numbers[-1],
                            'unit': '편',
                            'original_text': str(row)
                        })
                    
                    # IF 10이상
                    if len(numbers) > 1:
                        normalized.append({
                            'id': self._get_next_id('performance'),
                            'sub_project_id': self.current_context['sub_project_id'],
                            'raw_data_id': raw_data_id,
                            'performance_year': year,
                            'indicator_category': '논문',
                            'indicator_type': 'IF10이상',
                            'value': numbers[1] if numbers[1] < 500 else numbers[0],
                            'unit': '편',
                            'original_text': str(row)
                        })
            
            # 인력양성
            elif any(keyword in row_text for keyword in ['박사', '석사', '인력']):
                numbers = []
                for cell in row:
                    try:
                        num = int(str(cell).replace(',', '').strip())
                        if num > 0:
                            numbers.append(num)
                    except:
                        pass
                
                if numbers:
                    if '박사' in row_text:
                        normalized.append({
                            'id': self._get_next_id('performance'),
                            'sub_project_id': self.current_context['sub_project_id'],
                            'raw_data_id': raw_data_id,
                            'performance_year': year,
                            'indicator_category': '인력양성',
                            'indicator_type': '박사배출',
                            'value': numbers[0],
                            'unit': '명',
                            'original_text': str(row)
                        })
                    
                    if '석사' in row_text:
                        value = numbers[1] if len(numbers) > 1 else numbers[0]
                        normalized.append({
                            'id': self._get_next_id('performance'),
                            'sub_project_id': self.current_context['sub_project_id'],
                            'raw_data_id': raw_data_id,
                            'performance_year': year,
                            'indicator_category': '인력양성',
                            'indicator_type': '석사배출',
                            'value': value,
                            'unit': '명',
                            'original_text': str(row)
                        })
        
        return normalized
    
    def _normalize_budget_data(self, rows: List[List], raw_data_id: int) -> List[Dict]:
        """예산 데이터 정규화 - 연도별/유형별 분리"""
        normalized = []
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            # 연도 추출
            year_text = str(row[0])
            year_match = re.search(r'(\d{4})', year_text)
            if not year_match:
                continue
            
            budget_year = int(year_match.group(1))
            
            # 예산 금액 추출
            for i, cell in enumerate(row[1:], 1):
                try:
                    amount = float(str(cell).replace(',', '').strip())
                    if amount <= 0:
                        continue
                    
                    # 예산 타입 결정
                    budget_type = '정부'  # 기본값
                    if i == 2:
                        budget_type = '민간'
                    elif i == 3:
                        budget_type = '지방비'
                    elif i == 4:
                        budget_type = '기타'
                    
                    # 실적/계획 구분
                    is_actual = budget_year < self.current_context['plan_year']
                    
                    record = {
                        'id': self._get_next_id('budget'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'budget_year': budget_year,
                        'budget_category': '실적' if is_actual else '계획',
                        'budget_type': budget_type,
                        'amount': amount,
                        'currency': 'KRW',
                        'is_actual': is_actual,
                        'original_text': str(row)
                    }
                    normalized.append(record)
                    
                except (ValueError, TypeError):
                    continue
        
        return normalized
    
    def _process_table(self, table: Dict, page_number: int, table_index: int, 
                      category: str) -> bool:
        """테이블 처리"""
        rows = table.get('data', [])
        if not rows:
            return False
        
        self.validation_stats['total_tables'] += 1
        
        # 원본 저장
        raw_data_id = self._save_raw_data(category, table, page_number, table_index)
        
        # 테이블 타입 감지
        table_type = self._detect_table_type(rows)
        
        # 카테고리별 처리
        if category == 'overview':
            # 사업개요는 원본 형태로 저장
            self._process_overview(rows, raw_data_id)
            
        elif category == 'performance':
            # 성과 데이터 정규화
            normalized = self._normalize_performance_data(rows, raw_data_id)
            self.data['normalized_performances'].extend(normalized)
            self.validation_stats['normalized_records'] += len(normalized)
            
        elif category == 'plan':
            # 계획 데이터 처리
            if '일정' in table_type or '분기' in table_type:
                # 일정 정규화
                for row in rows[1:]:
                    if len(row) >= 2:
                        period = str(row[0]).strip()
                        task = str(row[1]).strip() if len(row) > 1 else ""
                        detail = str(row[2]).strip() if len(row) > 2 else ""
                        
                        if period and not '구분' in period:
                            normalized = self._normalize_schedule_data(
                                period, task, detail, raw_data_id
                            )
                            self.data['normalized_schedules'].extend(normalized)
                            self.validation_stats['normalized_records'] += len(normalized)
                            
            elif '예산' in table_type or '사업비' in table_type:
                # 예산 정규화
                normalized = self._normalize_budget_data(rows, raw_data_id)
                self.data['normalized_budgets'].extend(normalized)
                self.validation_stats['normalized_records'] += len(normalized)
        
        self.validation_stats['processed_tables'] += 1
        return True
    
    def _detect_table_type(self, rows: List[List]) -> str:
        """테이블 타입 감지"""
        if not rows:
            return "unknown"
        
        headers = ' '.join(str(h) for h in rows[0]).lower()
        first_cols = ' '.join(str(row[0]) for row in rows[:3] if row).lower()
        combined = headers + ' ' + first_cols
        
        if '내역사업' in combined:
            return "내역사업"
        elif any(k in combined for k in ['사업개요', '주관기관', '관리기관']):
            return "사업개요"
        elif any(k in combined for k in ['특허', '논문', '기술이전', '인력']):
            return "성과"
        elif any(k in combined for k in ['예산', '사업비', '백만원']):
            return "예산"
        elif any(k in combined for k in ['분기', '일정', '추진']):
            return "일정"
        
        return "unknown"
    
    def _process_overview(self, rows: List[List], raw_data_id: int):
        """사업개요 처리"""
        overview = {}
        for row in rows:
            if len(row) >= 2:
                key = str(row[0]).strip()
                value = str(row[1]).strip()
                overview[key] = value
        
        self.data['normalized_overviews'].append({
            'id': self._get_next_id('overview'),
            'sub_project_id': self.current_context['sub_project_id'],
            'raw_data_id': raw_data_id,
            'overview_data': json.dumps(overview, ensure_ascii=False)
        })
    
    def _process_sub_project(self, rows: List[List]) -> bool:
        """내역사업 처리"""
        for row in rows:
            if len(row) < 2:
                continue
            
            key = str(row[0]).strip()
            value = str(row[1]).strip()
            
            if '내역사업명' in key and value:
                # 내역사업 생성
                sub_id = self._get_next_id('sub_project')
                project = {
                    'id': sub_id,
                    'project_code': f"GOV-{self.current_context['document_year']}-{sub_id:03d}",
                    'department_name': '과학기술정보통신부',
                    'main_project_name': self.current_context.get('main_project', ''),
                    'sub_project_name': value,
                    'document_year': self.current_context['document_year']
                }
                
                self.data['sub_projects'].append(project)
                self.current_context['sub_project_id'] = sub_id
                
                logger.info(f"✅ 내역사업 등록: {value} (ID: {sub_id})")
                return True
            elif '세부사업명' in key:
                self.current_context['main_project'] = value
        
        return False
    
    def normalize(self, json_data: Dict) -> bool:
        """JSON 데이터 정규화 (main_government_standard.py 호환)"""
        try:
            logger.info(f"🚀 정부 표준 정규화 시작")
            
            # 문서 연도 설정
            self.current_context['document_year'] = 2024
            self.current_context['performance_year'] = 2023
            self.current_context['plan_year'] = 2024
            
            # 페이지별 처리
            for page_idx, page in enumerate(json_data.get('pages', []), 1):
                full_text = page.get('full_text', '')
                
                # 카테고리 감지
                category = 'overview'
                if '(2)' in full_text and '추진실적' in full_text:
                    category = 'performance'
                elif '(3)' in full_text and '추진계획' in full_text:
                    category = 'plan'
                
                # 테이블 처리
                for table_idx, table in enumerate(page.get('tables', [])):
                    rows = table.get('data', [])
                    if not rows:
                        continue
                    
                    # 내역사업 확인
                    table_type = self._detect_table_type(rows)
                    if table_type == "내역사업":
                        self._process_sub_project(rows)
                    
                    # 데이터 처리
                    if self.current_context['sub_project_id']:
                        self._process_table(table, page_idx, table_idx, category)
            
            return True
            
        except Exception as e:
            logger.error(f"처리 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def process_document(self) -> bool:
        """문서 전체 처리"""
        try:
            logger.info(f"🚀 정부 표준 정규화 시작: {self.json_path.name}")
            
            # JSON 로드
            with open(self.json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # normalize 메서드 호출
            return self.normalize(json_data)
            
        except Exception as e:
            logger.error(f"처리 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def save_to_csv(self):
        """CSV 저장"""
        for table_name, records in self.data.items():
            if not records or table_name == 'data_statistics':
                continue
            
            csv_path = self.output_dir / f"{table_name}.csv"
            
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                if records:
                    writer = csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)
            
            logger.info(f"✅ {table_name}.csv 저장 ({len(records)}건)")
    
    def validate_data(self) -> Dict[str, Any]:
        """데이터 검증"""
        validation_result = {
            'success': True,
            'statistics': {},
            'issues': []
        }
        
        # 통계 수집
        stats = {
            '내역사업': len(self.data['sub_projects']),
            '원본데이터': len(self.data['raw_data']),
            '정규화_일정': len(self.data['normalized_schedules']),
            '정규화_성과': len(self.data['normalized_performances']),
            '정규화_예산': len(self.data['normalized_budgets'])
        }
        
        validation_result['statistics'] = stats
        
        # 데이터 무결성 검증
        # 1. 모든 내역사업에 대한 데이터 존재 확인
        for project in self.data['sub_projects']:
            project_id = project['id']
            
            has_schedule = any(s['sub_project_id'] == project_id 
                             for s in self.data['normalized_schedules'])
            has_performance = any(p['sub_project_id'] == project_id 
                                 for p in self.data['normalized_performances'])
            has_budget = any(b['sub_project_id'] == project_id 
                           for b in self.data['normalized_budgets'])
            
            if not (has_schedule or has_performance or has_budget):
                validation_result['issues'].append(
                    f"내역사업 '{project['sub_project_name']}'에 데이터 없음"
                )
        
        # 2. 정규화 비율 확인
        if self.validation_stats['total_tables'] > 0:
            process_rate = (self.validation_stats['processed_tables'] / 
                          self.validation_stats['total_tables'] * 100)
            validation_result['process_rate'] = f"{process_rate:.1f}%"
        
        # 3. 분기별 데이터 완성도 확인
        quarters = {}
        for schedule in self.data['normalized_schedules']:
            q = schedule['quarter']
            if q not in quarters:
                quarters[q] = 0
            quarters[q] += 1
        
        validation_result['quarter_distribution'] = quarters
        
        # 성공 여부 판정
        validation_result['success'] = len(validation_result['issues']) == 0
        
        return validation_result
    
    def print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("📊 정부 표준 정규화 완료")
        print("="*80)
        
        print(f"\n📁 내역사업: {len(self.data['sub_projects'])}개")
        for project in self.data['sub_projects']:
            print(f"  - {project['sub_project_name']} ({project['project_code']})")
        
        print(f"\n📋 데이터 통계:")
        print(f"  원본 데이터: {len(self.data['raw_data'])}건")
        print(f"  정규화 일정: {len(self.data['normalized_schedules'])}건")
        print(f"  정규화 성과: {len(self.data['normalized_performances'])}건")
        print(f"  정규화 예산: {len(self.data['normalized_budgets'])}건")
        
        # 분기별 일정 분포
        quarters = {}
        for schedule in self.data['normalized_schedules']:
            q = f"{schedule['quarter']}/4분기"
            if q not in quarters:
                quarters[q] = 0
            quarters[q] += 1
        
        print(f"\n📅 분기별 일정 분포:")
        for q, count in sorted(quarters.items()):
            print(f"  {q}: {count}건")
        
        # 성과 지표별 분포
        indicators = {}
        for perf in self.data['normalized_performances']:
            key = f"{perf['indicator_category']}-{perf['indicator_type']}"
            if key not in indicators:
                indicators[key] = 0
            indicators[key] = perf['value']
        
        print(f"\n📊 성과 지표:")
        for indicator, value in indicators.items():
            print(f"  {indicator}: {value}")
        
        # 예산 연도별 분포
        budgets = {}
        for budget in self.data['normalized_budgets']:
            year = budget['budget_year']
            if year not in budgets:
                budgets[year] = 0
            budgets[year] += float(budget['amount'])
        
        print(f"\n💰 연도별 예산:")
        for year, amount in sorted(budgets.items()):
            print(f"  {year}년: {amount:,.0f} 백만원")
        
        print("="*80 + "\n")


def normalize_government_standard(json_path: str, output_dir: str) -> Tuple[bool, Dict]:
    """정부 표준 정규화 실행"""
    normalizer = GovernmentStandardNormalizer(json_path, output_dir)
    
    # 처리
    success = normalizer.process_document()
    
    if success:
        # 저장
        normalizer.save_to_csv()
        
        # 통계
        normalizer.print_statistics()
        
        # 검증
        validation_result = normalizer.validate_data()
        
        return True, validation_result
    
    return False, {}


if __name__ == "__main__":
    json_file = "output/extracted_data.json"
    output_folder = "normalized_government"
    
    if Path(json_file).exists():
        success, validation = normalize_government_standard(json_file, output_folder)
        
        if success:
            print("\n✅ 정규화 성공!")
            print(f"검증 결과: {validation}")
        else:
            print("❌ 정규화 실패!")