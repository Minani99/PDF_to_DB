"""
엔터프라이즈 정부사업 데이터 정규화 시스템
동적 연도 처리 및 정확한 숫자 추출
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


class CategoryType(Enum):
    """카테고리 타입"""
    OVERVIEW = "사업개요"
    PERFORMANCE = "추진실적"
    PLAN = "추진계획"


@dataclass
class SubProject:
    """내역사업 데이터 클래스"""
    id: int
    project_code: str
    department_name: str
    main_project_name: str
    sub_project_name: str
    document_year: int


@dataclass
class ProjectOverview:
    """사업개요 데이터 클래스"""
    id: int
    sub_project_id: int
    managing_organization: str = ""
    supervising_organization: str = ""
    project_type: str = ""
    research_period: str = ""
    total_research_budget: str = ""
    representative_field: str = ""
    objectives: str = ""
    content: str = ""


@dataclass
class PerformanceData:
    """추진실적 데이터 클래스"""
    id: int
    sub_project_id: int
    performance_year: int
    # 특허
    domestic_application: int = 0
    domestic_registration: int = 0
    foreign_application: int = 0
    foreign_registration: int = 0
    # 논문
    scie_total: int = 0
    scie_if10_above: int = 0
    scie_if20_above: int = 0
    non_scie: int = 0
    # 기술이전
    tech_transfer_count: int = 0
    tech_transfer_amount: Decimal = Decimal('0')
    # 인력양성
    phd_graduates: int = 0
    master_graduates: int = 0
    total_participants: int = 0


@dataclass
class PlanData:
    """추진계획 데이터 클래스"""
    id: int
    sub_project_id: int
    plan_year: int
    budgets: List[Dict] = field(default_factory=list)
    schedules: List[Dict] = field(default_factory=list)
    contents: List[str] = field(default_factory=list)


class EnterpriseNormalizer:
    """엔터프라이즈 정규화 클래스"""
    
    def __init__(self, json_path: str, output_dir: str):
        self.json_path = Path(json_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # ID 카운터
        self.id_counters = {
            'sub_project': 1,
            'overview': 1,
            'performance': 1,
            'plan': 1,
            'achievement': 1,
            'budget': 1,
            'schedule': 1
        }
        
        # 데이터 저장소
        self.data = {
            'sub_projects': [],
            'project_overviews': [],
            'project_objectives': [],
            'performance_master': [],
            'performance_patents': [],
            'performance_papers': [],
            'performance_technology': [],
            'performance_hr': [],
            'performance_achievements': [],
            'plan_master': [],
            'plan_budgets': [],
            'plan_schedules': [],
            'plan_contents': []
        }
        
        # 현재 컨텍스트
        self.current_context = {
            'department': '과학기술정보통신부',  # 기본값
            'main_project': '',
            'sub_project': '',
            'sub_project_id': None,
            'document_year': 2024,  # 기본값
            'current_category': None,
            'performance_year': 2023,  # 기본값
            'plan_year': 2024  # 기본값
        }
        
        # 캐시 (중복 방지)
        self.project_cache = {}  # (main, sub) -> id
        
    def _get_next_id(self, entity_type: str) -> int:
        """다음 ID 생성"""
        current = self.id_counters[entity_type]
        self.id_counters[entity_type] += 1
        return current
    
    def _extract_document_year(self, text: str) -> int:
        """문서 연도 추출 (예: "2024년도 생명공학육성시행계획")"""
        match = re.search(r'(\d{4})년도', text)
        if match:
            return int(match.group(1))
        return self.current_context['document_year']
    
    def _parse_number(self, value: Any, default: Any = 0) -> Any:
        """숫자 파싱 (정확한 추출)"""
        if value is None or value == '' or value == '-':
            return default
            
        # 문자열 변환
        str_value = str(value).strip()
        
        # 쉼표 제거
        str_value = str_value.replace(',', '')
        
        # 괄호 안 내용 제거 (예: "1,234 (건)")
        str_value = re.sub(r'\([^)]*\)', '', str_value).strip()
        
        try:
            # 소수점 있으면 float/Decimal
            if '.' in str_value:
                return Decimal(str_value)
            else:
                return int(str_value)
        except (ValueError, TypeError):
            logger.warning(f"숫자 파싱 실패: {value}")
            return default
    
    def _detect_category(self, text: str, page_number: int) -> Optional[CategoryType]:
        """카테고리 감지 - (1), (2), (3) 패턴"""
        # 명시적 패턴
        if '(1)' in text and '사업개요' in text:
            return CategoryType.OVERVIEW
        elif '(2)' in text and ('추진실적' in text or '주요 추진실적' in text):
            # 연도 추출
            year_match = re.search(r'(\d{4})년도\s*주요?\s*추진실적', text)
            if year_match:
                self.current_context['performance_year'] = int(year_match.group(1))
            return CategoryType.PERFORMANCE
        elif '(3)' in text and ('추진계획' in text or '년도 추진계획' in text):
            # 연도 추출
            year_match = re.search(r'(\d{4})년도\s*추진계획', text)
            if year_match:
                self.current_context['plan_year'] = int(year_match.group(1))
            return CategoryType.PLAN
        
        # 컨텍스트 유지
        return self.current_context.get('current_category')
    
    def _process_sub_project_header(self, rows: List[List], page_number: int):
        """내역사업 헤더 처리"""
        main_project = ""
        sub_project = ""
        
        for row in rows:
            if len(row) < 2:
                continue
            
            key = str(row[0]).strip()
            value = str(row[1]).strip() if len(row) > 1 else ""
            
            if '세부사업명' in key:
                main_project = value
                self.current_context['main_project'] = main_project
            elif '내역사업명' in key:
                sub_project = value
                self.current_context['sub_project'] = sub_project
        
        # 내역사업 생성/조회
        if main_project and sub_project:
            cache_key = (main_project, sub_project)
            
            if cache_key not in self.project_cache:
                sub_id = self._get_next_id('sub_project')
                project_code = f"SUB-{self.current_context['document_year']}-{sub_id:03d}"
                
                self.data['sub_projects'].append({
                    'id': sub_id,
                    'project_code': project_code,
                    'department_name': self.current_context['department'],
                    'main_project_name': main_project,
                    'sub_project_name': sub_project,
                    'document_year': self.current_context['document_year']
                })
                
                self.project_cache[cache_key] = sub_id
                self.current_context['sub_project_id'] = sub_id
                
                logger.info(f"✅ 내역사업 등록: {sub_project} (ID: {sub_id})")
            else:
                self.current_context['sub_project_id'] = self.project_cache[cache_key]
    
    def _process_overview(self, rows: List[List], full_text: str):
        """사업개요 처리"""
        if not self.current_context['sub_project_id']:
            return
        
        overview = ProjectOverview(
            id=self._get_next_id('overview'),
            sub_project_id=self.current_context['sub_project_id']
        )
        
        objectives_list = []
        content_list = []
        
        for row in rows:
            if len(row) < 2:
                continue
            
            key = str(row[0]).strip()
            value = str(row[1]).strip() if len(row) > 1 else ""
            
            # 추가 컬럼 처리 (3, 4번째)
            extra_key = str(row[2]).strip() if len(row) > 2 else ""
            extra_value = str(row[3]).strip() if len(row) > 3 else ""
            
            # 기본 정보 매핑
            if '주관기관' in key:
                overview.managing_organization = value
                if extra_key and '관리기관' in extra_key:
                    overview.supervising_organization = extra_value
            elif '관리기관' in key or '전문기관' in key:
                overview.supervising_organization = value
            elif '사업성격' in key:
                overview.project_type = value
            elif '연구기간' in key or '총 연구기간' in key:
                overview.research_period = value
            elif '총 연구비' in key or '총사업비' in key:
                overview.total_research_budget = value
            elif '대표분야' in key:
                overview.representative_field = value if not extra_value else extra_value
            elif '사업목표' in key:
                overview.objectives = value
                if value:
                    objectives_list.append(value)
            elif '사업내용' in key:
                overview.content = value
                if value:
                    content_list.append(value)
        
        # 사업내용 상세 파싱 (full_text에서)
        content_details = self._parse_project_content(full_text)
        if content_details:
            content_list.extend(content_details)
        
        # 데이터 저장
        self.data['project_overviews'].append(asdict(overview))
        
        # 목표/내용 별도 저장
        if objectives_list or overview.objectives:
            self.data['project_objectives'].append({
                'id': self._get_next_id('overview'),
                'sub_project_id': self.current_context['sub_project_id'],
                'objective_type': '목표',
                'content': overview.objectives,
                'parsed_json': json.dumps(objectives_list, ensure_ascii=False)
            })
        
        if content_list:
            self.data['project_objectives'].append({
                'id': self._get_next_id('overview'),
                'sub_project_id': self.current_context['sub_project_id'],
                'objective_type': '내용',
                'content': '\n'.join(content_list),
                'parsed_json': json.dumps(content_list, ensure_ascii=False)
            })
    
    def _parse_project_content(self, text: str) -> List[str]:
        """사업내용 상세 파싱"""
        contents = []
        
        # 패턴: "- 내용" 또는 "• 내용" 또는 숫자)
        patterns = [
            r'[-•]\s*([^-•\n]+)',  # 대시나 불릿
            r'\d+\)\s*([^\n]+)',    # 숫자)
            r'[①②③④⑤⑥⑦⑧⑨⑩]\s*([^\n]+)'  # 원문자
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            contents.extend([m.strip() for m in matches if m.strip()])
        
        return contents
    
    def _process_performance(self, rows: List[List], table_type: str):
        """추진실적 처리"""
        if not self.current_context['sub_project_id']:
            return
        
        # 실적 마스터 생성/조회
        perf_year = self.current_context['performance_year']
        perf_master_id = None
        
        # 기존 마스터 찾기
        for master in self.data['performance_master']:
            if (master['sub_project_id'] == self.current_context['sub_project_id'] and
                master['performance_year'] == perf_year):
                perf_master_id = master['id']
                break
        
        # 없으면 생성
        if not perf_master_id:
            perf_master_id = self._get_next_id('performance')
            self.data['performance_master'].append({
                'id': perf_master_id,
                'sub_project_id': self.current_context['sub_project_id'],
                'performance_year': perf_year
            })
        
        # 테이블 타입별 처리
        if '특허' in table_type:
            self._process_patent_table(rows, perf_master_id)
        elif '논문' in table_type:
            self._process_paper_table(rows, perf_master_id)
        elif '기술' in table_type:
            self._process_technology_table(rows, perf_master_id)
        elif '인력' in table_type:
            self._process_hr_table(rows, perf_master_id)
        else:
            # 통합 성과표 처리
            self._process_integrated_performance(rows, perf_master_id)
    
    def _process_integrated_performance(self, rows: List[List], perf_master_id: int):
        """통합 성과표 처리 (여러 지표가 한 표에)"""
        # 헤더 분석
        if not rows or len(rows) < 2:
            return
        
        headers = [str(h).strip().lower() for h in rows[0]]
        
        # 특허 데이터
        patents = {
            'id': self._get_next_id('performance'),
            'performance_id': perf_master_id,
            'domestic_application': 0,
            'domestic_registration': 0,
            'foreign_application': 0,
            'foreign_registration': 0
        }
        
        # 논문 데이터
        papers = {
            'id': self._get_next_id('performance'),
            'performance_id': perf_master_id,
            'scie_total': 0,
            'scie_if10_above': 0,
            'scie_if20_above': 0,
            'non_scie': 0,
            'total_papers': 0
        }
        
        # 기술이전 데이터
        technology = {
            'id': self._get_next_id('performance'),
            'performance_id': perf_master_id,
            'tech_transfer_count': 0,
            'tech_transfer_amount': 0,
            'commercialization_count': 0,
            'commercialization_amount': 0
        }
        
        # 인력양성 데이터
        hr = {
            'id': self._get_next_id('performance'),
            'performance_id': perf_master_id,
            'phd_graduates': 0,
            'master_graduates': 0,
            'short_term_training': 0,
            'long_term_training': 0,
            'total_participants': 0
        }
        
        # 데이터 행 처리
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            category = str(row[0]).strip().lower() if row[0] else ""
            
            # 특허 처리
            if '특허' in category:
                for i, header in enumerate(headers[1:], 1):
                    if i >= len(row):
                        break
                    value = self._parse_number(row[i])
                    if '국내' in header and '출원' in header:
                        patents['domestic_application'] = value
                    elif '국내' in header and '등록' in header:
                        patents['domestic_registration'] = value
                    elif '국외' in header and '출원' in header:
                        patents['foreign_application'] = value
                    elif '국외' in header and '등록' in header:
                        patents['foreign_registration'] = value
            
            # 논문 처리
            elif '논문' in category or 'scie' in category:
                for i, header in enumerate(headers[1:], 1):
                    if i >= len(row):
                        break
                    value = self._parse_number(row[i])
                    if 'if 20' in header or 'if20' in header:
                        papers['scie_if20_above'] = value
                    elif 'if 10' in header or 'if10' in header:
                        papers['scie_if10_above'] = value
                    elif '합계' in header or 'total' in header:
                        papers['scie_total'] = value
                    elif '비scie' in header or 'non' in header:
                        papers['non_scie'] = value
            
            # 기술이전 처리
            elif '기술이전' in category or '기술료' in category:
                for i, header in enumerate(headers[1:], 1):
                    if i >= len(row):
                        break
                    value = self._parse_number(row[i])
                    if '건수' in header:
                        technology['tech_transfer_count'] = value
                    elif '금액' in header or '백만원' in header:
                        technology['tech_transfer_amount'] = value
            
            # 인력양성 처리
            elif '인력' in category or '박사' in category or '석사' in category:
                for i, header in enumerate(headers[1:], 1):
                    if i >= len(row):
                        break
                    value = self._parse_number(row[i])
                    if '박사' in header:
                        hr['phd_graduates'] = value
                    elif '석사' in header:
                        hr['master_graduates'] = value
                    elif '참여' in header:
                        hr['total_participants'] = value
        
        # 특정 행에서 직접 숫자 추출 (예: "1,001 125 74 10")
        for row in rows[1:]:
            # 숫자만 있는 행 찾기
            numbers = []
            for cell in row:
                try:
                    num = self._parse_number(cell)
                    if num > 0:
                        numbers.append(num)
                except:
                    pass
            
            # 특허 데이터 패턴 (4개 숫자)
            if len(numbers) == 4 and numbers[0] > 100:  # 특허는 보통 큰 수
                patents['domestic_application'] = numbers[0]
                patents['domestic_registration'] = numbers[1]
                patents['foreign_application'] = numbers[2]
                patents['foreign_registration'] = numbers[3]
            
            # 논문 데이터 패턴 (5개 숫자)
            elif len(numbers) >= 5 and any(n > 1000 for n in numbers):
                papers['scie_if20_above'] = numbers[0] if numbers[0] < 200 else 0
                papers['scie_if10_above'] = numbers[1] if numbers[1] < 500 else 0
                papers['scie_total'] = max(numbers[2:4])
                papers['non_scie'] = numbers[-1] if numbers[-1] > 100 else 0
        
        # 데이터 저장
        if any(v > 0 for k, v in patents.items() if k != 'id' and k != 'performance_id'):
            self.data['performance_patents'].append(patents)
        
        if any(v > 0 for k, v in papers.items() if k != 'id' and k != 'performance_id'):
            self.data['performance_papers'].append(papers)
        
        if any(v > 0 for k, v in technology.items() if k != 'id' and k != 'performance_id'):
            self.data['performance_technology'].append(technology)
        
        if any(v > 0 for k, v in hr.items() if k != 'id' and k != 'performance_id'):
            self.data['performance_hr'].append(hr)
    
    def _process_plan(self, rows: List[List], table_type: str):
        """추진계획 처리"""
        if not self.current_context['sub_project_id']:
            return
        
        # 계획 마스터 생성/조회
        plan_year = self.current_context['plan_year']
        plan_master_id = None
        
        # 기존 마스터 찾기
        for master in self.data['plan_master']:
            if (master['sub_project_id'] == self.current_context['sub_project_id'] and
                master['plan_year'] == plan_year):
                plan_master_id = master['id']
                break
        
        # 없으면 생성
        if not plan_master_id:
            plan_master_id = self._get_next_id('plan')
            self.data['plan_master'].append({
                'id': plan_master_id,
                'sub_project_id': self.current_context['sub_project_id'],
                'plan_year': plan_year
            })
        
        # 테이블 타입별 처리
        if '예산' in table_type or '사업비' in table_type:
            self._process_budget_table(rows, plan_master_id)
        elif '일정' in table_type or '추진' in table_type:
            self._process_schedule_table(rows, plan_master_id)
    
    def _process_budget_table(self, rows: List[List], plan_master_id: int):
        """예산 테이블 처리"""
        if not rows or len(rows) < 2:
            return
        
        headers = [str(h).strip() for h in rows[0]]
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            # 첫 번째 컬럼이 연도 또는 구분
            first_col = str(row[0]).strip()
            
            # 연도 추출
            year_match = re.search(r'(\d{4})', first_col)
            if not year_match:
                continue
            
            budget_year = int(year_match.group(1))
            
            # 예산 데이터 추출
            for i, header in enumerate(headers[1:], 1):
                if i >= len(row):
                    break
                
                value = self._parse_number(row[i], 0)
                if value <= 0:
                    continue
                
                # 예산 타입 결정
                budget_type = '정부'  # 기본값
                if '민간' in header:
                    budget_type = '민간'
                elif '지방' in header:
                    budget_type = '지방비'
                elif '기타' in header:
                    budget_type = '기타'
                
                # 실적/계획 구분
                is_actual = budget_year < self.current_context['plan_year']
                
                self.data['plan_budgets'].append({
                    'id': self._get_next_id('budget'),
                    'plan_id': plan_master_id,
                    'budget_year': budget_year,
                    'budget_type': budget_type,
                    'planned_amount': value if not is_actual else 0,
                    'actual_amount': value if is_actual else 0
                })
    
    def _process_schedule_table(self, rows: List[List], plan_master_id: int):
        """추진일정 테이블 처리"""
        if not rows or len(rows) < 2:
            return
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            # 분기/시기
            period = str(row[0]).strip()
            if not period or '구분' in period:
                continue
            
            # 일정 타입 결정
            schedule_type = '연중'
            if '분기' in period:
                schedule_type = '분기'
            elif '월' in period:
                schedule_type = '월'
            
            # 작업 내용
            task = str(row[1]).strip() if len(row) > 1 else ""
            
            # 세부일정 (있으면)
            detail = str(row[2]).strip() if len(row) > 2 else ""
            
            # 날짜 파싱
            dates = self._extract_dates_from_text(detail or task)
            
            self.data['plan_schedules'].append({
                'id': self._get_next_id('schedule'),
                'plan_id': plan_master_id,
                'schedule_type': schedule_type,
                'schedule_period': period,
                'task_category': task.split('-')[0].strip() if '-' in task else task[:50],
                'task_description': task,
                'start_date': dates[0] if dates else None,
                'end_date': dates[1] if len(dates) > 1 else None,
                'status': 'planned'
            })
    
    def _extract_dates_from_text(self, text: str) -> List[str]:
        """텍스트에서 날짜 추출"""
        dates = []
        
        # 패턴: '24.1~3월, 2024.1~2024.3 등
        patterns = [
            r"'?(\d{2,4})\.(\d{1,2})~(\d{1,2})",  # '24.1~3
            r"'?(\d{2,4})\.(\d{1,2})~'?(\d{2,4})\.(\d{1,2})",  # '24.1~'24.3
            r"(\d{4})-(\d{1,2})-(\d{1,2})"  # 2024-01-15
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if len(match) == 3:  # 연도.시작월~종료월
                    year = match[0] if len(match[0]) == 4 else f"20{match[0]}"
                    dates.append(f"{year}-{match[1]:0>2}-01")
                    dates.append(f"{year}-{match[2]:0>2}-31")
                    break
        
        return dates[:2]  # 최대 2개 (시작, 종료)
    
    def _process_page(self, page: Dict, page_number: int):
        """페이지 처리"""
        full_text = page.get('full_text', '')
        
        # 카테고리 감지
        category = self._detect_category(full_text, page_number)
        if category:
            self.current_context['current_category'] = category
            logger.info(f"📄 페이지 {page_number}: {category.value} 처리")
        
        # 테이블 처리
        for table in page.get('tables', []):
            self._process_table(table, full_text, category)
    
    def _process_table(self, table: Dict, full_text: str, category: Optional[CategoryType]):
        """테이블 처리"""
        rows = table.get('data', [])
        if not rows:
            return
        
        # 테이블 타입 감지
        table_type = self._detect_table_type(rows, full_text)
        
        # 내역사업 헤더
        if '내역사업명' in table_type:
            self._process_sub_project_header(rows, 0)
        
        # 카테고리별 처리
        if category == CategoryType.OVERVIEW:
            if '사업개요' in table_type:
                self._process_overview(rows, full_text)
        
        elif category == CategoryType.PERFORMANCE:
            self._process_performance(rows, table_type)
        
        elif category == CategoryType.PLAN:
            self._process_plan(rows, table_type)
    
    def _detect_table_type(self, rows: List[List], full_text: str) -> str:
        """테이블 타입 감지"""
        if not rows:
            return "unknown"
        
        # 헤더 분석
        headers = ' '.join(str(h) for h in rows[0]).lower()
        
        # 첫 번째 컬럼 분석
        first_cols = ' '.join(str(row[0]) for row in rows[:3] if row).lower()
        
        # 내역사업 헤더
        if '내역사업명' in headers or '세부사업명' in headers:
            return "내역사업명"
        
        # 사업개요
        if '사업개요' in headers or '주관기관' in first_cols:
            return "사업개요"
        
        # 성과 관련
        if any(k in headers + first_cols for k in ['특허', '논문', 'scie', '기술이전', '인력']):
            return "성과지표"
        
        # 예산
        if any(k in headers + first_cols for k in ['예산', '사업비', '정부', '민간', '백만원']):
            return "예산"
        
        # 일정
        if any(k in headers + first_cols for k in ['분기', '추진', '일정', '계획']):
            return "일정"
        
        return "unknown"
    
    def normalize(self) -> bool:
        """정규화 실행"""
        try:
            logger.info(f"🚀 정규화 시작: {self.json_path.name}")
            
            # JSON 로드
            with open(self.json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # 문서 연도 추출
            doc_year = self._extract_document_year(str(json_data))
            self.current_context['document_year'] = doc_year
            self.current_context['performance_year'] = doc_year - 1
            self.current_context['plan_year'] = doc_year
            
            logger.info(f"📅 문서연도: {doc_year}, 실적연도: {doc_year-1}, 계획연도: {doc_year}")
            
            # 페이지별 처리
            for page_idx, page in enumerate(json_data.get('pages', []), 1):
                self._process_page(page, page_idx)
            
            # CSV 저장
            self._save_to_csv()
            
            # 통계 출력
            self._print_statistics()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 정규화 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _save_to_csv(self):
        """CSV 저장"""
        for table_name, records in self.data.items():
            if not records:
                continue
            
            csv_path = self.output_dir / f"{table_name}.csv"
            
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=records[0].keys())
                writer.writeheader()
                writer.writerows(records)
            
            logger.info(f"✅ {table_name}.csv 저장 ({len(records)}건)")
    
    def _print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("📊 엔터프라이즈 정규화 완료")
        print("="*80)
        
        print(f"\n📁 내역사업: {len(self.data['sub_projects'])}개")
        for project in self.data['sub_projects']:
            print(f"  - {project['sub_project_name']} ({project['project_code']})")
        
        print(f"\n📋 데이터 통계:")
        stats = {
            "사업개요": len(self.data['project_overviews']),
            "사업목표/내용": len(self.data['project_objectives']),
            "실적 마스터": len(self.data['performance_master']),
            "특허 데이터": len(self.data['performance_patents']),
            "논문 데이터": len(self.data['performance_papers']),
            "기술이전 데이터": len(self.data['performance_technology']),
            "인력양성 데이터": len(self.data['performance_hr']),
            "계획 마스터": len(self.data['plan_master']),
            "예산 데이터": len(self.data['plan_budgets']),
            "일정 데이터": len(self.data['plan_schedules'])
        }
        
        for label, count in stats.items():
            if count > 0:
                print(f"  {label}: {count}건")
        
        # 숫자 데이터 검증
        print(f"\n🔢 추출된 주요 숫자:")
        for patent in self.data['performance_patents'][:1]:
            print(f"  특허 - 국내출원: {patent.get('domestic_application', 0)}, "
                  f"국내등록: {patent.get('domestic_registration', 0)}")
        
        for paper in self.data['performance_papers'][:1]:
            print(f"  논문 - SCIE: {paper.get('scie_total', 0)}, "
                  f"IF10이상: {paper.get('scie_if10_above', 0)}")
        
        print("="*80 + "\n")


def normalize_enterprise(json_path: str, output_dir: str) -> bool:
    """엔터프라이즈 정규화 실행"""
    normalizer = EnterpriseNormalizer(json_path, output_dir)
    return normalizer.normalize()


if __name__ == "__main__":
    # 테스트
    json_file = "output/extracted_data.json"
    output_folder = "normalized_enterprise"
    
    if Path(json_file).exists():
        success = normalize_enterprise(json_file, output_folder)
        print("✅ 성공!" if success else "❌ 실패!")