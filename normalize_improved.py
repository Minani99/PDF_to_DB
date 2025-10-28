"""
개선된 JSON 데이터 정규화 모듈
엔터프라이즈급 3NF/BCNF 정규화 및 유연한 카테고리 시스템 구현
"""
import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field, asdict
import hashlib
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class Department:
    """부처 데이터 클래스"""
    id: int
    code: str
    name: str
    description: str = ""


@dataclass
class MainProject:
    """세부사업 데이터 클래스"""
    id: int
    department_id: int
    code: str
    name: str
    fiscal_year: int
    status: str = "active"


@dataclass
class SubProject:
    """내역사업 데이터 클래스"""
    id: int
    main_project_id: int
    code: str
    name: str
    project_type: str = ""
    priority: int = 0


@dataclass
class Category:
    """카테고리 데이터 클래스"""
    id: int
    code: str
    name: str
    parent_id: Optional[int] = None
    level: int = 1
    display_order: int = 0
    is_active: bool = True


@dataclass
class ProjectCategoryData:
    """프로젝트 카테고리 데이터 (EAV 패턴)"""
    id: int
    sub_project_id: int
    category_id: int
    attribute_key: str
    attribute_value: str
    data_type: str = "text"
    sequence_order: int = 0
    version: int = 1
    is_current: bool = True


class ImprovedNormalizer:
    """개선된 정규화 클래스"""
    
    # 카테고리 정의
    CATEGORY_DEFINITIONS = {
        "overview": {"name": "사업개요", "level": 1},
        "performance": {"name": "추진실적", "level": 1},
        "plan": {"name": "추진계획", "level": 1},
        "budget": {"name": "예산", "level": 2},
        "indicators": {"name": "성과지표", "level": 2},
        "schedule": {"name": "일정", "level": 2},
    }
    
    def __init__(self, json_path: str, output_dir: str):
        self.json_path = Path(json_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # ID 생성기
        self.id_generators = {
            "department": 1,
            "main_project": 1,
            "sub_project": 1,
            "category": 1,
            "category_data": 1,
            "overview": 1,
            "budget": 1,
            "performance": 1,
            "schedule": 1,
            "document": 1,
            "raw_page": 1,
            "audit": 1
        }
        
        # 데이터 저장소
        self.data = {
            "departments": [],
            "main_projects": [],
            "sub_projects": [],
            "categories": [],
            "project_category_data": [],
            "project_overviews": [],
            "budgets": [],
            "performance_indicators": [],
            "project_schedules": [],
            "document_metadata": [],
            "raw_page_data": [],
            "audit_logs": []
        }
        
        # 캐시 (중복 방지)
        self.cache = {
            "departments": {},  # code -> id
            "main_projects": {},  # (dept_id, name) -> id
            "sub_projects": {},  # (main_id, name) -> id
            "categories": {}  # code -> id
        }
        
        # 현재 컨텍스트
        self.current_context = {
            "department_id": None,
            "main_project_id": None,
            "sub_project_id": None,
            "fiscal_year": 2024  # 기본값
        }
        
        self._initialize_categories()
    
    def _initialize_categories(self):
        """카테고리 초기화"""
        for code, info in self.CATEGORY_DEFINITIONS.items():
            category = Category(
                id=self._get_next_id("category"),
                code=code,
                name=info["name"],
                level=info["level"],
                display_order=len(self.data["categories"])
            )
            self.data["categories"].append(asdict(category))
            self.cache["categories"][code] = category.id
    
    def _get_next_id(self, entity_type: str) -> int:
        """다음 ID 생성"""
        current_id = self.id_generators[entity_type]
        self.id_generators[entity_type] += 1
        return current_id
    
    def _extract_fiscal_year(self, text: str) -> int:
        """회계연도 추출"""
        match = re.search(r'20\d{2}', text)
        if match:
            return int(match.group())
        return self.current_context["fiscal_year"]
    
    def _extract_department(self, text: str) -> Tuple[str, str]:
        """부처 정보 추출"""
        # 부처 패턴 매칭
        dept_patterns = [
            (r'과학기술정보통신부', 'MSIT', '과학기술정보통신부'),
            (r'보건복지부', 'MOHW', '보건복지부'),
            (r'산업통상자원부', 'MOTIE', '산업통상자원부'),
            (r'교육부', 'MOE', '교육부'),
        ]
        
        for pattern, code, name in dept_patterns:
            if re.search(pattern, text):
                return code, name
        
        return 'UNKNOWN', '미분류'
    
    def _create_or_get_department(self, code: str, name: str) -> int:
        """부처 생성 또는 조회"""
        if code in self.cache["departments"]:
            return self.cache["departments"][code]
        
        dept = Department(
            id=self._get_next_id("department"),
            code=code,
            name=name,
            description=f"{name} 관련 사업"
        )
        
        self.data["departments"].append(asdict(dept))
        self.cache["departments"][code] = dept.id
        
        self._add_audit_log("departments", dept.id, "INSERT", None, asdict(dept))
        
        return dept.id
    
    def _create_or_get_main_project(self, dept_id: int, name: str, fiscal_year: int) -> int:
        """세부사업 생성 또는 조회"""
        cache_key = (dept_id, name)
        if cache_key in self.cache["main_projects"]:
            return self.cache["main_projects"][cache_key]
        
        # 사업 코드 생성
        code = f"MAIN-{fiscal_year}-{self.id_generators['main_project']:03d}"
        
        project = MainProject(
            id=self._get_next_id("main_project"),
            department_id=dept_id,
            code=code,
            name=name,
            fiscal_year=fiscal_year
        )
        
        self.data["main_projects"].append(asdict(project))
        self.cache["main_projects"][cache_key] = project.id
        
        self._add_audit_log("main_projects", project.id, "INSERT", None, asdict(project))
        
        return project.id
    
    def _create_or_get_sub_project(self, main_id: int, name: str, project_type: str = "") -> int:
        """내역사업 생성 또는 조회"""
        cache_key = (main_id, name)
        if cache_key in self.cache["sub_projects"]:
            return self.cache["sub_projects"][cache_key]
        
        # 사업 코드 생성
        code = f"SUB-{self.current_context['fiscal_year']}-{self.id_generators['sub_project']:03d}"
        
        project = SubProject(
            id=self._get_next_id("sub_project"),
            main_project_id=main_id,
            code=code,
            name=name,
            project_type=project_type
        )
        
        self.data["sub_projects"].append(asdict(project))
        self.cache["sub_projects"][cache_key] = project.id
        
        self._add_audit_log("sub_projects", project.id, "INSERT", None, asdict(project))
        
        return project.id
    
    def _add_category_data(self, sub_project_id: int, category_code: str, 
                          key: str, value: str, data_type: str = "text"):
        """카테고리 데이터 추가 (EAV 패턴)"""
        if not value or not value.strip():
            return
        
        category_id = self.cache["categories"].get(category_code)
        if not category_id:
            logger.warning(f"Category not found: {category_code}")
            return
        
        data = ProjectCategoryData(
            id=self._get_next_id("category_data"),
            sub_project_id=sub_project_id,
            category_id=category_id,
            attribute_key=key,
            attribute_value=value.strip(),
            data_type=data_type,
            sequence_order=len([d for d in self.data["project_category_data"] 
                               if d["sub_project_id"] == sub_project_id])
        )
        
        self.data["project_category_data"].append(asdict(data))
    
    def _process_overview_data(self, sub_project_id: int, rows: List[List[str]]):
        """사업개요 데이터 처리"""
        overview_data = {
            "id": self._get_next_id("overview"),
            "sub_project_id": sub_project_id,
            "managing_org": "",
            "supervising_org": "",
            "research_period": "",
            "total_budget": None,
            "objectives": "",
            "content": "",
            "representative_field": ""
        }
        
        for row in rows:
            if len(row) < 2:
                continue
            
            key = str(row[0]).strip()
            value = str(row[1]).strip() if len(row) > 1 else ""
            
            # 구조화된 데이터 추출
            if '주관기관' in key:
                overview_data["managing_org"] = value
            elif '관리기관' in key or '전문기관' in key:
                overview_data["supervising_org"] = value
            elif '연구기간' in key or '총 연구기간' in key:
                overview_data["research_period"] = value
            elif '사업목표' in key:
                overview_data["objectives"] = value
            elif '사업내용' in key:
                overview_data["content"] = value
            elif '대표분야' in key:
                overview_data["representative_field"] = value
            elif '총 연구비' in key or '총사업비' in key:
                # 금액 파싱
                budget_match = re.search(r'[\d,]+', value)
                if budget_match:
                    try:
                        overview_data["total_budget"] = float(
                            budget_match.group().replace(',', '')
                        )
                    except ValueError:
                        pass
            
            # EAV 패턴으로도 저장 (유연성)
            self._add_category_data(sub_project_id, "overview", key, value)
        
        self.data["project_overviews"].append(overview_data)
    
    def _process_budget_data(self, sub_project_id: int, rows: List[List[str]]):
        """예산 데이터 처리"""
        headers = rows[0] if rows else []
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            year = self._extract_fiscal_year(str(row[0]))
            if not year:
                continue
            
            for i, cell in enumerate(row[1:], 1):
                if i >= len(headers):
                    break
                
                budget_type = str(headers[i]).strip()
                if not budget_type or budget_type in ['구분', '년도']:
                    continue
                
                try:
                    amount = float(str(cell).replace(',', '').replace(' ', ''))
                    
                    budget_data = {
                        "id": self._get_next_id("budget"),
                        "sub_project_id": sub_project_id,
                        "fiscal_year": year,
                        "budget_type": budget_type,
                        "planned_amount": amount,
                        "executed_amount": None,
                        "execution_rate": None,
                        "currency": "KRW",
                        "remarks": ""
                    }
                    
                    self.data["budgets"].append(budget_data)
                    
                    # EAV 패턴으로도 저장
                    self._add_category_data(
                        sub_project_id, "budget",
                        f"{year}_{budget_type}", str(amount), "number"
                    )
                except (ValueError, TypeError):
                    continue
    
    def _process_performance_data(self, sub_project_id: int, rows: List[List[str]]):
        """성과 지표 데이터 처리"""
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            indicator_name = str(row[0]).strip()
            if not indicator_name or '성과지표' in indicator_name:
                continue
            
            # 지표 타입 추론
            indicator_type = self._infer_indicator_type(indicator_name)
            
            target_value = None
            unit = ""
            
            if len(row) > 1:
                value_str = str(row[1]).strip()
                # 숫자와 단위 분리
                match = re.match(r'([\d,.]+)\s*(.+)?', value_str)
                if match:
                    try:
                        target_value = float(match.group(1).replace(',', ''))
                        unit = match.group(2) or "" if match.lastindex > 1 else ""
                    except ValueError:
                        pass
            
            if len(row) > 2:
                unit = str(row[2]).strip() or unit
            
            performance_data = {
                "id": self._get_next_id("performance"),
                "sub_project_id": sub_project_id,
                "indicator_type": indicator_type,
                "indicator_name": indicator_name,
                "target_value": target_value,
                "achieved_value": None,
                "achievement_rate": None,
                "unit": unit,
                "measurement_year": self.current_context["fiscal_year"]
            }
            
            self.data["performance_indicators"].append(performance_data)
            
            # EAV 패턴으로도 저장
            self._add_category_data(
                sub_project_id, "indicators",
                indicator_name, f"{target_value} {unit}" if target_value else ""
            )
    
    def _infer_indicator_type(self, indicator_name: str) -> str:
        """지표 타입 추론"""
        type_patterns = {
            "특허": ["특허", "출원", "등록"],
            "논문": ["논문", "SCI", "SCIE", "학술지"],
            "기술이전": ["기술이전", "기술료", "라이센스"],
            "인력양성": ["박사", "석사", "인력", "양성"],
            "기타": []
        }
        
        lower_name = indicator_name.lower()
        for type_name, patterns in type_patterns.items():
            if any(pattern.lower() in lower_name for pattern in patterns):
                return type_name
        
        return "기타"
    
    def _process_schedule_data(self, sub_project_id: int, rows: List[List[str]]):
        """일정 데이터 처리"""
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            task_name = str(row[0]).strip()
            if not task_name or '추진계획' in task_name:
                continue
            
            schedule_text = str(row[1]).strip() if len(row) > 1 else ""
            
            # 분기 정보 추출
            phase = self._extract_phase(schedule_text)
            
            # 날짜 정보 추출 (간단한 예시)
            dates = self._extract_dates(schedule_text)
            
            schedule_data = {
                "id": self._get_next_id("schedule"),
                "sub_project_id": sub_project_id,
                "phase": phase,
                "task_name": task_name,
                "start_date": dates[0] if dates else None,
                "end_date": dates[1] if len(dates) > 1 else None,
                "status": "planned",
                "description": schedule_text
            }
            
            self.data["project_schedules"].append(schedule_data)
            
            # EAV 패턴으로도 저장
            self._add_category_data(
                sub_project_id, "schedule",
                task_name, schedule_text
            )
    
    def _extract_phase(self, text: str) -> str:
        """분기 정보 추출"""
        phase_match = re.search(r'(\d)/4\s*분기', text)
        if phase_match:
            return f"{phase_match.group(1)}/4분기"
        
        month_match = re.search(r'(\d{1,2})월', text)
        if month_match:
            month = int(month_match.group(1))
            if month <= 3:
                return "1/4분기"
            elif month <= 6:
                return "2/4분기"
            elif month <= 9:
                return "3/4분기"
            else:
                return "4/4분기"
        
        return "연중"
    
    def _extract_dates(self, text: str) -> List[str]:
        """날짜 정보 추출"""
        dates = []
        
        # YYYY.MM 또는 YYYY-MM 패턴
        date_pattern = r'20\d{2}[-./]\d{1,2}'
        matches = re.findall(date_pattern, text)
        
        for match in matches[:2]:  # 최대 2개 (시작, 종료)
            # 날짜 형식 정규화
            normalized = match.replace('.', '-').replace('/', '-')
            if len(normalized.split('-')[1]) == 1:
                parts = normalized.split('-')
                normalized = f"{parts[0]}-{parts[1]:0>2}"
            dates.append(f"{normalized}-01")  # 일자는 1일로 기본 설정
        
        return dates
    
    def _add_audit_log(self, table_name: str, record_id: int, 
                      action: str, old_value: Any, new_value: Any):
        """감사 로그 추가"""
        audit = {
            "id": self._get_next_id("audit"),
            "table_name": table_name,
            "record_id": record_id,
            "action": action,
            "old_value": json.dumps(old_value, ensure_ascii=False) if old_value else None,
            "new_value": json.dumps(new_value, ensure_ascii=False) if new_value else None,
            "changed_by": "system",
            "changed_at": datetime.now().isoformat()
        }
        
        self.data["audit_logs"].append(audit)
    
    def _process_page(self, page: Dict[str, Any], page_number: int, document_id: int):
        """페이지 처리"""
        # 원본 데이터 저장
        raw_data = {
            "id": self._get_next_id("raw_page"),
            "document_id": document_id,
            "page_number": page_number,
            "raw_text": page.get('full_text', ''),
            "extracted_tables": json.dumps(page.get('tables', []), ensure_ascii=False),
            "processing_notes": ""
        }
        self.data["raw_page_data"].append(raw_data)
        
        # 부처 정보 추출
        full_text = page.get('full_text', '')
        dept_code, dept_name = self._extract_department(full_text)
        dept_id = self._create_or_get_department(dept_code, dept_name)
        self.current_context["department_id"] = dept_id
        
        # 테이블 처리
        for table in page.get('tables', []):
            self._process_table(table, page_number, full_text)
    
    def _process_table(self, table: Dict[str, Any], page_number: int, full_text: str):
        """테이블 처리"""
        rows = table.get('data', [])
        if not rows:
            return
        
        headers = rows[0] if rows else []
        table_type = self._detect_table_type(headers, rows, full_text)
        
        if table_type == "project_header":
            self._process_project_header(rows, page_number)
        elif table_type == "budget" and self.current_context["sub_project_id"]:
            self._process_budget_data(self.current_context["sub_project_id"], rows)
        elif table_type == "performance" and self.current_context["sub_project_id"]:
            self._process_performance_data(self.current_context["sub_project_id"], rows)
        elif table_type == "schedule" and self.current_context["sub_project_id"]:
            self._process_schedule_data(self.current_context["sub_project_id"], rows)
    
    def _detect_table_type(self, headers: List[str], rows: List[List[str]], 
                          full_text: str) -> str:
        """테이블 타입 감지"""
        headers_str = ' '.join(str(h) for h in headers).lower()
        
        # 프로젝트 헤더 테이블
        if any(kw in headers_str for kw in ['세부사업명', '내역사업명', '사업개요']):
            return "project_header"
        
        # 카테고리별 테이블
        if '예산' in headers_str or '사업비' in headers_str or '국고' in headers_str:
            return "budget"
        elif '성과' in headers_str or '지표' in headers_str or '특허' in headers_str:
            return "performance"
        elif '일정' in headers_str or '추진계획' in headers_str or '분기' in headers_str:
            return "schedule"
        
        # 컨텍스트 기반 추론
        if '추진실적' in full_text:
            return "performance"
        elif '추진계획' in full_text:
            return "schedule"
        
        return "unknown"
    
    def _process_project_header(self, rows: List[List[str]], page_number: int):
        """프로젝트 헤더 처리"""
        main_project_name = ""
        sub_project_name = ""
        project_type = ""
        
        for row in rows:
            if len(row) < 2:
                continue
            
            key = str(row[0]).strip()
            value = str(row[1]).strip() if len(row) > 1 else ""
            
            if '세부사업명' in key:
                main_project_name = value
            elif '내역사업명' in key:
                sub_project_name = value
            elif '사업성격' in key:
                project_type = value
        
        # 세부사업 처리
        if main_project_name and self.current_context["department_id"]:
            fiscal_year = self._extract_fiscal_year(main_project_name)
            self.current_context["fiscal_year"] = fiscal_year
            
            main_id = self._create_or_get_main_project(
                self.current_context["department_id"],
                main_project_name,
                fiscal_year
            )
            self.current_context["main_project_id"] = main_id
        
        # 내역사업 처리
        if sub_project_name and self.current_context["main_project_id"]:
            sub_id = self._create_or_get_sub_project(
                self.current_context["main_project_id"],
                sub_project_name,
                project_type
            )
            self.current_context["sub_project_id"] = sub_id
            
            # 사업개요 데이터 처리
            self._process_overview_data(sub_id, rows)
    
    def normalize(self) -> bool:
        """정규화 실행"""
        try:
            logger.info(f"정규화 시작: {self.json_path}")
            
            # JSON 로드
            with open(self.json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # 문서 메타데이터 저장
            document_id = self._get_next_id("document")
            self.data["document_metadata"].append({
                "id": document_id,
                "file_name": self.json_path.stem,
                "file_path": str(self.json_path),
                "file_size": self.json_path.stat().st_size,
                "page_count": len(json_data.get('pages', [])),
                "extraction_date": datetime.now().isoformat(),
                "processing_status": "processing",
                "error_message": None
            })
            
            # 페이지별 처리
            for page_idx, page in enumerate(json_data.get('pages', []), 1):
                self._process_page(page, page_idx, document_id)
            
            # 문서 상태 업데이트
            for doc in self.data["document_metadata"]:
                if doc["id"] == document_id:
                    doc["processing_status"] = "completed"
                    break
            
            # CSV 저장
            self._save_to_csv()
            
            # 통계 출력
            self._print_statistics()
            
            logger.info("정규화 완료")
            return True
            
        except Exception as e:
            logger.error(f"정규화 실패: {e}")
            
            # 에러 상태 업데이트
            for doc in self.data["document_metadata"]:
                if doc["id"] == document_id:
                    doc["processing_status"] = "failed"
                    doc["error_message"] = str(e)
                    break
            
            import traceback
            traceback.print_exc()
            return False
    
    def _save_to_csv(self):
        """CSV 파일로 저장"""
        for table_name, records in self.data.items():
            if not records:
                continue
            
            csv_path = self.output_dir / f"{table_name}.csv"
            
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=records[0].keys())
                writer.writeheader()
                writer.writerows(records)
            
            logger.info(f"✅ {table_name}.csv 저장 완료 ({len(records)}건)")
    
    def _print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("정규화 완료 통계")
        print("="*80)
        
        stats = {
            "📂 부처": len(self.data["departments"]),
            "📋 세부사업": len(self.data["main_projects"]),
            "📁 내역사업": len(self.data["sub_projects"]),
            "🏷️ 카테고리": len(self.data["categories"]),
            "📊 카테고리 데이터": len(self.data["project_category_data"]),
            "📄 사업개요": len(self.data["project_overviews"]),
            "💰 예산": len(self.data["budgets"]),
            "📈 성과지표": len(self.data["performance_indicators"]),
            "📅 일정": len(self.data["project_schedules"]),
            "📑 문서": len(self.data["document_metadata"]),
            "📃 페이지": len(self.data["raw_page_data"]),
            "📝 감사로그": len(self.data["audit_logs"])
        }
        
        for label, count in stats.items():
            print(f"{label}: {count}건")
        
        print("="*80 + "\n")


def normalize_json_improved(json_path: str, output_dir: str) -> bool:
    """개선된 정규화 실행 함수"""
    normalizer = ImprovedNormalizer(json_path, output_dir)
    return normalizer.normalize()


if __name__ == "__main__":
    # 테스트 실행
    json_file = "output/extracted_data.json"
    output_folder = "normalized_output_improved"
    
    if Path(json_file).exists():
        success = normalize_json_improved(json_file, output_folder)
        if success:
            print("✅ 정규화 성공!")
        else:
            print("❌ 정규화 실패!")
    else:
        print(f"❌ JSON 파일을 찾을 수 없습니다: {json_file}")