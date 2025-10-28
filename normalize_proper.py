"""
JSON 데이터를 정규화된 관계형 DB 구조로 변환
Foreign Key 기반의 완전한 정규화 구조 생성 + 모든 텍스트 저장
"""
import json
import csv
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime


class ProperNormalizer:
    """JSON 데이터를 정규화된 테이블 구조로 변환하는 클래스"""

    def __init__(self, json_path: str, output_dir: str):
        self.json_path = Path(json_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # ID 카운터
        self.detail_project_id = 1
        self.sub_project_id = 1
        self.program_id = 1
        self.budget_id = 1
        self.performance_id = 1
        self.schedule_id = 1
        self.raw_table_id = 1

        # 데이터 저장소
        self.data = {
            "document_metadata": [],
            "detail_projects": [],
            "sub_projects": [],
            "sub_project_programs": [],
            "budgets": [],
            "performances": [],
            "schedules": [],
            "raw_tables": []
        }

        # 현재 컨텍스트 추적
        self.current_detail_project_id = None
        self.current_sub_project_id = None
        self.current_detail_project_name = ""
        self.current_sub_project_name = ""

    def normalize(self) -> bool:
        """JSON 파일을 정규화하여 CSV 파일로 저장"""
        try:
            print(f"\n{'='*80}")
            print(f"정규화 시작: {self.json_path.name}")
            print(f"{'='*80}\n")

            with open(self.json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            self._extract_document_metadata(json_data)

            for page_idx, page in enumerate(json_data.get('pages', []), 1):
                self._process_page(page, page_idx)

            self._save_to_csv()
            self._print_statistics()

            return True
        except Exception as e:
            print(f"❌ 정규화 오류: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _extract_document_metadata(self, json_data: Dict[str, Any]):
        """문서 메타데이터 추출"""
        self.data["document_metadata"].append({
            "id": 1,
            "source_file": self.json_path.stem,
            "total_pages": len(json_data.get('pages', [])),
            "total_tables": sum(len(p.get('tables', [])) for p in json_data.get('pages', [])),
            "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "file_size": self.json_path.stat().st_size if self.json_path.exists() else 0
        })

    def _process_page(self, page: Dict[str, Any], page_number: int):
        """페이지의 테이블들과 텍스트를 처리"""
        full_text = page.get('full_text', '')

        for table in page.get('tables', []):
            self._process_table(table, page_number, full_text)

    def _process_table(self, table: Dict[str, Any], page_number: int, full_text: str = ""):
        """개별 테이블 처리"""
        rows = table.get('data', [])
        if not rows:
            return

        headers = rows[0] if rows else []
        table_type = self._detect_table_type(headers, rows)

        if table_type == "project_header":
            self._process_project_header(rows, page_number, full_text)
        elif table_type == "program":
            self._process_program_table(rows, page_number)
        elif table_type == "budget":
            self._process_budget_table(rows, page_number)
        elif table_type == "performance":
            self._process_performance_table(rows, page_number)
        elif table_type == "schedule":
            self._process_schedule_table(rows, page_number)

        self._save_raw_table(table, page_number, table_type, full_text)

    def _detect_table_type(self, headers: List[str], rows: List[List[str]]) -> str:
        """테이블 타입 감지"""
        headers_str = ' '.join(str(h) for h in headers).lower()

        if any(kw in headers_str for kw in ['세부사업명', '내역사업명', '사업개요']):
            return "project_header"
        elif '세부프로그램' in headers_str or '프로그램명' in headers_str:
            return "program"
        elif '사업비' in headers_str or '예산' in headers_str or '국고' in headers_str:
            return "budget"
        elif '성과지표' in headers_str or '특허' in headers_str or '논문' in headers_str:
            return "performance"
        elif '추진계획' in headers_str or '일정' in headers_str or '분기' in headers_str:
            return "schedule"
        else:
            return "unknown"

    def _process_project_header(self, rows: List[List[str]], page_number: int, full_text: str = ""):
        """프로젝트 헤더 테이블 처리 - 모든 필드 추출"""
        detail_project_name = ""
        sub_project_name = ""
        overview = ""
        objectives = ""
        content = ""
        department = ""
        project_type = ""
        representative_field = ""
        research_period = ""
        total_budget_text = ""
        managing_org = ""
        supervising_org = ""

        for row in rows:
            if len(row) < 2:
                continue

            key = str(row[0]).strip() if row[0] else ""
            value = str(row[1]).strip() if len(row) > 1 and row[1] else ""
            extra_key = str(row[2]).strip() if len(row) > 2 and row[2] else ""
            extra_value = str(row[3]).strip() if len(row) > 3 and row[3] else ""

            if '세부사업명' in key:
                detail_project_name = value
            elif '내역사업명' in key:
                sub_project_name = value
            elif '사업개요' in key:
                overview = value
            elif '사업목표' in key or '사업목적' in key:
                objectives = value
            elif '사업내용' in key:
                content = value
            elif '부처' in key or '담당부서' in key:
                department = value
            elif '사업성격' in key:
                project_type = value
            elif '대표분야' in extra_key:
                representative_field = extra_value
            elif '연구기간' in key or '총 연구기간' in key:
                research_period = value
            elif '총 연구비' in key or '총사업비' in key:
                total_budget_text = value
            elif '주관기관' in key:
                managing_org = value
            elif '관리기관' in key or '전문기관' in key:
                supervising_org = value

        # 세부사업 생성
        if detail_project_name and detail_project_name != self.current_detail_project_name:
            self.current_detail_project_name = detail_project_name
            self.current_detail_project_id = self.detail_project_id

            self.data["detail_projects"].append({
                "id": self.detail_project_id,
                "name": detail_project_name,
                "department": department,
                "project_type": project_type,
                "representative_field": representative_field,
                "managing_org": managing_org,
                "supervising_org": supervising_org,
                "page_number": page_number
            })
            self.detail_project_id += 1

        # 내역사업 생성
        if sub_project_name and sub_project_name != self.current_sub_project_name:
            self.current_sub_project_name = sub_project_name
            self.current_sub_project_id = self.sub_project_id

            self.data["sub_projects"].append({
                "id": self.sub_project_id,
                "detail_project_id": self.current_detail_project_id,
                "name": sub_project_name,
                "overview": overview,
                "objectives": objectives,
                "content": content,
                "project_type": project_type,
                "representative_field": representative_field,
                "research_period": research_period,
                "total_budget_text": total_budget_text,
                "managing_org": managing_org,
                "supervising_org": supervising_org,
                "full_page_text": full_text,
                "page_number": page_number
            })
            self.sub_project_id += 1

    def _process_program_table(self, rows: List[List[str]], page_number: int):
        """세부 프로그램 테이블 처리"""
        if not self.current_sub_project_id:
            return

        for row in rows[1:]:
            if len(row) < 2:
                continue

            program_name = str(row[0]).strip() if len(row) > 0 else ""
            description = str(row[1]).strip() if len(row) > 1 else ""

            if program_name and '세부프로그램' not in program_name:
                self.data["sub_project_programs"].append({
                    "id": self.program_id,
                    "sub_project_id": self.current_sub_project_id,
                    "program_name": program_name,
                    "description": description,
                    "page_number": page_number
                })
                self.program_id += 1

    def _process_budget_table(self, rows: List[List[str]], page_number: int):
        """예산 테이블 처리"""
        if not self.current_sub_project_id:
            return

        for row in rows[1:]:
            if len(row) < 2:
                continue

            year = self._extract_year(row)
            total_budget = self._extract_number(row, ['계', '합계', '총액'])
            national_budget = self._extract_number(row, ['국고', '정부'])
            local_budget = self._extract_number(row, ['지방비', '지자체'])
            other_budget = self._extract_number(row, ['기타', '민간'])

            if year or total_budget:
                self.data["budgets"].append({
                    "id": self.budget_id,
                    "sub_project_id": self.current_sub_project_id,
                    "year": year,
                    "total_budget": total_budget,
                    "national_budget": national_budget,
                    "local_budget": local_budget,
                    "other_budget": other_budget,
                    "page_number": page_number
                })
                self.budget_id += 1

    def _process_performance_table(self, rows: List[List[str]], page_number: int):
        """성과 테이블 처리"""
        if not self.current_sub_project_id:
            return

        for row in rows[1:]:
            if len(row) < 2:
                continue

            indicator = str(row[0]).strip() if len(row) > 0 else ""
            target_value = str(row[1]).strip() if len(row) > 1 else ""
            unit = str(row[2]).strip() if len(row) > 2 else ""

            if indicator and '성과지표' not in indicator:
                self.data["performances"].append({
                    "id": self.performance_id,
                    "sub_project_id": self.current_sub_project_id,
                    "indicator": indicator,
                    "target_value": target_value,
                    "unit": unit,
                    "page_number": page_number
                })
                self.performance_id += 1

    def _process_schedule_table(self, rows: List[List[str]], page_number: int):
        """일정 테이블 처리"""
        if not self.current_sub_project_id:
            return

        for row in rows[1:]:
            if len(row) < 2:
                continue

            task = str(row[0]).strip() if len(row) > 0 else ""
            schedule = str(row[1]).strip() if len(row) > 1 else ""

            if task and '추진계획' not in task:
                self.data["schedules"].append({
                    "id": self.schedule_id,
                    "sub_project_id": self.current_sub_project_id,
                    "task": task,
                    "schedule": schedule,
                    "page_number": page_number
                })
                self.schedule_id += 1

    def _save_raw_table(self, table: Dict[str, Any], page_number: int, table_type: str, full_text: str = ""):
        """원본 테이블 저장"""
        if not self.current_sub_project_id:
            return

        self.data["raw_tables"].append({
            "id": self.raw_table_id,
            "sub_project_id": self.current_sub_project_id,
            "page_number": page_number,
            "table_type": table_type,
            "table_data": json.dumps(table, ensure_ascii=False),
            "full_page_text": full_text
        })
        self.raw_table_id += 1

    def _extract_year(self, row: List[str]) -> str:
        """연도 추출"""
        for cell in row:
            cell_str = str(cell)
            if '20' in cell_str and len(cell_str) >= 4:
                import re
                match = re.search(r'20\d{2}', cell_str)
                if match:
                    return match.group()
        return ""

    def _extract_number(self, row: List[str], keywords: List[str]) -> float:
        """숫자 추출"""
        for i, cell in enumerate(row):
            cell_str = str(cell).strip()
            if any(kw in cell_str for kw in keywords):
                if i + 1 < len(row):
                    try:
                        num_str = str(row[i + 1]).replace(',', '').replace(' ', '')
                        return float(num_str)
                    except:
                        pass
        return 0.0

    def _save_to_csv(self):
        """모든 데이터를 CSV 파일로 저장"""
        for table_name, records in self.data.items():
            if not records:
                continue

            csv_path = self.output_dir / f"{table_name}.csv"

            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=records[0].keys())
                writer.writeheader()
                writer.writerows(records)

            print(f"✅ {table_name}.csv 저장 완료 ({len(records)}건)")

    def _print_statistics(self):
        """통계 출력"""
        print(f"\n{'='*80}")
        print("정규화 완료 통계")
        print(f"{'='*80}")
        print(f"📄 문서 메타데이터: {len(self.data['document_metadata'])}건")
        print(f"📋 세부사업: {len(self.data['detail_projects'])}건")
        print(f"📁 내역사업: {len(self.data['sub_projects'])}건")
        print(f"🔹 세부프로그램: {len(self.data['sub_project_programs'])}건")
        print(f"💰 예산: {len(self.data['budgets'])}건")
        print(f"📊 성과: {len(self.data['performances'])}건")
        print(f"📅 일정: {len(self.data['schedules'])}건")
        print(f"📦 원본테이블: {len(self.data['raw_tables'])}건")
        print(f"{'='*80}\n")


def normalize_json_for_db(json_path: str, output_dir: str) -> bool:
    """JSON 파일을 정규화하여 CSV로 저장하는 메인 함수"""
    normalizer = ProperNormalizer(json_path, output_dir)
    return normalizer.normalize()

