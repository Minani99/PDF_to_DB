"""
개선된 추진일정 파서
다양한 일정 표 형식 지원
"""
import re
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


@dataclass
class ScheduleItem:
    """일정 아이템"""
    period_type: str  # '분기', '월', '기간', '연중'
    period_text: str  # '1/4분기', '1~3월', '연중' 등
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    task_category: str = ""
    task_description: str = ""
    detail_schedule: str = ""
    status: str = "planned"


class ImprovedScheduleParser:
    """개선된 일정 파서"""
    
    def __init__(self, base_year: int = 2024):
        self.base_year = base_year
    
    def parse_schedule_table(self, rows: List[List], table_format: str = None) -> List[ScheduleItem]:
        """일정 테이블 파싱"""
        
        # 테이블 형식 자동 감지
        if not table_format:
            table_format = self._detect_table_format(rows)
        
        logger.info(f"일정 테이블 형식: {table_format}")
        
        if table_format == "merged_quarters":
            # 1/4분기~2/4분기 같은 병합된 형식
            return self._parse_merged_quarters(rows)
        elif table_format == "separate_quarters":
            # 1/4분기, 2/4분기 각각 분리된 형식
            return self._parse_separate_quarters(rows)
        elif table_format == "monthly":
            # 월별 형식
            return self._parse_monthly(rows)
        elif table_format == "detailed":
            # 상세 일정 (날짜 포함)
            return self._parse_detailed(rows)
        else:
            # 기본 처리
            return self._parse_generic(rows)
    
    def _detect_table_format(self, rows: List[List]) -> str:
        """테이블 형식 자동 감지"""
        if not rows or len(rows) < 2:
            return "generic"
        
        # 헤더와 첫 번째 데이터 행 분석
        headers = ' '.join(str(cell) for cell in rows[0]).lower()
        first_data = ' '.join(str(cell) for row in rows[1:3] for cell in row).lower()
        
        # 병합된 분기 형식 감지 (예: "1/4분기 ~ 2/4분기")
        if re.search(r'\d/4\s*분기\s*~\s*\d/4\s*분기', first_data):
            return "merged_quarters"
        
        # 분리된 분기 형식 (각 행이 하나의 분기)
        elif '1/4분기' in first_data and '2/4분기' in first_data:
            return "separate_quarters"
        
        # 월별 형식
        elif any(f'{i}월' in first_data for i in range(1, 13)):
            return "monthly"
        
        # 상세 일정 (날짜 포함)
        elif re.search(r'\d{4}[.-]\d{1,2}[.-]\d{1,2}', first_data) or \
             re.search(r"'\d{2}\.\d{1,2}", first_data):
            return "detailed"
        
        return "generic"
    
    def _parse_merged_quarters(self, rows: List[List]) -> List[ScheduleItem]:
        """병합된 분기 형식 파싱
        예: 1/4분기 ~ 2/4분기 | 작업내용 | 세부일정
        """
        schedules = []
        
        for row in rows[1:]:  # 헤더 제외
            if len(row) < 2:
                continue
            
            period = str(row[0]).strip()
            if not period or '구분' in period:
                continue
            
            # 병합된 분기 파싱 (예: "1/4분기 ~ 2/4분기")
            quarter_match = re.search(r'(\d)/4\s*분기\s*~\s*(\d)/4\s*분기', period)
            if quarter_match:
                start_quarter = int(quarter_match.group(1))
                end_quarter = int(quarter_match.group(2))
                
                # 기간 계산
                start_month = (start_quarter - 1) * 3 + 1
                end_month = end_quarter * 3
                
                start_date = f"{self.base_year}-{start_month:02d}-01"
                end_date = f"{self.base_year}-{end_month:02d}-31"
                
                schedule = ScheduleItem(
                    period_type='기간',
                    period_text=period,
                    start_date=start_date,
                    end_date=end_date,
                    task_description=str(row[1]).strip() if len(row) > 1 else "",
                    detail_schedule=str(row[2]).strip() if len(row) > 2 else ""
                )
                
                # 작업 카테고리 추출
                if '-' in schedule.task_description:
                    parts = schedule.task_description.split('-', 1)
                    schedule.task_category = parts[0].strip()
                
                schedules.append(schedule)
            
            # 단일 분기
            elif '분기' in period:
                quarter_num = re.search(r'(\d)/4', period)
                if quarter_num:
                    quarter = int(quarter_num.group(1))
                    start_month = (quarter - 1) * 3 + 1
                    end_month = quarter * 3
                    
                    schedule = ScheduleItem(
                        period_type='분기',
                        period_text=period,
                        start_date=f"{self.base_year}-{start_month:02d}-01",
                        end_date=f"{self.base_year}-{end_month:02d}-31",
                        task_description=str(row[1]).strip() if len(row) > 1 else "",
                        detail_schedule=str(row[2]).strip() if len(row) > 2 else ""
                    )
                    
                    schedules.append(schedule)
        
        return schedules
    
    def _parse_separate_quarters(self, rows: List[List]) -> List[ScheduleItem]:
        """분리된 분기 형식 파싱
        예:
        1/4분기 | 작업1 | 일정1
        2/4분기 | 작업2 | 일정2
        """
        schedules = []
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            period = str(row[0]).strip()
            
            # 분기 파싱
            quarter_match = re.search(r'(\d)/4\s*분기', period)
            if quarter_match:
                quarter = int(quarter_match.group(1))
                start_month = (quarter - 1) * 3 + 1
                end_month = quarter * 3
                
                schedule = ScheduleItem(
                    period_type='분기',
                    period_text=period,
                    start_date=f"{self.base_year}-{start_month:02d}-01",
                    end_date=f"{self.base_year}-{end_month:02d}-31",
                    task_description=str(row[1]).strip() if len(row) > 1 else "",
                    detail_schedule=str(row[2]).strip() if len(row) > 2 else ""
                )
                
                # 세부일정에서 구체적 날짜 추출 시도
                detail_dates = self._extract_dates_from_detail(schedule.detail_schedule)
                if detail_dates:
                    schedule.start_date = detail_dates[0]
                    if len(detail_dates) > 1:
                        schedule.end_date = detail_dates[1]
                
                schedules.append(schedule)
        
        return schedules
    
    def _parse_monthly(self, rows: List[List]) -> List[ScheduleItem]:
        """월별 형식 파싱"""
        schedules = []
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            period = str(row[0]).strip()
            
            # 월 범위 파싱 (예: "1~3월")
            month_range = re.search(r'(\d{1,2})\s*~\s*(\d{1,2})\s*월', period)
            if month_range:
                start_month = int(month_range.group(1))
                end_month = int(month_range.group(2))
                
                schedule = ScheduleItem(
                    period_type='기간',
                    period_text=period,
                    start_date=f"{self.base_year}-{start_month:02d}-01",
                    end_date=f"{self.base_year}-{end_month:02d}-31",
                    task_description=str(row[1]).strip() if len(row) > 1 else ""
                )
                schedules.append(schedule)
            
            # 단일 월 파싱
            elif '월' in period:
                month_match = re.search(r'(\d{1,2})\s*월', period)
                if month_match:
                    month = int(month_match.group(1))
                    
                    schedule = ScheduleItem(
                        period_type='월',
                        period_text=period,
                        start_date=f"{self.base_year}-{month:02d}-01",
                        end_date=f"{self.base_year}-{month:02d}-31",
                        task_description=str(row[1]).strip() if len(row) > 1 else ""
                    )
                    schedules.append(schedule)
        
        return schedules
    
    def _parse_detailed(self, rows: List[List]) -> List[ScheduleItem]:
        """상세 일정 파싱 (구체적 날짜 포함)"""
        schedules = []
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            period = str(row[0]).strip()
            task = str(row[1]).strip() if len(row) > 1 else ""
            detail = str(row[2]).strip() if len(row) > 2 else ""
            
            # 모든 텍스트에서 날짜 추출
            all_text = f"{period} {task} {detail}"
            dates = self._extract_all_dates(all_text)
            
            # 기본 기간 텍스트
            period_text = period if period else "일정"
            
            # 분기 정보가 있으면 추가
            if '분기' in all_text:
                quarter_match = re.search(r'(\d)/4\s*분기', all_text)
                if quarter_match:
                    period_text = f"{quarter_match.group(0)}"
            
            schedule = ScheduleItem(
                period_type='기간' if len(dates) > 1 else '날짜',
                period_text=period_text,
                start_date=dates[0] if dates else None,
                end_date=dates[1] if len(dates) > 1 else dates[0] if dates else None,
                task_description=task,
                detail_schedule=detail
            )
            
            schedules.append(schedule)
        
        return schedules
    
    def _parse_generic(self, rows: List[List]) -> List[ScheduleItem]:
        """일반적인 형식 파싱"""
        schedules = []
        
        for row in rows[1:]:
            if len(row) < 2:
                continue
            
            period = str(row[0]).strip()
            if not period or '구분' in period:
                continue
            
            # 연중 처리
            if '연중' in period:
                schedule = ScheduleItem(
                    period_type='연중',
                    period_text=period,
                    start_date=f"{self.base_year}-01-01",
                    end_date=f"{self.base_year}-12-31",
                    task_description=str(row[1]).strip() if len(row) > 1 else ""
                )
                schedules.append(schedule)
            else:
                # 기타 처리
                schedule = ScheduleItem(
                    period_type='기타',
                    period_text=period,
                    task_description=str(row[1]).strip() if len(row) > 1 else "",
                    detail_schedule=str(row[2]).strip() if len(row) > 2 else ""
                )
                
                # 날짜 추출 시도
                dates = self._extract_all_dates(f"{period} {schedule.detail_schedule}")
                if dates:
                    schedule.start_date = dates[0]
                    if len(dates) > 1:
                        schedule.end_date = dates[1]
                
                schedules.append(schedule)
        
        return schedules
    
    def _extract_dates_from_detail(self, text: str) -> List[str]:
        """세부일정에서 날짜 추출"""
        dates = []
        
        # '24.1~3월 형식
        pattern1 = r"'?(\d{2})\.(\d{1,2})\s*~\s*(\d{1,2})\s*월?"
        match1 = re.search(pattern1, text)
        if match1:
            year = f"20{match1.group(1)}"
            start_month = match1.group(2)
            end_month = match1.group(3)
            dates.append(f"{year}-{start_month:0>2}-01")
            dates.append(f"{year}-{end_month:0>2}-31")
            return dates
        
        # '24.1~'24.3 형식
        pattern2 = r"'?(\d{2})\.(\d{1,2})\s*~\s*'?(\d{2})\.(\d{1,2})"
        match2 = re.search(pattern2, text)
        if match2:
            start_year = f"20{match2.group(1)}"
            start_month = match2.group(2)
            end_year = f"20{match2.group(3)}"
            end_month = match2.group(4)
            dates.append(f"{start_year}-{start_month:0>2}-01")
            dates.append(f"{end_year}-{end_month:0>2}-31")
            return dates
        
        # 2024.1.15 형식
        pattern3 = r"(\d{4})\.(\d{1,2})\.(\d{1,2})"
        matches3 = re.findall(pattern3, text)
        for match in matches3:
            dates.append(f"{match[0]}-{match[1]:0>2}-{match[2]:0>2}")
        
        return dates[:2]
    
    def _extract_all_dates(self, text: str) -> List[str]:
        """텍스트에서 모든 날짜 형식 추출"""
        dates = []
        
        # 다양한 날짜 패턴
        patterns = [
            # '24.1~3월
            (r"'?(\d{2})\.(\d{1,2})\s*~\s*(\d{1,2})\s*월?", 
             lambda m: [f"20{m.group(1)}-{m.group(2):0>2}-01", 
                       f"20{m.group(1)}-{m.group(3):0>2}-31"]),
            
            # '24.1~'24.3
            (r"'?(\d{2})\.(\d{1,2})\s*~\s*'?(\d{2})\.(\d{1,2})",
             lambda m: [f"20{m.group(1)}-{m.group(2):0>2}-01",
                       f"20{m.group(3)}-{m.group(4):0>2}-31"]),
            
            # 2024.1.15 또는 2024-01-15
            (r"(\d{4})[.-](\d{1,2})[.-](\d{1,2})",
             lambda m: [f"{m.group(1)}-{m.group(2):0>2}-{m.group(3):0>2}"]),
            
            # 2024년 1월
            (r"(\d{4})년\s*(\d{1,2})월",
             lambda m: [f"{m.group(1)}-{m.group(2):0>2}-01"])
        ]
        
        for pattern, formatter in patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                result = formatter(match)
                dates.extend(result)
                if len(dates) >= 2:
                    return dates[:2]
        
        return dates
    
    def to_db_format(self, schedules: List[ScheduleItem]) -> List[Dict]:
        """DB 저장 형식으로 변환"""
        db_records = []
        
        for schedule in schedules:
            record = {
                'schedule_type': schedule.period_type,
                'schedule_period': schedule.period_text,
                'task_category': schedule.task_category,
                'task_description': schedule.task_description,
                'start_date': schedule.start_date,
                'end_date': schedule.end_date,
                'status': schedule.status,
                'detail_schedule': schedule.detail_schedule
            }
            db_records.append(record)
        
        return db_records


# 사용 예시
if __name__ == "__main__":
    parser = ImprovedScheduleParser(base_year=2024)
    
    # 테스트 데이터 1: 병합된 분기
    test_data1 = [
        ["구분", "추진계획", "세부일정"],
        ["1/4분기 ~ 2/4분기", "리더연구 신규과제 선정평가", "'24.1 ~ '24.7월"],
        ["3/4분기", "중견연구 신규과제 선정", "'24.7 ~ '24.9월"],
        ["4/4분기", "종료과제 평가", "'24.10 ~ '24.12월"]
    ]
    
    # 테스트 데이터 2: 분리된 분기
    test_data2 = [
        ["구분", "주요내용", "세부일정", "비고"],
        ["1/4분기", "신규과제 공고", "'24.1~3월", ""],
        ["2/4분기", "선정평가", "'24.4~6월", ""],
        ["3/4분기", "협약 및 연구개시", "'24.7~9월", ""],
        ["4/4분기", "중간평가", "'24.10~12월", ""]
    ]
    
    # 파싱
    schedules1 = parser.parse_schedule_table(test_data1)
    schedules2 = parser.parse_schedule_table(test_data2)
    
    print("📅 병합된 분기 형식:")
    for s in schedules1:
        print(f"  {s.period_text}: {s.task_description} ({s.start_date} ~ {s.end_date})")
    
    print("\n📅 분리된 분기 형식:")
    for s in schedules2:
        print(f"  {s.period_text}: {s.task_description} ({s.start_date} ~ {s.end_date})")