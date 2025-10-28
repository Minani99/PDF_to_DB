"""
일정 파서 테스트
실제 PDF 데이터로 다양한 형식 테스트
"""
from schedule_parser_improved import ImprovedScheduleParser

def test_real_data():
    """실제 PDF 데이터 테스트"""
    
    parser = ImprovedScheduleParser(base_year=2024)
    
    # 3페이지 데이터: 병합된 분기 형식
    page3_data = [
        ["구분", "추진계획", "주요내용", "세부일정", "비고"],
        ["1/4분기 ~ 2/4분기", 
         "리더연구 - '24년 신규과제 선정평가 계속과제 단계평가 창의연구",
         "",
         "'24.1월 ~ '24.7월 '24.2월",
         ""],
        ["", 
         "중견연구 - '24년 신규과제 선정평가 및 연구개시 - 종료과제 최종평가",
         "",
         "'23.1월 ~'23.5월",
         ""],
        ["3/4분기",
         "중견연구 - '24년 신규과제 선정평가 및 연구개시",
         "",
         "'23.7월 ~'23.9월",
         ""],
        ["4/4분기",
         "리더연구 - 종료과제 최종평가",
         "",
         "'23.12월",
         ""]
    ]
    
    # 6페이지 데이터: 분리된 분기 형식  
    page6_data = [
        ["구분", "주요내용", "추진사항", "세부일정", "비고"],
        ["1/4분기", 
         "계속과제 관리 및 신규과제 공고, 개시",
         "",
         "연중",
         ""],
        ["2/4분기",
         "계속과제 관리",
         "",
         "연중",
         ""],
        ["3/4분기",
         "계속과제 관리",
         "",
         "연중",
         ""],
        ["4/4분기",
         "계속과제 관리 및 단계평가 실시 - 차년도 사업 준비 사업 시행계획 수립",
         "",
         "연중",
         ""]
    ]
    
    # 9페이지 데이터: 상세 날짜 포함
    page9_data = [
        ["구분", "주요내용", "추진사항", "세부일정", "비고"],
        ["1/4분기",
         "사업 관리",
         "'24년 차 신규과제 선정 공고 및 선정 평가 계속과제 관리 및 최종평가",
         "'24.1~3월 '24.1~12월",
         ""],
        ["2/4분기",
         "사업 관리",
         "'24년 차 신규과제 선정 평가 및 협약 계속과제 관리 및 최종평가",
         "'24.4~6월 '24.1~12월",
         ""],
        ["3/4분기",
         "사업 관리",
         "계속과제 관리 및 최종평가",
         "'24.1~12월",
         ""],
        ["4/4분기",
         "사업 관리 계속과제 단계평가 차년도 사업 준비",
         "계속과제 관리 및 최종평가 사업 시행계획 수립",
         "'24.1~12월 '24.11월 '24.12월",
         ""]
    ]
    
    print("="*80)
    print("📅 일정 파싱 테스트")
    print("="*80)
    
    # 3페이지 테스트
    print("\n[3페이지 - 병합된 분기 형식]")
    schedules1 = parser.parse_schedule_table(page3_data)
    for s in schedules1:
        print(f"📌 기간: {s.period_text}")
        print(f"   유형: {s.period_type}")
        print(f"   작업: {s.task_description[:50]}...")
        print(f"   날짜: {s.start_date} ~ {s.end_date}")
        print(f"   세부: {s.detail_schedule}")
        print()
    
    # 6페이지 테스트
    print("\n[6페이지 - 분리된 분기 + 연중]")
    schedules2 = parser.parse_schedule_table(page6_data)
    for s in schedules2:
        print(f"📌 기간: {s.period_text}")
        print(f"   유형: {s.period_type}")
        print(f"   작업: {s.task_description}")
        print(f"   날짜: {s.start_date} ~ {s.end_date}")
        print()
    
    # 9페이지 테스트
    print("\n[9페이지 - 상세 날짜 포함]")
    schedules3 = parser.parse_schedule_table(page9_data)
    for s in schedules3:
        print(f"📌 기간: {s.period_text}")
        print(f"   유형: {s.period_type}")
        print(f"   작업: {s.task_description[:50]}...")
        print(f"   날짜: {s.start_date} ~ {s.end_date}")
        print(f"   세부: {s.detail_schedule[:50]}..." if len(s.detail_schedule) > 50 else s.detail_schedule)
        print()
    
    # DB 저장 형식 테스트
    print("\n[DB 저장 형식]")
    db_records = parser.to_db_format(schedules1[:2])
    for record in db_records:
        print(record)
    
    # 통계
    print("\n[통계]")
    all_schedules = schedules1 + schedules2 + schedules3
    print(f"총 일정 수: {len(all_schedules)}")
    
    types = {}
    for s in all_schedules:
        types[s.period_type] = types.get(s.period_type, 0) + 1
    
    print("유형별 분포:")
    for t, count in types.items():
        print(f"  - {t}: {count}개")
    
    # 날짜가 정확히 파싱된 항목
    with_dates = [s for s in all_schedules if s.start_date and s.end_date]
    print(f"\n날짜 파싱 성공: {len(with_dates)}/{len(all_schedules)} ({len(with_dates)*100/len(all_schedules):.1f}%)")


if __name__ == "__main__":
    test_real_data()