"""
PDF 문서를 JSON으로 변환하고 정규화하여 MySQL DB에 적재하는 메인 프로그램
"""
from config import INPUT_DIR, OUTPUT_DIR, NORMALIZED_OUTPUT_DIR
from extract_pdf_tables import extract_pdf_to_json
from normalize_proper import normalize_json_for_db
from load_proper_db import load_to_mysql
def main():
    """메인 실행 함수"""
    print("\n" + "="*80)
    print("PDF 데이터 처리 및 DB 적재 프로그램")
    print("="*80 + "\n")
    # [단계 1] PDF → JSON 변환
    pdf_files = list(INPUT_DIR.glob("*.pdf"))
    if pdf_files:
        print(f"📁 발견된 PDF 파일: {len(pdf_files)}개\n")
        print("[단계 1] PDF → JSON 변환")
        print("-" * 80)
        for pdf_file in pdf_files:
            print(f"\n처리 중: {pdf_file.name}")
            success = extract_pdf_to_json(
                pdf_path=str(pdf_file),
                output_dir=str(OUTPUT_DIR)
            )
            if not success:
                print(f"❌ {pdf_file.name} 변환 실패")
                return False
    else:
        print("⚠️  input 폴더에 PDF 파일이 없습니다.")
        print("   기존 JSON 파일로 진행합니다...\n")
    # [단계 2] JSON → CSV 정규화
    json_files = list(OUTPUT_DIR.glob("*.json"))
    if not json_files:
        print("❌ output 폴더에 JSON 파일이 없습니다.")
        print("   먼저 PDF를 input 폴더에 넣어주세요.")
        return False
    print(f"📁 발견된 JSON 파일: {len(json_files)}개\n")
    print("[단계 2] JSON 데이터 정규화")
    print("-" * 80)
    for json_file in json_files:
        print(f"\n처리 중: {json_file.name}")
        success = normalize_json_for_db(
            json_path=str(json_file),
            output_dir=str(NORMALIZED_OUTPUT_DIR)
        )
        if not success:
            print(f"❌ {json_file.name} 정규화 실패")
            return False
    # [단계 3] CSV → MySQL DB 적재
    print("\n[단계 3] MySQL DB 적재")
    print("-" * 80)
    success = load_to_mysql(csv_dir=str(NORMALIZED_OUTPUT_DIR))
    if success:
        print("\n" + "="*80)
        print("✅ 모든 작업이 성공적으로 완료되었습니다!")
        print("="*80)
        print("\n💡 MySQL에서 확인하기:")
        print("   mysql -u root -p")
        print("   USE convert_pdf;")
        print("   SHOW TABLES;")
        print("   SELECT * FROM detail_projects;")
        print("="*80 + "\n")
        return True
    else:
        print("\n❌ DB 적재 실패")
        return False
if __name__ == "__main__":
    main()
