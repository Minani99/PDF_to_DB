"""
PDF에서 테이블 데이터와 모든 텍스트를 추출하여 JSON으로 변환하는 모듈
pdfplumber를 사용한 고정밀 테이블 및 텍스트 추출
"""
import pdfplumber
import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class PDFTableExtractor:
    """PDF 파일에서 테이블과 모든 텍스트를 추출하는 클래스"""

    def __init__(self, pdf_path: str, output_dir: str):
        """
        Args:
            pdf_path: 입력 PDF 파일 경로
            output_dir: 출력 JSON 디렉토리 경로
        """
        self.pdf_path = Path(pdf_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # 출력 JSON 파일명 생성
        self.output_json = self.output_dir / f"{self.pdf_path.stem}_output.json"

    def extract(self) -> bool:
        """PDF에서 테이블과 텍스트를 추출하여 JSON으로 저장"""
        try:
            print(f"\n{'='*80}")
            print(f"PDF 테이블 및 텍스트 추출 시작: {self.pdf_path.name}")
            print(f"{'='*80}\n")

            result = {
                "metadata": {
                    "source_file": self.pdf_path.name,
                    "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "total_pages": 0
                },
                "pages": []
            }

            # PDF 열기
            with pdfplumber.open(self.pdf_path) as pdf:
                result["metadata"]["total_pages"] = len(pdf.pages)

                # 각 페이지 처리
                for page_num, page in enumerate(pdf.pages, 1):
                    print(f"📄 페이지 {page_num}/{len(pdf.pages)} 처리 중...")

                    # 전체 텍스트 추출
                    full_text = page.extract_text() or ""

                    page_data = {
                        "page_number": page_num,
                        "full_text": full_text,  # 페이지 전체 텍스트 저장
                        "tables": []
                    }

                    # 테이블 추출
                    tables = page.extract_tables()

                    if tables:
                        print(f"   ✅ {len(tables)}개 테이블 발견")

                        for table_idx, table in enumerate(tables, 1):
                            if table and len(table) > 0:
                                # 빈 행 제거
                                cleaned_table = [
                                    row for row in table
                                    if row and any(cell for cell in row if cell and str(cell).strip())
                                ]

                                if cleaned_table:
                                    page_data["tables"].append({
                                        "table_number": table_idx,
                                        "rows": len(cleaned_table),
                                        "columns": len(cleaned_table[0]) if cleaned_table else 0,
                                        "data": cleaned_table
                                    })
                    else:
                        print(f"   ⚠️  테이블 없음")

                    result["pages"].append(page_data)

            # JSON 저장
            with open(self.output_json, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            # 통계 출력
            self._print_statistics(result)

            print(f"\n✅ JSON 저장 완료: {self.output_json.name}\n")
            return True

        except Exception as e:
            print(f"❌ PDF 추출 오류: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _print_statistics(self, result: Dict[str, Any]):
        """추출 통계 출력"""
        total_tables = sum(len(page["tables"]) for page in result["pages"])
        total_rows = sum(
            table["rows"]
            for page in result["pages"]
            for table in page["tables"]
        )
        total_text_length = sum(
            len(page.get("full_text", ""))
            for page in result["pages"]
        )

        print(f"\n{'='*80}")
        print("추출 완료 통계")
        print(f"{'='*80}")
        print(f"📄 총 페이지: {result['metadata']['total_pages']}개")
        print(f"📊 총 테이블: {total_tables}개")
        print(f"📋 총 행 수: {total_rows}개")
        print(f"📝 총 텍스트: {total_text_length:,}자")
        print(f"{'='*80}")


def extract_pdf_to_json(pdf_path: str, output_dir: str) -> bool:
    """
    PDF 파일을 JSON으로 변환하는 메인 함수

    Args:
        pdf_path: 입력 PDF 파일 경로
        output_dir: 출력 JSON 디렉토리 경로

    Returns:
        성공 여부
    """
    extractor = PDFTableExtractor(pdf_path, output_dir)
    return extractor.extract()

