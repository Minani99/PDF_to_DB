"""
PDF에서 테이블 데이터를 추출하여 JSON으로 변환하는 모듈
"""
import sys
import pdfplumber
import json
from pathlib import Path
import logging
from typing import List, Dict, Any
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PDFTableExtractor:
    """PDF에서 테이블을 추출하여 JSON으로 변환"""

    def __init__(self, pdf_path: str, output_dir: str = "output"):
        self.pdf_path = Path(pdf_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.extracted_data = []

    def extract_tables(self) -> List[Dict[str, Any]]:
        """PDF에서 모든 테이블 추출"""
        logger.info(f"📄 PDF 파일 열기: {self.pdf_path.name}")

        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                total_pages = len(pdf.pages)
                logger.info(f"📖 총 페이지 수: {total_pages}")

                for page_num, page in enumerate(pdf.pages, start=1):
                    logger.info(f"🔍 페이지 {page_num}/{total_pages} 처리 중...")

                    # 페이지에서 테이블 추출
                    tables = page.extract_tables()

                    if tables:
                        logger.info(f"  ✓ {len(tables)}개 테이블 발견")

                        for table_idx, table in enumerate(tables):
                            if table and len(table) > 0:
                                # 테이블 데이터 정리
                                cleaned_table = self._clean_table(table)

                                # 메타데이터와 함께 저장
                                table_data = {
                                    "page_number": page_num,
                                    "table_index": table_idx,
                                    "rows": len(cleaned_table),
                                    "columns": len(cleaned_table[0]) if cleaned_table else 0,
                                    "data": cleaned_table
                                }

                                self.extracted_data.append(table_data)
                                logger.info(f"    • 테이블 {table_idx}: {len(cleaned_table)}행 × {len(cleaned_table[0]) if cleaned_table else 0}열")
                    else:
                        logger.info(f"  - 테이블 없음")

                logger.info(f"✅ 총 {len(self.extracted_data)}개 테이블 추출 완료")

        except Exception as e:
            logger.error(f"❌ PDF 추출 실패: {e}")
            raise

        return self.extracted_data

    def _clean_table(self, table: List[List[str]]) -> List[List[str]]:
        """테이블 데이터 정리"""
        cleaned = []

        for row in table:
            if row and any(cell for cell in row if cell):  # 빈 행 제외
                # 각 셀 정리
                cleaned_row = []
                for cell in row:
                    if cell is None:
                        cleaned_row.append("")
                    else:
                        # 공백 정리 및 줄바꿈 정규화
                        cleaned_cell = str(cell).strip()
                        cleaned_cell = re.sub(r'\s+', ' ', cleaned_cell)
                        cleaned_row.append(cleaned_cell)

                cleaned.append(cleaned_row)

        return cleaned

    def save_to_json(self, output_filename: str = None) -> str:
        """추출된 데이터를 JSON 파일로 저장"""
        if not self.extracted_data:
            logger.warning("⚠️ 저장할 데이터가 없습니다.")
            raise ValueError("저장할 데이터가 없습니다.")

        # 출력 파일명 생성
        if output_filename is None:
            output_filename = self.pdf_path.stem + "_extracted.json"

        output_path = self.output_dir / output_filename

        # JSON 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.extracted_data, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 JSON 파일 저장: {output_path}")
        logger.info(f"   • 총 테이블 수: {len(self.extracted_data)}")

        # 통계 정보
        total_rows = sum(t['rows'] for t in self.extracted_data)
        logger.info(f"   • 총 행 수: {total_rows}")

        return str(output_path)

    def extract_and_save(self, output_filename: str = None) -> str:
        """추출 및 저장을 한번에 실행"""
        self.extract_tables()
        return self.save_to_json(output_filename)


def extract_pdf_to_json(pdf_path: str, output_dir: str = "output", output_filename: str = None) -> str:
    """
    PDF 파일에서 테이블을 추출하여 JSON으로 저장하는 헬퍼 함수

    Args:
        pdf_path: PDF 파일 경로
        output_dir: JSON 파일 저장 디렉토리
        output_filename: 출력 JSON 파일명 (선택사항)

    Returns:
        저장된 JSON 파일 경로
    """
    extractor = PDFTableExtractor(pdf_path, output_dir)
    return extractor.extract_and_save(output_filename)


def main():
    """테스트 실행"""

    # PDF 파일 경로 확인
    pdf_files = list(Path("input").glob("*.pdf"))

    if not pdf_files:
        logger.error("❌ input 폴더에 PDF 파일이 없습니다.")
        sys.exit(1)

    # 첫 번째 PDF 파일 처리
    pdf_path = pdf_files[0]
    logger.info(f"🚀 PDF 추출 시작: {pdf_path.name}")

    try:
        output_path = extract_pdf_to_json(str(pdf_path))
        logger.info(f"✅ 추출 완료: {output_path}")

        # 결과 확인
        with open(output_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.info(f"\n📊 추출 결과:")
            logger.info(f"   • 총 테이블: {len(data)}개")
            for i, table in enumerate(data[:3]):  # 처음 3개만 표시
                logger.info(f"   • 테이블 {i+1}: 페이지 {table['page_number']}, "
                          f"{table['rows']}행 × {table['columns']}열")

        return True

    except Exception as e:
        logger.error(f"❌ 추출 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

