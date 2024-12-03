from src.database.qa_database import QaDatabase
from src.utils.config import config
from src.utils.excel_handler import ExcelHandler


def main():
    qa_db = QaDatabase('qa')
    qa_info = qa_db.get_qa_info(config.department)
    qa_info['keywords'] = qa_info['keywords'].str.extract(r'(\d+)')
    with ExcelHandler(config.export_file_path) as excel:
        excel.export_dataframe_to_excel(qa_info, sheet_name='qa', string_columns=['keywords'])


if __name__ == '__main__':
    main()
