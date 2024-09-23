import openpyxl
import pandas as pd


class ExcelHandler(object):
    def __init__(self):
        pass

    def get_visible_sheets(self, file_path):
        sheets = openpyxl.load_workbook(file_path, read_only=True).worksheets
        visible_sheets = []
        for sheet in sheets:
            if sheet.sheet_state != 'hidden':
                visible_sheets.append(sheet.title)
        if len(visible_sheets) != 1:
            raise Exception(f'{file_path}: the excel file should contain only one visible sheet')
        return visible_sheets

    def read_excel(self, file_path):
        try:
            dataframe = pd.read_excel(file_path, sheet_name=self.get_visible_sheets(file_path)[0])
            return dataframe
        except FileNotFoundError:
            return "The file does not exist"
