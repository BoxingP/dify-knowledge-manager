import numpy as np
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

    def split_excel_into_files(self, file_path, rows_per_file):
        df = self.read_excel(file_path)
        split_count = len(df) // rows_per_file
        if len(df) % rows_per_file != 0:
            split_count += 1
        df['_temp_split_group'] = np.arange(len(df)) // rows_per_file
        for name, group_df in df.groupby('_temp_split_group'):
            group_df.drop(columns='_temp_split_group', inplace=True)
            group_df.to_excel(file_path.parent / f"{file_path.stem}_{name}.xlsx", index=False)
