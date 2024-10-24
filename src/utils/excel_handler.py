import numpy as np
import openpyxl
import pandas as pd
import wcwidth


class ExcelHandler(object):
    def __init__(self, path):
        self.path = path

    def get_visible_sheets(self):
        sheets = openpyxl.load_workbook(self.path, read_only=True).worksheets
        visible_sheets = []
        for sheet in sheets:
            if sheet.sheet_state != 'hidden':
                visible_sheets.append(sheet.title)
        if len(visible_sheets) != 1:
            raise Exception(f'{self.path}: the excel file should contain only one visible sheet')
        return visible_sheets

    def read_excel(self, str_columns: list):
        try:
            dataframe = pd.read_excel(self.path, sheet_name=self.get_visible_sheets()[0],
                                      dtype={col: str for col in str_columns} if str_columns else None)
            return dataframe
        except FileNotFoundError:
            return "The file does not exist"

    def split_excel_into_files(self, rows_per_file):
        df = self.read_excel()
        split_count = len(df) // rows_per_file
        if len(df) % rows_per_file != 0:
            split_count += 1
        df['_temp_split_group'] = np.arange(len(df)) // rows_per_file
        for name, group_df in df.groupby('_temp_split_group'):
            group_df.drop(columns='_temp_split_group', inplace=True)
            group_df.to_excel(self.path.parent / f"{self.path.stem}_{name}.xlsx", index=False)

    def __enter__(self):
        self.writer = pd.ExcelWriter(self.path, engine='xlsxwriter')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.writer.close()

    def export_dataframe_to_excel(self, dataframe, sheet_name, string_columns: list = None, set_width_by_value=False):
        workbook = self.writer.book
        if sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name].clear()
        else:
            sheet = workbook.add_worksheet(sheet_name)
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#5B9BD5',
            'font_color': '#FFFFFF'
        })
        fmt_time = workbook.add_format({'num_format': 'yyyy-mm-dd'})
        row = 1
        for i, row_data in dataframe.iterrows():
            for col_idx, col_value in enumerate(row_data):
                if pd.isna(col_value):
                    sheet.write(row, col_idx, None)
                elif isinstance(col_value, pd.Timestamp):
                    sheet.write_datetime(row, col_idx, col_value.to_pydatetime(), fmt_time)
                else:
                    col_name = dataframe.columns[col_idx]
                    if string_columns and col_name in string_columns:
                        sheet.write_string(row, col_idx, str(col_value))
                    else:
                        sheet.write(row, col_idx, col_value)
            row += 1
        worksheet = self.writer.sheets[sheet_name]
        if not isinstance(dataframe.columns, pd.RangeIndex):
            columns_width = [max(len(str(col)), wcwidth.wcswidth(str(col))) + 4 for col in dataframe.columns]
            for col_idx, col_name in enumerate(dataframe.columns):
                if set_width_by_value:
                    max_value_length = dataframe[col_name].astype(str).str.len().max()
                    columns_width[col_idx] = max(columns_width[col_idx], max_value_length)
                worksheet.set_column(col_idx, col_idx, columns_width[col_idx])
                worksheet.write(0, col_idx, col_name, header_format)
