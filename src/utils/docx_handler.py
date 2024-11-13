import base64
import csv
import hashlib
import io
import re
from collections import namedtuple
from io import StringIO
from xml.etree import ElementTree

import docx.document
import pandas as pd
from docx import Document
from docx.oxml import CT_P, CT_Tbl
from docx.table import _Cell, Table
from docx.text.paragraph import Paragraph


class DocxHandler(object):
    def __init__(self, file_path):
        self.file_path = file_path
        self.document = Document(self.file_path)

    def read(self):
        full_text = ''
        for para in self.document.paragraphs:
            full_text += para.text + '\n'
        return full_text

    def extract_qa_info(self):
        text = self.read()
        pattern = re.compile(r'(question|context|answer):(.*?)(?=(question|context|answer):|$)', re.I | re.S)
        matches = pattern.findall(text)
        qa_dicts = []
        curr_dict = {}
        for match in matches:
            key, value, _ = match
            key = key.lower()
            value = value.strip().replace('_x000D_', '')
            curr_dict[key] = value
            if len(curr_dict) == 3:
                qa_dicts.append(curr_dict)
                curr_dict = {}
        return qa_dicts

    def calculate_hash(self) -> str:
        with open(self.file_path, 'rb') as f:
            file_hash = hashlib.sha256()
            while chunk := f.read(8192):
                file_hash.update(chunk)
        return file_hash.hexdigest()

    def read_content(self):
        def iterate_block_items(document):
            if isinstance(document, docx.document.Document):
                parent_elm = document.element.body
            elif isinstance(document, _Cell):
                parent_elm = document._tc
            else:
                raise ValueError('document is not an acceptable instance')

            for child in parent_elm.iterchildren():
                if isinstance(child, CT_P):
                    yield Paragraph(child, document)
                elif isinstance(child, CT_Tbl):
                    yield Table(child, document)

        def read_docx_table(table_id=None, **kwargs):
            def read_docx_tab(table, **kwargs):
                vf = io.StringIO()
                writer = csv.writer(vf)
                for row in table.rows:
                    writer.writerow(cell.text for cell in row.cells)
                vf.seek(0)
                if vf.getvalue().strip() == '':
                    return pd.DataFrame()
                try:
                    return pd.read_csv(vf, **kwargs)
                except pd.errors.EmptyDataError:
                    print(f'Warning: Table {table_id} is empty or malformed')
                    return pd.DataFrame()

            if table_id is None:
                return [read_docx_tab(table, **kwargs) for table in self.document.tables]
            else:
                try:
                    return read_docx_tab(self.document.tables[table_id], **kwargs)
                except IndexError:
                    print(f'Error: specified [table_id]: {table_id} does not exist')
                    raise

        document_df = pd.DataFrame(columns=['text', 'image_id', 'table_id', 'style'])
        table_df = pd.DataFrame(columns=['table_id', 'table_string'])
        image_df = pd.DataFrame(columns=['image_id', 'image_name', 'image_type', 'image_base64_string'])

        table_list = []
        xml_list = []
        i = 0
        image_counter = 0
        is_append = True
        append_text = None
        table_id = None
        image_id = None
        style = None

        for block in iterate_block_items(self.document):
            if 'text' in str(block):
                image_id = ''
                table_id = ''
                bold_text = ''
                for para in block.runs:
                    if para.bold:
                        bold_text += para.text
                style = str(block.style.name)
                append_text = str(block.text).replace('\n', '').replace('\r', '')

                for para in block.runs:
                    xml_str = str(para.element.xml)
                    name_spaces = dict(
                        [node for _, node in ElementTree.iterparse(StringIO(xml_str), events=['start-ns'])])
                    root = ElementTree.fromstring(xml_str)
                    if 'pic:pic' in xml_str:
                        xml_list.append(xml_str)
                        for pic in root.findall('.//pic:pic', name_spaces):
                            cnvpr_elem = pic.find('pic:nvPicPr/pic:cNvPr', name_spaces)
                            name_attr = cnvpr_elem.get('name')
                            blip_elem = pic.find('pic:blipFill/a:blip', name_spaces)
                            embed_attr = blip_elem.get(
                                '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed')
                            image_id = image_counter
                            document_part = self.document.part
                            image_part = document_part.related_parts[embed_attr]
                            image_type = image_part.content_type.split("/")[-1]
                            image_base64 = base64.b64encode(image_part._blob).decode()
                            df_temp = pd.DataFrame(
                                {
                                    'image_id': [image_id],
                                    'image_name': [name_attr],
                                    'image_type': [image_type],
                                    'image_base64_string': [image_base64]
                                }
                            )
                            image_df = pd.concat([image_df, df_temp], sort=False)
                            style = ''
                        image_counter += 1
            elif 'table' in str(block):
                append_text = ''
                style = ''
                table_id = i
                dfs = read_docx_table(table_id=i)
                df_temp = pd.DataFrame(
                    {
                        'table_id': [i],
                        'table_string': dfs.fillna('').to_markdown()
                    }
                )
                table_df = pd.concat([table_df, df_temp], sort=False)
                table_list.append(dfs)
                i += 1
            if is_append and append_text is not None and table_id is not None and style is not None:
                df_temp = pd.DataFrame(
                    {
                        'text': [append_text],
                        'image_id': [image_id],
                        'table_id': [table_id],
                        'style': [style]
                    }
                )
                document_df = pd.concat([document_df, df_temp], sort=False)

        document_df = document_df.reset_index(drop=True)
        image_df = image_df.reset_index(drop=True)
        table_df = table_df.reset_index(drop=True)
        content = namedtuple('content', 'document image table')
        return content(document_df, image_df, table_df)
