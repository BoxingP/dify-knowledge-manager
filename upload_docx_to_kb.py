import base64
import datetime
import os
import re

import pandas as pd
from docx import Document

from src.services.dify_platform import DifyPlatform
from src.services.knowledge_base import KnowledgeBase
from src.services.windows_share_folder import WindowsShareFolder
from src.utils.config import config
from src.utils.docx_handler import DocxHandler


def extract_origin_link(text: str) -> str:
    match = re.search(r"文章来源\s*[:：]\s*(https?://\S+)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return ''


def extract_release_date(text: str) -> str:
    match = re.search(r"release date\s*[:：]\s*(\d{4}-\d{2}-\d{2})", text, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return ''


def extract_content_as_str(document_df, image_dict=None, table_dict=None) -> str:
    if image_dict is None:
        image_dict = {}
    if table_dict is None:
        table_dict = {}

    def process_row_content(row):
        content = str(row['text']).strip()
        if not content:
            if pd.notna(row['image_id']) and str(row['image_id']).strip():
                return image_dict.get(str(row['image_id']), '[image]')
            if pd.notna(row['table_id']) and str(row['table_id']).strip():
                return table_dict.get(str(row['table_id']), '[table]')
            return ''
        if row['style'].lower() == 'title':
            content = f'Title: {content}'
        content = re.sub(r"发布日期[:：]\s*", 'Release Date: ', content)
        return content

    return '\n'.join(filter(None, (process_row_content(row) for _, row in document_df.iterrows())))


def upload_files_to_dify(dify, files):
    summary_kb = KnowledgeBase(dify.dataset_api, dify.get_dataset_id_by_name(config.summary_dataset),
                               config.summary_dataset, dify.record_db)
    details_kb = KnowledgeBase(dify.dataset_api, dify.get_dataset_id_by_name(config.details_dataset),
                               config.details_dataset, dify.record_db)
    for file in files['path']:
        docx_file = DocxHandler(file)
        docx_content = docx_file.read_content()
        document_df = docx_content.document

        images_dict = {}
        if not docx_content.image.empty:
            doc = Document()
            for index, row in docx_content.image.iterrows():
                img_data = base64.b64decode(row['image_base64_string'])
                img_file = config.image_dir_path / f"{str(row['image_id'])}.{str(row['image_name'])}.{str(row['image_type'])}"
                with open(img_file, 'wb') as f:
                    f.write(img_data)
                doc.add_picture(str(img_file))
                os.remove(img_file)
            word_file = config.word_dir_path / f"{docx_file.file_path.stem}.docx"
            doc.save(word_file)
            document_id = details_kb.create_document_by_file(word_file)
            image_documents = details_kb.get_documents(source='api', document_id=document_id, with_segment=True,
                                                       with_image=True)
            images_dict = {str(index): f'\n![image](/files/{value}/file-preview)\n' for index, value in
                           enumerate(image_documents['image'])}
            details_kb.delete_document(document_id)

        tables_dict = {}
        if not docx_content.table.empty:
            tables_dict = {str(index): value for index, value in
                           docx_content.table[['table_id', 'table_string']].values}
        document_str = extract_content_as_str(document_df, images_dict, tables_dict)
        response = dify.analyze_content(document_str)
        title = document_df[document_df['style'] == 'Title'].iloc[0]['text']
        release_date = extract_release_date(document_str)
        document_name = f'{release_date}: {title}'
        origin_link = extract_origin_link(document_str)

        details_document = {
            'name': document_name,
            'segment': [
                {
                    'content': f'{document_str}',
                    'answer': None,
                    'keywords': response.get('keywords', [])
                }
            ]
        }
        details_document_ids = details_kb.add_document(details_document, replace_document=True, sort_document=False)
        details_document_id = details_document_ids[document_name]

        summary_document = {
            'name': document_name,
            'segment': [
                {
                    'content': (
                        f'#Title\n'
                        f'{title}\n\n'
                        f'# Release Date\n'
                        f'{release_date}\n\n'
                        f'# Category\n'
                        f'{response.get("category", "")}\n\n'
                        f'# Summary\n'
                        f'{response.get("summary", "")}\n\n'
                        f'"# Origin Link\n'
                        f'{origin_link}\n\n'
                        f'# Source Document ID\n'
                        f'{details_document_id}'
                    ),
                    'answer': None,
                    'keywords': response.get('keywords', [])
                }
            ]
        }
        summary_kb.add_document(summary_document, replace_document=True, sort_document=False)

    dify.record_db.save_docx_file(files[['name', 'extension', 'hash']])


def get_valid_files(dify) -> list:
    wsd = WindowsShareFolder(
        config.share_folder['path'],
        config.share_folder['username'],
        config.share_folder['password']
    )
    files_list = wsd.get_files_list(
        include_subfolders=False,
        file_type='docx',
        sort_alphabetical='desc'
    )
    file_df = pd.DataFrame(columns=['name', 'extension', 'hash'])
    for file in files_list:
        docx_file = DocxHandler(file)
        df_temp = pd.DataFrame(
            {
                'path': [file],
                'name': [docx_file.file_path.stem],
                'extension': [docx_file.file_path.suffix.split('.')[1]],
                'hash': [docx_file.calculate_hash()]
            }
        )
        file_df = pd.concat([file_df, df_temp], sort=False)
    sql_df = dify.record_db.get_docx_file()
    merged_hash_df = pd.merge(file_df, sql_df, on=['name', 'extension'], suffixes=('_file', '_sql'))
    diff_hash_df = merged_hash_df[merged_hash_df['hash_file'] != merged_hash_df['hash_sql']]
    merged_exist_df = file_df.merge(sql_df, on=['name', 'extension'], how='left', indicator=True,
                                    suffixes=('_file', '_sql'))
    not_in_sql_df = merged_exist_df[merged_exist_df['_merge'] == 'left_only']
    final_df = pd.concat([diff_hash_df, not_in_sql_df], sort=False, ignore_index=True)

    return final_df[['name', 'extension', 'path', 'hash_file']].rename(columns={'hash_file': 'hash'})


def main():
    start = datetime.datetime.now()
    dify = DifyPlatform(api_config=config.api_config('sandbox'))
    print('Getting valid files...')
    valid_files = get_valid_files(dify)
    print('Uploading files...')
    upload_files_to_dify(dify, valid_files)
    end = datetime.datetime.now()
    duration = (end - start).total_seconds()
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"cost time: {minutes} min {seconds} sec")


if __name__ == '__main__':
    main()
