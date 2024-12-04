import base64
import datetime
import re

import pandas as pd

from src.database.crawl_database import CrawlDatabase
from src.services.dify_platform import DifyPlatform
from src.services.windows_share_folder import WindowsShareFolder
from src.utils.config import config
from src.utils.docx_handler import DocxHandler
from src.utils.time_utils import timing


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


def process_images(dify, images, file_stem, kb):
    images_dict = {}
    if not images.empty:
        image_paths = []
        for _, row in images.iterrows():
            img_data = base64.b64decode(row['image_base64_string'])
            img_path = config.image_dir_path / f"{file_stem}.{row['image_id']}.{row['image_name']}.{row['image_type']}"
            with open(img_path, 'wb') as f:
                f.write(img_data)
            image_paths.append(img_path)
        images_path_to_id = dify.upload_images_to_dify(image_paths, kb, file_stem)
        images_dict = {
            str(index): f'\n![image](/files/{value}/file-preview)\n'
            for index, (key, value) in enumerate(images_path_to_id.items())
            if value
        }
    return images_dict


def process_tables(tables):
    if tables.empty:
        return {}
    return {str(index): value for index, value in tables[['table_id', 'table_string']].values}


def get_first_non_empty_row(df, style: str):
    filtered_rows = df[
        (df['style'] == style) &
        (df['text'].notna()) &
        (df['text'].apply(lambda x: x.strip() != ''))
        ]
    if not filtered_rows.empty:
        return filtered_rows.iloc[0]['text']
    return None


def extract_document_info(document_df, document_str):
    title = get_first_non_empty_row(document_df, 'Title')
    if title is None:
        title = get_first_non_empty_row(document_df, 'Normal')
    if title is None:
        title = ''
    release_date = extract_release_date(document_str)
    document_name = f'{release_date}: {title}' if release_date else title
    return document_name, release_date


def add_document_to_kb(kb, document_name, document_str, response):
    document = {
        'name': document_name,
        'segment': [
            {
                'content': document_str,
                'answer': None,
                'keywords': response.get('keywords', []),
                'enabled': True
            }
        ]
    }
    document_id = kb.add_document(document, replace_listed=True, sort_document=False)
    return document_id[document_name]


def add_summary_to_kb(kb, document_name, release_date, response, document_id):
    document = {
        'name': document_name,
        'segment': [
            {
                'content': (
                    f'{document_id}\n'
                    f'{release_date}\n'
                    f'{response.get("summary", "")}'
                ),
                'answer': None,
                'keywords': response.get('keywords', []),
                'enabled': True
            }
        ]
    }
    kb.add_document(document, replace_listed=True, sort_document=False)


def save_docx_file(dify, file):
    dify.record_db.save_docx_file(file[['name', 'extension', 'hash']].to_frame().T)


def process_file(dify, file, summary_kb, details_kb):
    file_path = file['path']
    docx_file = DocxHandler(file_path)
    docx_content = docx_file.read_content()
    document_df = docx_content.document

    images_dict = process_images(dify, docx_content.image, docx_file.file_path.stem, details_kb)
    tables_dict = process_tables(docx_content.table)

    document_str = extract_content_as_str(document_df, images_dict, tables_dict)
    response = dify.analyze_content(dify.summary_api, document_str)
    document_name, release_date = extract_document_info(document_df, document_str)

    details_document_id = add_document_to_kb(details_kb, document_name, document_str, response)
    add_summary_to_kb(summary_kb, document_name, release_date, response, details_document_id)

    save_docx_file(dify, file)


@timing
def upload_files_to_dify(dify, files):
    if files.empty:
        return

    summary_kb = dify.init_knowledge_base(config.summary_dataset)
    details_kb = dify.init_knowledge_base(config.details_dataset)

    for _, file_row in files.iterrows():
        process_file(dify, file_row, summary_kb, details_kb)


def get_first_day_of_month(year: int = None, month: int = None) -> int:
    if year is None:
        year = config.initial_datetime.year
    if month is None:
        month = config.initial_datetime.month
    first_day = datetime.datetime(year, month, 1)
    return int(first_day.strftime('%Y%m%d'))


@timing
def get_valid_files(dify, get_specific_documents: bool = False) -> pd.DataFrame:
    if get_specific_documents:
        documents = CrawlDatabase('crawl').get_documents(get_first_day_of_month())
        include_files = documents['doc_name'].tolist()
    else:
        include_files = None

    wsd = WindowsShareFolder(
        config.share_folder.path,
        config.share_folder.username,
        config.share_folder.password
    )
    files_list = wsd.get_files_list(
        include_subfolders=False,
        file_type='docx',
        include_files=include_files,
        sort_alphabetical='desc'
    )
    if files_list:
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
        if sql_df.empty:
            diff_hash_df = pd.DataFrame(columns=file_df.columns)
            not_in_sql_df = file_df.copy()
        else:
            merged_hash_df = pd.merge(file_df, sql_df, on=['name', 'extension'], suffixes=('_file', '_sql'))
            diff_hash_df = merged_hash_df[merged_hash_df['hash_file'] != merged_hash_df['hash_sql']]
            merged_exist_df = file_df.merge(sql_df, on=['name', 'extension'], how='left', indicator=True,
                                            suffixes=('_file', '_sql'))
            not_in_sql_df = merged_exist_df[merged_exist_df['_merge'] == 'left_only']
        final_df = pd.concat([diff_hash_df, not_in_sql_df], sort=False, ignore_index=True)

        if 'hash_file' in final_df.columns:
            return final_df[['name', 'extension', 'path', 'hash_file']].rename(columns={'hash_file': 'hash'})
        else:
            return final_df[['name', 'extension', 'path', 'hash']]
    return pd.DataFrame()


def main():
    dify = DifyPlatform('sandbox', apps=['summary'])
    print('Getting valid files...')
    valid_files = get_valid_files(dify)
    print(f'{len(valid_files)} valid files')
    print('Uploading files...')
    upload_files_to_dify(dify, valid_files)


if __name__ == '__main__':
    main()
