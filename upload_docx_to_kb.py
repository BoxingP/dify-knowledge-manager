import datetime
import re

import pandas as pd

from src.database.crawl_database import CrawlDatabase
from src.services.dify_platform import DifyPlatform
from src.utils.config import config
from src.utils.docx_handler import DocxHandler
from src.utils.folder_handler import FolderHandler
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
    origin_link = extract_origin_link(document_str)
    document_name = f'{release_date}: {title}' if release_date else title
    return document_name, release_date, origin_link


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
    docs_name_id_mapping = kb.sync_documents(document, sync_config=config.get_doc_sync_config(scenario='docx'))
    return docs_name_id_mapping[document_name]


def add_summary_to_kb(kb, document_name, release_date, origin_link, response, document_id):
    document = {
        'name': document_name,
        'segment': [
            {
                'content': (
                    f'{document_id}\n'
                    f'{release_date}\n'
                    f'{response.get("summary", "")}\n'
                    f'{origin_link}'
                ),
                'answer': None,
                'keywords': response.get('keywords', []),
                'enabled': True
            }
        ]
    }
    kb.sync_documents(document, sync_config=config.get_doc_sync_config(scenario='docx'))


def save_docx_file(dify, file):
    dify.record_db.save_docx_file(file[['name', 'extension', 'hash']].to_frame().T)


def process_file(dify, file, summary_kb, details_kb):
    file_path = file['path']

    handler = DocxHandler(
        file_path=file_path,
        title_prefix='Title: ',
        text_rules={r"发布日期[:：]\s*": "Release Date: "}
    )

    docx_content = handler.extract_content()
    document_df = docx_content.document

    document_str = handler.convert_to_str(docx_content, image_reference_type='dify', knowledge_base=details_kb)
    summary_agent = dify.studios.summary
    response = summary_agent.query_app(document_str, parse_json=True, streaming_mode=False)
    document_name, release_date, origin_link = extract_document_info(document_df, document_str)

    details_document_id = add_document_to_kb(details_kb, document_name, document_str, response)
    add_summary_to_kb(summary_kb, document_name, release_date, origin_link, response, details_document_id)

    save_docx_file(dify, file)


@timing
def upload_docx_files(dify, files):
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
def get_docx_files(dify, get_specific_documents: bool = False) -> pd.DataFrame:
    if get_specific_documents:
        documents = CrawlDatabase('crawl').get_documents(get_first_day_of_month())
        include_files = documents['doc_name'].tolist()
    else:
        include_files = None

    wsd = FolderHandler(
        config.share_folder.path,
        config.share_folder.username,
        config.share_folder.password
    )
    files_list = wsd.get_files(
        include_sub_dirs=False,
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
    upload_platform = DifyPlatform(env='sandbox', apps=['summary'])
    print('Getting valid .docx files...')
    docx_files = get_docx_files(upload_platform)
    print(f'{len(docx_files)} valid .docx files')
    print('Uploading files...')
    upload_docx_files(upload_platform, docx_files)


if __name__ == '__main__':
    main()
