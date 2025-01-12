import re
import uuid
from collections import defaultdict

import pandas as pd
import win32com.client
from dateutil.relativedelta import relativedelta

from src.database.record_database import RecordDatabase
from src.services.dify_platform import DifyPlatform
from src.utils.config import config
from src.utils.proofpoint_url_decoder import decode_ppv3
from src.utils.web_scraper import scrape_web_page_content


def get_sender_info(mail):
    sender_email_type = mail.SenderEmailType
    if sender_email_type == 'EX':
        sender_email = mail.Sender.GetExchangeUser().PrimarySmtpAddress
        sender_name = mail.Sender.GetExchangeUser().Name
    elif sender_email_type == 'SMTP':
        sender_email = mail.SenderEmailAddress
        sender_name = mail.SenderName
    else:
        sender_email = ''
        sender_name = ''
    return sender_email, sender_name


def get_cc(mail):
    cc = []
    for recipient in mail.Recipients:
        if recipient.Type == 2:
            cc.append(recipient.addressentry.GetExchangeUser().PrimarySmtpAddress)
    if cc:
        return ';'.join(cc)
    else:
        return ''


def convert_datetime_to_str(datetime_object):
    str_datetime = datetime_object.strftime('%Y-%m-%d %H:%M:%S')
    return str_datetime


def read_mail(mailbox) -> list:
    result = []
    try:
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        email = mailbox.get('email')
        inbox = mailbox.get('inbox')
        inbox_root = outlook.Folders(email).Folders(inbox.get('root_folder'))
        for subfolder in inbox.get('subfolders'):
            subfolder_name = subfolder.get('name')
            mails = inbox_root.Folders(subfolder_name).Items
            mails_count = mails.Count
            print(f'There are {mails_count} mails under "{subfolder_name}"')
            if mails_count == 0:
                continue
            mail_counter = 0
            mail = mails.GetFirst()
            while mail:
                while mail_counter < mails_count:
                    mail_counter = mail_counter + 1
                    sender_email, sender_name = get_sender_info(mail)
                    mail_info = dict(
                        entry_id=mail.EntryID,
                        category=subfolder.get('category'),
                        sender_email=sender_email,
                        sender_name=sender_name,
                        cc=get_cc(mail),
                        subject=mail.Subject,
                        body=mail.Body,
                        html_body=mail.HTMLBody,
                        sent_on=convert_datetime_to_str(mail.SentOn),
                        received_on=convert_datetime_to_str(mail.ReceivedTime)
                    )
                    result.append(mail_info)
                    mail = mails.GetNext()
    except Exception as e:
        print(e)
    finally:
        return result


def get_kb_name_by_category(info, category):
    for item in info:
        for sub in item['sub_folder']:
            if sub['category'].lower() == category.lower():
                return sub['knowledge_base']
    return None


def extract_info(row):
    summary_segment = []
    details_segment = []
    delimiter = '\n'
    for item in row['cleaned_body']:
        category = item['category']
        for content in item['content']:
            title = content['title']['cn']
            source = content['source']
            news_source = re.search(r'[（(](.*?)[)）]\s*\d', source)
            news_source_str = news_source.group(1) if news_source else ''
            date = re.search(r'(\d{1,2})/(\d{1,2})-(\d{4})', source)
            date_str = f'{date.group(3)}{date.group(2).zfill(2)}{date.group(1).zfill(2)}' if date else ''
            name = f'{date_str} - {category} - {news_source_str} - {title}'
            summary = content["summary"]["cn"] if content["summary"]["cn"] else content["summary"]["en"]
            summary_segment.append(
                {
                    'content': (
                        f'# {name}{delimiter}{delimiter}'
                        f'## category{delimiter}{category.lower()}{delimiter}{delimiter}'
                        f'## title{delimiter}{title}{delimiter}{delimiter}'
                        f'## date{delimiter}{date_str}{delimiter}{delimiter}'
                        f'## source{delimiter}{content["source"]}{delimiter}{delimiter}'
                        f'## url{delimiter}{content["url"]}{delimiter}{delimiter}'
                        f'## summary{delimiter}{summary}'
                    ),
                    'answer': None,
                    'keywords': [],
                    'enabled': True
                }
            )
            details_segment.append(
                {
                    'content': (
                        f'# {name}{delimiter}{delimiter}'
                        f'## category{delimiter}{category.lower()}{delimiter}{delimiter}'
                        f'## title{delimiter}{title}{delimiter}{delimiter}'
                        f'## date{delimiter}{date_str}{delimiter}{delimiter}'
                        f'## source{delimiter}{content["source"]}{delimiter}{delimiter}'
                        f'## url{delimiter}{content["url"]}{delimiter}{delimiter}'
                        f'## details{delimiter}{content["details"]}'
                    ),
                    'answer': None,
                    'keywords': [],
                    'enabled': True
                }
            )

    summary_document = {
        'name': f'{row["subject"]} - {row["sent_on"][:4]}',
        'segment': summary_segment
    }
    details_document = {
        'name': f'{row["subject"]} - {row["sent_on"][:4]}',
        'segment': details_segment
    }
    return {
        'mail_id': row['id'],
        'dataset': config.get_dataset_by_category(row['category']),
        'document': {'summary': summary_document, 'details': details_document}
    }


def get_mails(source) -> list:
    mails = []
    if source == 'local':
        for mailbox in config.get_mailbox():
            mails.extend(read_mail(mailbox))
    return mails


def remove_useless_lines(s: str) -> str:
    patterns_to_remove = ["top",
                          ".*to receive the thermo fisher scientific daily news report.*",
                          "please email to \S+@thermofisher\.com.*"]
    for pattern in patterns_to_remove:
        s = re.sub(pattern, '', s, flags=re.I)
    return s


def remove_title(s: str) -> str:
    lines = s.strip().split('\n')
    pattern = r'.*News\s+(\|\s*.*News\s*)+'
    title_index = None
    for index, line in enumerate(lines):
        if re.match(pattern, line, flags=re.IGNORECASE):
            title_index = index
    if title_index is not None:
        del lines[title_index]
    return '\n'.join(lines)


def remove_empty_lines(s: str) -> str:
    lines = s.strip().split('\n')
    return '\n'.join([line for line in lines if line.strip()])


def split_text_by_headings(s: str) -> list:
    lines = s.splitlines()
    sections = []
    current_section = []
    for line in lines:
        if re.match(r'.* News$', line, flags=re.IGNORECASE):
            if current_section:
                sections.append('\n'.join(current_section))
            current_section = [line]
        else:
            current_section.append(line)

    if current_section:
        sections.append('\n'.join(current_section))

    return sections


def is_url_in(text: str, url: str) -> bool:
    pattern = r'<\s*' + re.escape(url) + r'\s*>'
    return re.search(pattern, text) is not None


def extract_url(text: str) -> str:
    match = re.search(r'<\s*(https?://.*?)>', text)
    return match.group(1).strip() if match else ''


def remove_url(text: str, url: str) -> str:
    pattern = r'<\s*' + re.escape(url) + r'\s*>'
    return re.sub(pattern, '', text).strip()


def split_lines_by_indices(lines: list, indices: list) -> list:
    split_indices = [0] + [i for i in indices]
    return [lines[start:end] for start, end in zip(split_indices, split_indices[1:] + [None])]


def convert_string_to_json(s: str) -> dict:
    lines = [line for line in s.splitlines() if line.strip()]
    sections = {'category': '', 'content': []}

    heading_index = next(
        (index for index, line in enumerate(lines) if re.match(r'.* News$', line.strip(), flags=re.IGNORECASE)), -1
    )
    if heading_index != -1:
        sections['category'] = lines[heading_index]
        del lines[heading_index]

    source_pattern = r'[(（].*?[)）].*?\d{4}$'
    source_indices = [index for index, line in enumerate(lines) if re.match(source_pattern, line.strip())]

    if source_indices:
        previous_indices = [i - 1 for i in source_indices if i - 1 >= 0]
        split_lines = split_lines_by_indices(lines, previous_indices)
        for sublist in split_lines:
            if not sublist:
                continue

            source_index = next(
                (index for index, line in enumerate(sublist) if re.match(source_pattern, line.strip())), -1
            )
            if source_index == -1:
                sections['content'].append({'summary': '\n'.join([line for line in sublist if line.strip()])})
            else:
                title = sublist[:source_index]
                title_str = '\n'.join([line for line in title if line.strip()])

                source = sublist[source_index].strip()
                summary = sublist[source_index + 1:]

                url = extract_url(title_str)
                title_str_en = ''

                summary_cn = ''
                details = ''
                if url:
                    title_str = remove_url(title_str, url)
                    summary_without_title = []
                    for line in summary:
                        if is_url_in(line, url):
                            title_str_en = remove_url(line, url)
                        else:
                            summary_without_title.append(line)
                    summary = summary_without_title
                    url = decode_ppv3(url)
                    summary_cn, details = scrape_web_page_content(url)

                summary_str = '\n'.join([line for line in summary if line.strip()])

                sections['content'].append({
                    'title': {'cn': title_str, 'en': title_str_en},
                    'source': source,
                    'url': url,
                    'summary': {'en': summary_str, 'cn': summary_cn},
                    'details': details
                })
    else:
        sections['content'] = '\n'.join([line for line in lines if line.strip()])

    return sections


def convert_text_to_structured_list(s: str) -> list:
    text = remove_useless_lines(s)
    text = remove_title(text)
    text = remove_empty_lines(text)
    parts = split_text_by_headings(text)
    news = []
    for part in parts:
        news.append(convert_string_to_json(part))
    return news


def record_mails(mails):
    record_db = RecordDatabase('record')
    for mail in mails:
        mail_id = record_db.get_mail_id_by_entry_id(mail.get('entry_id'))
        if mail_id:
            mail['id'] = mail_id
        else:
            mail['id'] = str(uuid.uuid4())
    record_db.save_mails(pd.DataFrame(mails), ignored_columns=['message_id', 'cleaned_body'])


def process_mails():
    record_db = RecordDatabase('record')
    mails = record_db.get_mails(get_recent_updated=True, time_delta=relativedelta(days=1), sort_order='asc')
    for index, row in mails.iterrows():
        row['cleaned_body'] = convert_text_to_structured_list(row['body'])
        record_db.save_mails(pd.DataFrame([row]), ignored_columns=['message_id'])


def process_info(info, key: str, dify, record_db, doc_sync_config, source):
    info_dict = defaultdict(list)
    for item in info:
        dataset = item['dataset'].get(key)
        document = {
            'mail_id': item['mail_id'],
            'document': item['document'].get(key)
        }
        info_dict[dataset].append(document)
    dataset_mails_mapping = [
        {'dataset_object': dify.init_knowledge_base(dataset), 'mails': mails}
        for dataset, mails in info_dict.items() if dataset is not None
    ]
    for item in dataset_mails_mapping:
        kb = item.get('dataset_object')
        documents_in_kb = kb.fetch_documents(source=source, with_segment=False)
        doc_ids_in_kb = [document['id'] for document in documents_in_kb] if documents_in_kb is not None else []
        for mail in item.get('mails'):
            mail_id = mail.get('mail_id')
            mail_doc_ids_in_record = record_db.get_mail_related_document_ids(mail_id, kb.dataset_id)
            doc_ids_to_remove = list(set(doc_ids_in_kb) & set(mail_doc_ids_in_record))
            kb.delete_document(doc_ids_to_remove)
            docs_name_id_mapping = kb.sync_documents(mail.get('document'), doc_sync_config)
            for document_id in [value for key, value in docs_name_id_mapping.items()]:
                record_db.save_mail_document_mapping(mail_id, document_id, kb.dataset_id)


def upload_mails_to_knowledge_base(env, mails_category: list, doc_sync_config: dict, sync_summary: bool = True,
                                   sync_details: bool = True, get_recent_updated: bool = None,
                                   time_delta: relativedelta = None):
    dify = DifyPlatform(env)
    record_db = RecordDatabase('record')
    mails = record_db.get_mails(mails_category, get_recent_updated=get_recent_updated, time_delta=time_delta)
    if not mails.empty:
        info = mails.apply(extract_info, axis=1).tolist()
        if sync_summary:
            process_info(info, 'summary', dify, record_db, doc_sync_config, source='db')
        if sync_details:
            process_info(info, 'details', dify, record_db, doc_sync_config, source='db')


def main():
    doc_sync_config = config.get_doc_sync_config(scenario='mail')
    mails = get_mails(source='local')
    record_mails(mails)
    process_mails()
    upload_mails_to_knowledge_base(
        env='dev',
        mails_category=['china daily news'],
        doc_sync_config=doc_sync_config,
        sync_summary=True,
        sync_details=True,
        get_recent_updated=True,
        time_delta=relativedelta(days=1)
    )


if __name__ == '__main__':
    main()
