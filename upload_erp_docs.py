import re

from src.services.dify_platform import DifyPlatform
from src.utils.config import config
from src.utils.docx_handler import DocxHandler
from src.utils.folder_handler import FolderHandler
from src.utils.time_utils import timing


def create_documents_dict(name, segment_list):
    return {
        'name': name,
        'segment': [
            {
                'content':
                    f'# Question\n{segment.get("question", "")}\n\n'
                    f'# Context\n{segment.get("context", "")}\n\n'
                    f'# Answer\n{segment.get("answer", "")}',
                'keywords': re.split('[,，]', segment.get('context')) if segment.get('context') else [],
                'enabled': True
            }
            for segment in segment_list
        ]
    }


def extract_qa_info(text) -> list:
    pattern = re.compile(
        r'(question|context|answer)\s*[:：]\s*(.*?)(?=(question|context|answer)\s*[:：]|$)',
        re.I | re.S
    )
    matches = pattern.findall(text)

    qa_dicts = []
    curr_dict = {}

    for match in matches:
        key, value, _ = match
        key = key.lower().strip()
        value = value.strip().replace('_x000D_', '')
        curr_dict[key] = value
        if len(curr_dict) == 3:
            qa_dicts.append(curr_dict)
            curr_dict = {}
    return qa_dicts


@timing
def get_erp_data(erp_dir, knowledge_base, segment_size: int = None) -> list:
    erp_data = []
    files = FolderHandler(path=erp_dir).get_files()
    for file in files:
        if file.suffix == '.docx':
            handler = DocxHandler(file)
            docx_content = handler.extract_content()
            docx_content = handler.convert_to_str(
                docx_content, image_reference_type='dify', knowledge_base=knowledge_base
            )
            qa_info = extract_qa_info(docx_content)
            if segment_size is None:
                segment_size = len(qa_info)
            segment_list = []
            i = 1
            for index, item in enumerate(qa_info):
                segment_list.append(item)
                if len(segment_list) == segment_size or (index == len(qa_info) - 1 and segment_list):
                    document_name = f'{file.stem}{file.suffix}' \
                        if segment_size == len(qa_info) else f'{file.stem}.{i}{file.suffix}'
                    documents_dict = create_documents_dict(document_name, segment_list)
                    erp_data.append(documents_dict)
                    segment_list = []
                    i += 1

    return erp_data


def main():
    kb = DifyPlatform('dev').init_knowledge_base(dataset_name=config.erp_dataset)
    erp_data = get_erp_data(config.erp_dir, kb)
    print('ERP data length:', len(erp_data))
    kb.sync_documents(erp_data, sync_config=config.get_doc_sync_config(scenario='file'))


if __name__ == '__main__':
    main()
