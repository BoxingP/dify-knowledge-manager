import datetime

from src.services.dify_platform import DifyPlatform
from src.services.knowledge_base import KnowledgeBase
from src.utils.config import config
from src.utils.docx_handler import DocxHandler


def create_data_dict(name, segment_list):
    return {
        'name': name,
        'segment': [
            {
                'content': f'# Question\n{segment["question"]}\n\n# Context\n{segment["context"]}\n\n# Answer\n{segment["answer"]}',
                'keywords': [segment['context']]
            }
            for segment in segment_list
        ]
    }


def get_data_list(segment_size: int = None):
    qa_info = DocxHandler(config.erp_file).extract_qa_info()
    if segment_size is None:
        segment_size = len(qa_info)
    data_list = []
    segment_list = []
    i = 1
    file_name = config.erp_file.stem
    file_extension = config.erp_file.suffix

    for index, item in enumerate(qa_info):
        segment_list.append(item)
        if len(segment_list) == segment_size or (index == len(qa_info) - 1 and segment_list):
            data_dict_name = f'{file_name}{file_extension}' \
                if segment_size == len(qa_info) else f'{file_name}.{i}{file_extension}'
            data_dict = create_data_dict(data_dict_name, segment_list)
            data_list.append(data_dict)
            segment_list = []
            i += 1

    return data_list


def upload_data_list(data_list):
    upload_dify = DifyPlatform(api_config=config.api_config('dev'))
    kb_name = config.erp_dataset
    kb = KnowledgeBase(upload_dify.dataset_api, upload_dify.get_dataset_id_by_name(kb_name), kb_name, upload_dify.record_db)
    kb.add_document(data_list)


def main():
    data_list = get_data_list()
    print('Data length:', len(data_list))
    start = datetime.datetime.now()
    upload_data_list(data_list)
    end = datetime.datetime.now()
    duration = (end - start).total_seconds()
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"cost time: {minutes} min {seconds} sec")


if __name__ == '__main__':
    main()
