from src.services.dify_platform import DifyPlatform
from src.utils.config import config
from src.utils.docx_handler import DocxHandler
from src.utils.time_utils import timing


def create_data_dict(name, segment_list):
    return {
        'name': name,
        'segment': [
            {
                'content': f'# Question\n{segment["question"]}\n\n# Context\n{segment["context"]}\n\n# Answer\n{segment["answer"]}',
                'keywords': [segment['context']],
                'enabled': True
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


@timing
def upload_data_list(data_list):
    upload_dify = DifyPlatform('dev')
    kb_name = config.erp_dataset
    kb = upload_dify.init_knowledge_base(kb_name)
    kb.add_document(data_list, replace_listed=True)


def main():
    data_list = get_data_list()
    print('Data length:', len(data_list))
    upload_data_list(data_list)


if __name__ == '__main__':
    main()
