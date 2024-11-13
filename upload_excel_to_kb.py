import datetime
import threading
from queue import Queue

from src.services.dify_platform import DifyPlatform
from src.services.knowledge_base import KnowledgeBase
from src.utils.config import config
from src.utils.excel_handler import ExcelHandler


def create_row_string(row):
    result_str = ''
    for col_name in row.keys():
        result_str += f'# {col_name}\n{row[col_name]}\n'
    return result_str


def get_data_list(segment_size: int):
    mark_column = config.upload_file_mark_column
    keywords_column = config.upload_file_keywords_column
    df = ExcelHandler(config.upload_file).read_excel([keywords_column] if keywords_column else None)
    sub_df = df.copy()
    sub_df.loc[:, :].fillna('', inplace=True)
    data_list = []
    temp_list = []
    temp_keywords_list = []
    i = 1

    for index, row in sub_df.iterrows():
        item = create_row_string(row)
        temp_list.append(item)
        if keywords_column:
            temp_keywords_list.extend(row[keywords_column].split(','))
        if len(temp_list) == segment_size:
            data_dict_name = row[mark_column] if segment_size == 1 else f"{mark_column}_{i}"
            data_dict = {
                "name": data_dict_name,
                "segment": {
                    "content": '\n\n---\n\n'.join(temp_list),
                    "keywords": temp_keywords_list
                }
            }
            data_list.append(data_dict)
            temp_list = []
            temp_keywords_list = []
            i += 1

        if index == sub_df.index[-1] and temp_list:
            data_dict_name = row[mark_column] if segment_size == 1 else f"{mark_column}_{i}"
            data_dict = {
                "name": data_dict_name,
                "segment": {
                    "content": '\n\n---\n\n'.join(temp_list),
                    "keywords": temp_keywords_list
                }
            }
            data_list.append(data_dict)

    return data_list


def worker(kb, queue):
    while not queue.empty():
        customer_chunk = queue.get()
        try:
            kb.add_document([customer_chunk], replace_document=False)
            queue.task_done()
        except Exception as ex:
            print(f'An exception occurred: {ex}, re-querying...')
            queue.put(customer_chunk)


def upload_data_list_multithread(customer_data, num_threads):
    upload_dify = DifyPlatform(api_config=config.api_config('dev'))
    kb_name = config.upload_dataset
    kb = KnowledgeBase(upload_dify.dataset_api, upload_dify.get_dataset_id_by_name(kb_name), kb_name, upload_dify.record_db)

    queue = Queue()
    for data_chunk in customer_data:
        queue.put(data_chunk)

    for _ in range(num_threads):
        thread = threading.Thread(target=worker, args=(kb, queue))
        thread.daemon = True
        thread.start()

    queue.join()


def main():
    data_list = get_data_list(segment_size=1)
    print('Data length:', len(data_list))
    start = datetime.datetime.now()
    upload_data_list_multithread(data_list, num_threads=3)
    end = datetime.datetime.now()
    duration = (end - start).total_seconds()
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"cost time: {minutes} min {seconds} sec")


if __name__ == '__main__':
    main()
