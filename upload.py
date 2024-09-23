import datetime

from src.api.dify_api import DifyApi
from src.services.knowledge_base import KnowledgeBase
from src.utils.config import config
from src.utils.excel_handler import ExcelHandler


def get_customer_data(content_size: int):
    df = ExcelHandler().read_excel(config.data_location / 'ucid_basic.xlsx')
    sub_df = df.copy()
    sub_df.loc[:, :].fillna('', inplace=True)
    data_list = []
    temp_list = []
    i = 1

    for index, row in sub_df.iterrows():
        item = f"# CN_Name\n{row['CN_Name']}\n# EN_Name\n{row['EN_Name']}\n# keywords\n{row['keywords']}"
        temp_list.append(item)
        if len(temp_list) == content_size:
            data_dict_name = row['CN_Name'] if content_size == 1 else f"customers_{i}"
            data_dict = {
                "name": data_dict_name,
                "segment": {
                    "content": '\n\n---\n\n'.join(temp_list)
                }
            }
            data_list.append(data_dict)
            temp_list = []
            i += 1

        if index == sub_df.index[-1] and temp_list:
            data_dict_name = row['CN_Name'] if content_size == 1 else f"customers_{i}"
            data_dict = {
                "name": data_dict_name,
                "segment": {
                    "content": '\n\n---\n\n'.join(temp_list)
                }
            }
            data_list.append(data_dict)

    return data_list


def upload_customer_data(customer_data):
    api = DifyApi(config.upload['url'], config.upload['secret_key'])
    kb = KnowledgeBase(api, 'Customer Name Knowledge Base')
    start = datetime.datetime.now()
    kb.upload_document(customer_data)
    end = datetime.datetime.now()
    duration = (end - start).total_seconds()
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"cost time: {minutes} min {seconds} sec")


def main():
    customer_data = get_customer_data(content_size=1)
    upload_customer_data(customer_data)


if __name__ == '__main__':
    main()
