from src.api.dify_api import DifyApi
from src.database.ai_database import AiDatabase
from src.utils.config import config


def get_knowledge_base_documents():
    source_url = config.mapping['url']['source']
    source_api = DifyApi(source_url, config.mapping['secret_key']['source'])
    ai_database = AiDatabase()
    for item in config.mapping['knowledge_base']:
        dataset_id = source_api.get_dataset_id_by_name(item['source'])
        knowledge_base = {'id': dataset_id, 'url': source_url, 'name': item['source']}
        ai_database.save_knowledge_base_info(knowledge_base)
        documents = source_api.get_documents_in_dataset(knowledge_base['id'])
        for document in documents:
            document['dataset_id'] = knowledge_base['id']
            ai_database.save_document(document)
            segments = source_api.get_segments_from_document(knowledge_base['id'], document['id'])
            ai_database.delete_no_exist_segment(document['id'], [segment['id'] for segment in segments])
            for segment in segments:
                keywords = segment['keywords']
                keywords.sort()
                segment['keywords'] = ','.join(keywords)
                ai_database.save_document_segment(segment)


def get_document_id_by_name(name, documents):
    for document in documents:
        if document['name'].rpartition('.')[0].lower() == name.lower():
            return document['id']
    return None


def upload_document_to_knowledge_base(api, knowledge_base, documents: list, is_replace_document=True):
    dataset_id = api.get_dataset_id_by_name(knowledge_base)
    exist_documents = api.get_documents_in_dataset(dataset_id)
    for document in documents:
        document_name_without_extension = document['name'].rpartition('.')[0]
        if is_replace_document:
            document_id = get_document_id_by_name(document_name_without_extension, exist_documents)
            if document_id is not None:
                api.delete_document(dataset_id, document_id)
        document_id = api.create_document(dataset_id, document_name_without_extension)
        sorted_segments = sorted(document['segment'], key=lambda x: x['position'])
        for segment in sorted_segments:
            api.create_segment_in_document(dataset_id, document_id,
                                           segment['content'], segment['answer'], segment['keywords'])


def add_documents_to_knowledge_base():
    source_url = config.mapping['url']['source']
    target_url = config.mapping['url']['target']
    target_api = DifyApi(target_url, config.mapping['secret_key']['target'])
    ai_database = AiDatabase()
    for item in config.mapping['knowledge_base']:
        documents = ai_database.get_knowledge_base_documents(source_url, item['source'])
        upload_document_to_knowledge_base(target_api, item['target'], documents)


def main():
    get_knowledge_base_documents()
    add_documents_to_knowledge_base()


if __name__ == '__main__':
    main()
