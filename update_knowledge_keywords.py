import os

from src.services.dify_platform import DifyPlatform
from src.utils.config import config


def get_dataset_document_mapping(platform: DifyPlatform, datasets: list, documents: list):
    kb_objects = []
    if datasets:
        for dataset in datasets:
            kb_objects.append(platform.init_knowledge_base(dataset))
    else:
        for dataset in platform.datasets:
            kb_objects.append(platform.init_knowledge_base(dataset['name']))

    mapping = []
    for kb in kb_objects:
        documents_in_kb = kb.fetch_documents(source='db', with_segment=False)
        if documents:
            for document in documents:
                document_id = kb.get_document_id_by_name(document, documents_in_kb)
                if document_id is not None:
                    mapping.append({'dataset': kb.dataset_name, 'document_ids': [document_id]})
        else:
            mapping.append({
                'dataset': kb.dataset_name,
                'document_ids': [doc['id'] for doc in documents_in_kb]
            })
    return mapping


def main():
    platform = DifyPlatform(env='dev', include_dataset=True)
    dataset_document_mapping = get_dataset_document_mapping(
        platform, config.keywords_datasets, config.keywords_documents
    )
    if dataset_document_mapping:
        keywords_agent = DifyPlatform(env='dev', apps=['keywords'], include_dataset=False).studio.get_app('keywords')
        for item in dataset_document_mapping:
            kb = platform.init_knowledge_base(item['dataset'])
            for doc_id in item['document_ids']:
                document = kb.fetch_documents(source='db', document_id=doc_id, with_segment=True)
                print(f"Updating keywords for document '{document['name']}' in dataset '{kb.dataset_name}'")
                for segment in document['segment']:
                    default_keywords = segment['keywords'] + [os.path.splitext(document['name'])[0]]
                    segment['keywords'] = keywords_agent.get_keywords(
                        text=segment['content'], default_keywords=default_keywords
                    )
                    kb.update_segment_in_document(segment)


if __name__ == '__main__':
    main()
