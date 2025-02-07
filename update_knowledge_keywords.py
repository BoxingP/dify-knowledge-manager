import os

from src.database.record_database import RecordDatabase
from src.services.dify_platform import DifyPlatform
from src.utils.config import config
from src.utils.hash_calculator import HashCalculator


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


def generate_keywords(text: str, re_generate_keywords: bool) -> list:
    record_db = RecordDatabase('record')
    hash_calculator = HashCalculator()
    text_hash = hash_calculator.calculate_text_hash(text)

    if not re_generate_keywords:
        keywords = record_db.get_keywords(hash_value=text_hash, algorithm=hash_calculator.algorithm)
        if keywords:
            return keywords
    keywords_agent = DifyPlatform(env='dev', apps=['keywords'], include_dataset=False).studio.get_app('keywords')
    keywords = keywords_agent.get_keywords(text=text)
    record_db.save_keywords(
        hash_value=text_hash,
        keywords=keywords,
        algorithm=hash_calculator.algorithm
    )
    return keywords


def main():
    re_generate_keywords = False

    platform = DifyPlatform(env='dev', include_dataset=True)
    dataset_document_mapping = get_dataset_document_mapping(
        platform, config.keywords_datasets, config.keywords_documents
    )
    if dataset_document_mapping:
        for item in dataset_document_mapping:
            kb = platform.init_knowledge_base(item['dataset'])
            for doc_id in item['document_ids']:
                document = kb.fetch_documents(source='db', document_id=doc_id, with_segment=True)
                print(f"Updating keywords for document '{document['name']}' in dataset '{kb.dataset_name}'")
                for segment in document['segment']:
                    default_keywords = segment['keywords'] + [os.path.splitext(document['name'])[0]]
                    keywords = generate_keywords(text=segment['content'], re_generate_keywords=re_generate_keywords)
                    if keywords:
                        segment['keywords'] = keywords
                    else:
                        segment['keywords'] = default_keywords
                    original_status = segment.get('enabled')
                    if original_status:
                        kb.update_segment_in_document(segment)
                    else:
                        segment['enabled'] = True
                        kb.update_segment_in_document(segment)
                        segment['enabled'] = False
                        kb.update_segment_in_document(segment)


if __name__ == '__main__':
    main()
