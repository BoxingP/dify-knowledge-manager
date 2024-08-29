from src.api.dify_api import DifyApi
from src.services.knowledge_base import KnowledgeBase
from src.utils.config import config


def get_source_knowledge_base_documents():
    source_api = DifyApi(config.mapping['url']['source'], config.mapping['secret_key']['source'])
    for kb_mapping in config.mapping['knowledge_base']:
        source_kb = KnowledgeBase(source_api, kb_mapping['source'])
        source_kb.save_knowledge_base_info_to_db()
        source_documents = source_kb.get_documents(source='api', with_segment=True)
        source_kb.sync_documents_to_db(source_documents)


def sync_documents_to_target_knowledge_base():
    source_api = DifyApi(config.mapping['url']['source'], config.mapping['secret_key']['source'])
    target_api = DifyApi(config.mapping['url']['target'], config.mapping['secret_key']['target'])
    for kb_mapping in config.mapping['knowledge_base']:
        source_kb = KnowledgeBase(source_api, kb_mapping['source'])
        source_documents = source_kb.get_documents(source='db', with_segment=True)
        target_kb = KnowledgeBase(target_api, kb_mapping['target'])
        target_kb.add_document(source_documents)


def replace_images_in_target_knowledge_base_documents():
    source_api = DifyApi(config.mapping['url']['source'], config.mapping['secret_key']['source'])
    target_api = DifyApi(config.mapping['url']['target'], config.mapping['secret_key']['target'])

    for kb_mapping in config.mapping['knowledge_base']:
        source_kb = KnowledgeBase(source_api, kb_mapping['source'])
        target_kb = KnowledgeBase(target_api, kb_mapping['target'])

        source_docs = source_kb.get_documents(source='db', with_segment=True, with_image=True)
        source_docs_with_images = list(filter(lambda item: item['image'], source_docs))
        if source_docs_with_images:
            images_mapping = target_kb.upload_images_to_knowledge_base(source_docs_with_images)
            target_documents = target_kb.get_documents(source='api', with_segment=True)
            for document in target_documents:
                for segment in document['segment']:
                    origin_segment_content = segment['content']
                    for key, value in images_mapping.items():
                        segment['content'] = segment['content'].replace(key, value)
                    if segment['content'] != origin_segment_content:
                        target_kb.update_segment_in_document(segment)


def main():
    get_source_knowledge_base_documents()
    sync_documents_to_target_knowledge_base()
    replace_images_in_target_knowledge_base_documents()


if __name__ == '__main__':
    main()
