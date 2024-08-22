from pathlib import Path

from src.api.dify_api import DifyApi
from src.services.knowledge_base import KnowledgeBase
from src.utils.config import config


def get_source_knowledge_base_documents():
    source_api = DifyApi(config.mapping['url']['source'], config.mapping['secret_key']['source'])
    for kb_mapping in config.mapping['knowledge_base']:
        source_kb = KnowledgeBase(source_api, kb_mapping['source'])
        source_kb.save_knowledge_base_info_to_db()
        source_kb.get_documents_segments_from_api(sync_to_database=True)


def sync_documents_to_target_knowledge_base():
    source_api = DifyApi(config.mapping['url']['source'], config.mapping['secret_key']['source'])
    target_api = DifyApi(config.mapping['url']['target'], config.mapping['secret_key']['target'])
    for kb_mapping in config.mapping['knowledge_base']:
        source_kb = KnowledgeBase(source_api, kb_mapping['source'])
        source_documents = source_kb.get_documents_segments_from_db()
        target_kb = KnowledgeBase(target_api, kb_mapping['target'])
        target_kb.add_document(source_documents)


def replace_images_in_target_knowledge_base_documents():
    source_api = DifyApi(config.mapping['url']['source'], config.mapping['secret_key']['source'])
    target_api = DifyApi(config.mapping['url']['target'], config.mapping['secret_key']['target'])
    assets_file_root_path = Path(__file__).parent.absolute() / Path('src/assets')
    word_file_root_path = assets_file_root_path / Path('word_files')

    for kb_mapping in config.mapping['knowledge_base']:
        source_kb = KnowledgeBase(source_api, kb_mapping['source'])
        target_kb = KnowledgeBase(target_api, kb_mapping['target'])

        source_documents = source_kb.get_documents_segments_from_db()
        source_docs_with_images = source_kb.get_images_from_documents(source_documents)
        if source_docs_with_images:
            images_mapping = {}
            for document in source_docs_with_images:
                images_paths = []
                source_images = document['images']
                for uuid in source_images:
                    image_path = assets_file_root_path / source_kb.get_image_path(uuid)
                    images_paths.append(image_path)
                word_file_path = word_file_root_path / f'{document["document_id"]}.docx'
                target_kb.add_images_to_word_file(word_file_path, images_paths)
                word_file_id = target_kb.create_document_by_file(word_file_path)
                word_file_document = target_kb.get_documents_segments_from_api(document_id=word_file_id)
                target_docs_with_images = target_kb.get_images_from_documents(word_file_document)
                images_mapping.update(dict(zip(source_images, target_docs_with_images[0]['images'])))
                target_kb.delete_document(word_file_id)

            target_documents = target_kb.get_documents_segments_from_api()
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
