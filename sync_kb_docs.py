from src.services.dify_platform import DifyPlatform
from src.utils.config import config


def get_documents_from_source_knowledge_base(source_dify):
    for kb_mapping in config.mapping:
        kb_name = kb_mapping['source']
        kb = source_dify.init_knowledge_base(kb_name)
        kb.save_knowledge_base_info_to_db()
        documents = kb.get_documents(source='api', with_segment=True)
        kb.sync_documents_to_db(documents)


def sync_documents_to_target_knowledge_base(source_dify, target_dify):
    for kb_mapping in config.mapping:
        source_kb_name = kb_mapping['source']
        target_kb_name = kb_mapping['target']
        source_kb = source_dify.init_knowledge_base(source_kb_name)
        target_kb = target_dify.init_knowledge_base(target_kb_name)
        source_documents = source_kb.get_documents(source='db', with_segment=True)
        target_kb.add_document(source_documents, sort_document=True)


def replace_images_in_target_knowledge_base_documents(source_dify, target_dify):
    for kb_mapping in config.mapping:
        source_kb_name = kb_mapping['source']
        target_kb_name = kb_mapping['target']
        source_kb = source_dify.init_knowledge_base(source_kb_name)
        target_kb = target_dify.init_knowledge_base(target_kb_name)
        source_docs = source_kb.get_documents(source='db', with_segment=True, with_image=True)
        source_docs_with_images = list(filter(lambda item: item['image'], source_docs))
        if source_docs_with_images:

            images = [image for item in source_docs_with_images for image in item['image']]
            local_images = source_dify.download_images_to_local(images)
            docs_with_images = [
                {'id': doc['id'], 'image': [local_images[uuid] for uuid in doc['image'] if uuid in local_images]} for
                doc in source_docs_with_images if 'id' in doc and 'image' in doc]
            target_images = {}
            for doc in docs_with_images:
                images_path_to_id = target_dify.upload_images_to_dify(
                    images_path=doc['image'], dataset=target_kb, doc_name=doc['id']
                )
                target_images.update(images_path_to_id)
            images_mapping = {k: target_images[v] for k, v in local_images.items()}

            target_documents = target_kb.get_documents(source='api', with_segment=True)
            for document in target_documents:
                for segment in document['segment']:
                    origin_segment_content = segment['content']
                    for key, value in images_mapping.items():
                        segment['content'] = segment['content'].replace(key, value)
                    if segment['content'] != origin_segment_content:
                        target_kb.update_segment_in_document(segment)


def main():
    source_dify = DifyPlatform(
        api_config=config.api_config('prod'),
        s3_config=config.s3_config('prod'),
        dify_db_name='prod'
    )
    target_dify = DifyPlatform(api_config=config.api_config('dev'))
    get_documents_from_source_knowledge_base(source_dify)
    sync_documents_to_target_knowledge_base(source_dify, target_dify)
    replace_images_in_target_knowledge_base_documents(source_dify, target_dify)


if __name__ == '__main__':
    main()
