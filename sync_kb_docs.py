from src.services.dify_platform import DifyPlatform
from src.utils.config import config
from src.utils.time_utils import timing


def filter_documents(documents):
    filtered = []
    if documents is None:
        return filtered
    for document in documents:
        if all(segment['status'] == 'completed' for segment in document['segment']):
            filtered.append(document)
    return filtered


@timing
def sync_documents_to_target_knowledge_base(source_kb, target_kb, sync_config, source='api', record_documents=True):
    source_documents = source_kb.fetch_documents(source=source, with_segment=True, is_enabled=True)
    filtered_documents = filter_documents(source_documents)
    print(
        f"Fetching completed: {len(filtered_documents)} source files in dataset '{source_kb.dataset_name}' from '{source}'"
    )
    if filtered_documents:
        target_kb.sync_documents(documents=filtered_documents, sync_config=sync_config)
        if record_documents:
            source_kb.record_knowledge_base_info()
            source_kb.record_documents(filtered_documents)


@timing
def replace_images_in_target_knowledge_base_documents(source_dify, target_dify, dataset_mapping, source='db'):
    for mapping in dataset_mapping:
        source_kb = source_dify.init_knowledge_base(mapping.get('source'))
        target_kb = target_dify.init_knowledge_base(mapping.get('target'))
        source_docs = source_kb.fetch_documents(source=source, with_segment=True, with_image=True)
        source_docs_with_images = list(filter(lambda item: item['image'], source_docs))
        if source_docs_with_images:
            images = [image for item in source_docs_with_images for image in item['image']]
            local_images = source_dify.download_images_to_local(images, skip_if_exists=True)
            docs_with_images = [
                {'id': doc['id'], 'image': [local_images[uuid] for uuid in doc['image'] if uuid in local_images]} for
                doc in source_docs_with_images if 'id' in doc and 'image' in doc]
            target_images = {}
            for doc in docs_with_images:
                images_path_to_id = target_kb.upload_images(images_path=doc['image'], doc_name=doc['id'])
                target_images.update(images_path_to_id)
            images_mapping = {k: target_images[v] for k, v in local_images.items()}
            if not images_mapping:
                print(f"No images to replace in target dataset '{target_kb.dataset_name}'")
                continue

            target_documents = target_kb.fetch_documents(source='api', with_segment=True)
            for document in target_documents:
                for segment in document['segment']:
                    origin_segment_content = segment['content']
                    for key, value in images_mapping.items():
                        segment['content'] = segment['content'].replace(key, value)
                    if segment['content'] != origin_segment_content:
                        target_kb.update_segment_in_document(segment)


def main():
    doc_sync_config = config.get_doc_sync_config(scenario='dataset')

    source_dify = DifyPlatform('dev')
    target_dify = DifyPlatform('dev')
    for mapping in doc_sync_config.dataset_mapping:
        source_kb = source_dify.init_knowledge_base(mapping.get('source'))
        target_kb = target_dify.init_knowledge_base(mapping.get('target'))
        sync_documents_to_target_knowledge_base(source_kb, target_kb, sync_config=doc_sync_config, source='db')
    replace_images_in_target_knowledge_base_documents(source_dify, target_dify, doc_sync_config.dataset_mapping)


if __name__ == '__main__':
    main()
