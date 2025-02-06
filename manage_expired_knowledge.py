from src.services.dify_platform import DifyPlatform
from src.utils.config import config


def filter_document_by_tags(documents: list[dict], tags: list[str]) -> list[dict]:
    filtered_documents = []
    for document in documents:
        parts = document.get('name').split('.', 1)
        if len(parts) > 1 and any(tag in parts[1].split('.') for tag in tags):
            filtered_documents.append(document)
    return filtered_documents


def main():
    platform = DifyPlatform(env='dev', include_dataset=True)
    action = config.expired_action
    datasets = config.expired_datasets
    tags = config.expired_tags
    for dataset in datasets:
        kb = platform.init_knowledge_base(dataset)
        documents = [{'id': doc.get('id'), 'name': doc.get('name')}
                     for doc in kb.fetch_documents(source='db', with_segment=False)]
        filtered_documents = filter_document_by_tags(documents, tags)
        filtered_doc_ids = [doc.get('id') for doc in filtered_documents]
        if filtered_doc_ids:
            if action == 'delete':
                print('Backup documents...')
                kb.backup_documents(document_ids=filtered_doc_ids, source='db')
                print('Delete documents...')
                kb.delete_documents(document_ids=filtered_doc_ids)
            elif action == 'disable':
                print('Disable documents...')
                kb.disable_documents(filtered_doc_ids, source='db')
            else:
                raise ValueError(f'Invalid action: {action}')
        else:
            print('No documents to process')


if __name__ == '__main__':
    main()
