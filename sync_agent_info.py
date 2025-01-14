from src.database.ab_database import AbDatabase
from src.database.record_database import RecordDatabase
from src.services.dify_platform import DifyPlatform
from src.utils.config import config


def generate_segment(row):
    content = (
        f"# abid\n{row['abid']}\n\n"
        f"# name\n{row['name']}\n\n"
        f"# country\n{row['country']}\n\n"
        f"# category\n{row['category']}\n\n"
        f"# description\n{row['description']}\n\n"
        f"# remark\n{row['remark']}\n\n"
    )
    return {'content': content, 'enabled': True}


def update_agent_info(agent_info):
    dify = DifyPlatform('dev')
    kb_name = 'Agent pool'
    kb = dify.init_knowledge_base(kb_name)
    kb.sync_documents(
        documents={'name': 'agent_info', 'segment': agent_info.apply(generate_segment, axis=1).to_list()},
        sync_config=config.get_doc_sync_config(scenario='agent')
    )


def main():
    agent_info_in_ab = AbDatabase('ab').get_agent_info()
    record_db = RecordDatabase('record')
    record_db.save_agent_info(agent_info_in_ab)
    agent_info_in_record = record_db.get_agent_info()
    update_agent_info(agent_info_in_record)


if __name__ == '__main__':
    main()
