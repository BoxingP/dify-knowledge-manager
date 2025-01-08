from dataclasses import dataclass


@dataclass(frozen=True)
class DocumentSyncConfig(object):
    skip_existing: bool
    replace_existing: bool
    remove_extra: bool
    preserve_document_order: bool
    preserve_segment_order: bool
    dataset_mapping: list
