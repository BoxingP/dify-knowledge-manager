import re

from docx import Document


class DocxHandler(object):
    def __init__(self, file_path):
        self.document = Document(file_path)

    def read(self):
        full_text = ''
        for para in self.document.paragraphs:
            full_text += para.text + '\n'
        return full_text

    def extract_qa_info(self):
        text = self.read()
        pattern = re.compile(r'(question|context|answer):(.*?)(?=(question|context|answer):|$)', re.I | re.S)
        matches = pattern.findall(text)
        qa_dicts = []
        curr_dict = {}
        for match in matches:
            key, value, _ = match
            key = key.lower()
            value = value.strip().replace('_x000D_', '')
            curr_dict[key] = value
            if len(curr_dict) == 3:
                qa_dicts.append(curr_dict)
                curr_dict = {}
        return qa_dicts
