from typing import Optional

from src.services.studio import Studio


class KeywordsAgent(Studio):
    def __init__(self, app_pai):
        super(KeywordsAgent, self).__init__(app_pai)

    def get_keywords(self, text: str, default_keywords: Optional[list[str]] = None) -> list:
        if default_keywords is None:
            default_keywords = []
        try:
            response = self.query_app(text, parse_json=True, streaming_mode=True)
            if not response:
                return default_keywords
            if isinstance(response, dict):
                return response.get('keywords', default_keywords)
            elif isinstance(response, str) and not response.strip():
                return default_keywords
        except AttributeError:
            return default_keywords
        return default_keywords
