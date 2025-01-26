from pathlib import Path

from src.services.app import App


class ImageAgent(App):
    def __init__(self, app_api):
        super(ImageAgent, self).__init__(app_api)

    def extract_image_info(self, image_path: Path) -> dict:
        try:
            response = self.query_app(
                user_input='image',
                streaming_mode=True,
                files=[image_path],
                parse_json=True
            )
            return response

        except Exception as e:
            print(e)
