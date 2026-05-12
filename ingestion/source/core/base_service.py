import os

class BaseService:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID", "default-project-id")
        self._init_auth()

    def _init_auth(self):
        # Logic Auth (ADC)
        pass
