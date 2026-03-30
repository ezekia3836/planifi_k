from openai import OpenAI
import json
import os
from dotenv import load_dotenv
load_dotenv("app.env")

class Agent:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("MISTRAL_API_KEY"),
            base_url="https://api.mistral.ai/v1"
        )
        self.model = "mistral-small-latest"

    def build_prompt(self,result):
        pass