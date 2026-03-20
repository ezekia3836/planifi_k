import json
from openai import OpenAI
import os

class CorrectionAgent:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("MISTRAL_API_KEY"),
            base_url="https://api.mistral.ai/v1")
        self.model = "mistral-small-latest"

    def correct(self, schedule, error):
        print(error)
        prompt = f"""
            Le planning contient une erreur : {error}
            Corrige-le sans violer les règles.
            Réponds uniquement en JSON valide, sans texte ni balises markdown.
            Planning :
            {json.dumps(schedule, indent=2)}
            """
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.choices[0].message.content
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())