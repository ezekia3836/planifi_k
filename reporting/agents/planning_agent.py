from openai import OpenAI
import json
import os
from dotenv import load_dotenv

load_dotenv("app.env")

class PlanningAgent:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("MISTRAL_API_KEY"),
            base_url="https://api.mistral.ai/v1"
        )
        self.model = "mistral-small-latest"

    def _build_prompt(self, candidats):
        return f"""
            Tu es un moteur de planification marketing.
            Choisis les meilleurs envois parmi les candidats fournis.
            Règles STRICTES :
            1. Utiliser uniquement les candidats fournis — ne pas inventer de bases ou segments.
            2. Un advertiser ne peut pas avoir deux envois le même jour pour un même segment.
            3. Priorise les scores les plus élevés.
            4. Réponds uniquement en JSON valide, sans texte avant ou après, sans balises markdown.

            Candidats :
            {json.dumps(candidats, indent=2)}

            Format attendu :
            [
            {{
                "jour": "",
                "advertisers": [
                {{
                    "advertiser": "",
                    "tags": "",
                    "recommandation": [
                    {{
                        "base": "",
                        "age": "",
                        "gender":"",
                        "isp":"",
                        "heure": "",
                        "mois":"",
                        "country":"",
                        "currency":"",
                        "score": ""
                    }}
                    ]
                }}
                ]
            }}
            ]
            """
    def _call_mistral(self, candidats):
        response = self.client.chat.completions.create(model=self.model,messages=[{"role": "user", "content": self._build_prompt(candidats)}])
        return self._parse_json(response.choices[0].message.content)

    def _parse_json(self, text):
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())

    def generate_schedule(self, candidats):
        schedule = self._call_mistral(candidats)
        return schedule