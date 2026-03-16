import os
import json
from math import log1p
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from openai import OpenAI
from config.ClickHouseConfig import ClickHouseConfig as conn

load_dotenv("app.env")


class AutoReport:

    def __init__(self):
        self.clk   = conn().getClient_prod()
        self.client = OpenAI(
            api_key=os.getenv("MISTRAL_API_KEY"),
            base_url="https://api.mistral.ai/v1"
        )
        self.model    = "mistral-small-latest"
        self.table    = "dev_reporting_agg"
        self.days_map = {
            1: "Lundi", 2: "Mardi", 3: "Mercredi", 4: "Jeudi",
            5: "Vendredi", 6: "Samedi", 7: "Dimanche"
        }

    def _execute_query(self, query, params=None):
        result = self.clk.query(query, parameters=params or {})
        return [dict(zip(result.column_names, r)) for r in result.result_rows]

    def get_candidates(self, adv_ids):

        now = datetime.now()
        valid_months = set()
        m, y = now.month, now.year
        for _ in range(12):
            valid_months.add((y, m))
            m -= 1
            if m == 0:
                m, y = 12, y - 1

        ids_str = ", ".join(str(i) for i in adv_ids)
        query = f"""
            SELECT
                r.adv_id,r.database_id,r.age_gender_isp  AS segment,t.tag,toDayOfWeek(parseDateTimeBestEffort(ds)) AS day,
                toMonth(parseDateTimeBestEffort(ds)) AS month,toYear(parseDateTimeBestEffort(ds)) AS year,toHour(r.date_event) AS hour,
                SUM(r.sends)    AS sends,SUM(r.openers)  AS openers,SUM(r.clickers) AS clickers,SUM(r.unsubs)   AS unsubs
            FROM {self.table} r
            ARRAY JOIN r.date_schedule AS ds
            LEFT JOIN tags t ON r.tag_id = t.id
            WHERE r.adv_id IN ({ids_str})
            GROUP BY r.adv_id, segment, tag, day, hour, database_id, month, year
        """
        rows = self._execute_query(query)
        best = {}
        for r in rows:
            if (r["year"], r["month"]) not in valid_months:
                continue
            sends = r["sends"]
            if sends < 50:
                continue
            ctr = (r["clickers"] / sends) * 100
            open_rate  = (r["openers"]  / sends) * 100
            unsub_rate = (r["unsubs"]   / sends) * 100
            score = (ctr * 0.5 + open_rate * 0.3 - unsub_rate * 0.2) * log1p(sends)
            candidat = {
                "advertiser": r["adv_id"],
                "base":       r["database_id"],
                "segment":    r["segment"],
                "tag":        r["tag"] or "General",
                "jour":       self.days_map[r["day"]],
                "heure":      f"{str(int(r['hour'])).zfill(2)}:00",
                "score":      round(score, 4),
            }
            key = (candidat["advertiser"], candidat["segment"], candidat["jour"])
            if key not in best or candidat["score"] > best[key]["score"]:
                best[key] = candidat
        return list(best.values())

    def get_adv_ids(self) -> list[int]:
        current_month=datetime.now().month
        current_month=current_month
        query = f"""
            SELECT DISTINCT adv_id
            FROM {self.table} ARRAY JOIN date_schedule as ds WHERE toMonth(parseDateTimeBestEffort(ds))={current_month}
        """
        rows = self._execute_query(query)
        return [r["adv_id"] for r in rows]

    CHUNK_SIZE   = 50   
    MAX_WORKERS  = 4   

    def _build_prompt(self, candidats: list) -> str:
        return f"""Tu es un moteur de planification marketing.
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
                        "segment": "",
                        "heure": "",
                        "score": ""
                    }}
                    ]
                }}
                ]
            }}
            ]"""

    def _call_mistral(self, candidats: list) -> list:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": self._build_prompt(candidats)}]
        )
        return self._parse_json(response.choices[0].message.content)

    def generate_schedule(self, candidats: list) -> list:
        chunks = [
            candidats[i:i + self.CHUNK_SIZE]
            for i in range(0, len(candidats), self.CHUNK_SIZE)
        ]
        print(f"[AutoReport] {len(chunks)} chunk(s) × {self.CHUNK_SIZE} candidats max — {self.MAX_WORKERS} workers")
        results = []
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {executor.submit(self._call_mistral, chunk): i for i, chunk in enumerate(chunks)}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"[AutoReport] Chunk {idx} échoué : {e}")
        return self._merge_schedules(results)

    def _merge_schedules(self, schedules: list[list]) -> list:
        merged: dict[str, list] = defaultdict(list)
        for schedule in schedules:
            for day in schedule:
                merged[day["jour"]].extend(day.get("advertisers", []))
        return [
            {"jour": jour, "advertisers": advs}
            for jour, advs in merged.items()
        ]

    def _parse_json(self, text: str):
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())

    def validate_schedule(self, schedule):
        seen = set()
        for day in schedule:
            jour = day["jour"]
            for adv in day["advertisers"]:
                for reco in adv["recommandation"]:
                    key = (adv["advertiser"], reco["segment"], jour)
                    if key in seen:
                        return False, f"doublon : advertiser {adv['advertiser']} segment {reco['segment']} le {jour}"
                    seen.add(key)
        return True, None

    def correct_schedule(self, schedule, error):
        prompt = f"""Le planning contient une erreur : {error}
        Corrige-le sans violer les règles.
        Réponds uniquement en JSON valide, sans texte ni balises markdown.
        Planning :
        {json.dumps(schedule, indent=2)}"""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content

    def generate_reporting(self):
        current_date = datetime.now()
        adv_ids = self.get_adv_ids()
        print(f"[AutoReport] {len(adv_ids)} advertisers actifs ce mois")
        candidats = self.get_candidates(adv_ids)
        if not candidats:
            print("[AutoReport] Aucun candidat trouvé.")
            return []
        print(f"[AutoReport] {len(candidats)} candidats (après déduplication)")
        schedule = self.generate_schedule(candidats)
        valid, error = self.validate_schedule(schedule)
        if not valid:
            print(f"[AutoReport] Correction : {error}")
            try:
                schedule = self._parse_json(self.correct_schedule(schedule, error))
            except json.JSONDecodeError:
                pass
        elapsed = (datetime.now() - current_date).total_seconds()
        print(f"[AutoReport] Terminé en {elapsed:.1f}s — {len(schedule)} jour(s) planifiés")
        return schedule