from math import log1p
from unittest import result
from collections import defaultdict
import numpy as np
from config.ClickHouseConfig import ClickHouseConfig as conn
from datetime import datetime, timedelta

import pandas as pd
class autoreport:
    def __init__(self):
        self.clk = conn().getClient_prod()
        self.table= "dev_reporting_agg"
        self.limit=5
        self.days_map = {1: "Lundi", 2: "Mardi", 3: "Mercredi", 4: "Jeudi", 5: "Vendredi", 6: "Samedi", 7: "Dimanche"}
    def _execute_query(self, query,params=None):
        result = self.clk.query(query,parameters=params or {})
        return [dict(zip(result.column_names, r)) for r in result.result_rows]
    def get_calendrier(self, adv_ids,top_n=3):
        calendar = {day: [] for day in self.days_map.values()}
        current_day = datetime.now().day
        current_month = datetime.now().month
        for adv_id in adv_ids:
            query = """ 
                SELECT r.database_id, r.age_gender_isp AS segment, t.tag, r.country AS country, toDayOfWeek(parseDateTimeBestEffort(ds)) AS day,toMonth(parseDateTimeBestEffort(ds)) AS month,
                    toHour(r.date_event) AS hour,SUM(r.sends) AS sends,SUM(r.openers) AS openers,SUM(r.clickers) AS clickers,
                    SUM(r.unsubs) AS unsubs FROM dev_reporting_agg r ARRAY JOIN r.date_schedule AS ds LEFT JOIN tags t ON r.tag_id = t.id
                WHERE r.adv_id = %(adv_id)s GROUP BY segment, tag, country, day, hour,database_id,month
            """
            rows = self._execute_query(query, params={"adv_id": adv_id})
            df_by_month = [row for row in rows if row["month"]==current_month]
            scored_data = []
            for r in df_by_month:
                sends = r["sends"]
                if sends < 50:
                    continue
                ctr = (r["clickers"] / sends) * 100 if sends else 0
                open_rate = (r["openers"] / sends) * 100 if sends else 0
                unsub_rate = (r["unsubs"] / sends) * 100 if sends else 0
                score = (ctr * 0.5 + open_rate * 0.3 - unsub_rate * 0.2) * log1p(sends)
                scored_data.append({
                    "segment": r["segment"],
                    "tag": r["tag"] or "General",
                    "base":r["database_id"],
                    "country": r["country"],
                    "jour": self.days_map[r["day"]],
                    "heure": f"{str(int(r['hour'])).zfill(2)}:00",
                    "score": round(score, 4)
                })
            recent_data=[
                (19,"O_age_O_gender_O_isp","Ecommerce","Lundi")
            ]
            filtre = [
                s for s in scored_data if (s["base"],s["segment"],s["tag"],s["jour"]) not in recent_data
            ]
            grouped=defaultdict(list)
            for s in filtre:
                key = (s["jour"],s["tag"])
                grouped[key].append(s)
            adv_days = defaultdict(lambda:defaultdict(list))
            for (jour, tag), values in grouped.items():
                best_base_segment={}
                for v in values:
                    key = (v["base"],v["segment"])
                    if key not in best_base_segment or v["score"] > best_base_segment[key]["score"]:
                        best_base_segment[key] = v
                values_filted = list(best_base_segment.values())
                values_sorted = sorted(values_filted, key=lambda x: x["score"], reverse=True)
                top_values = values_sorted[:top_n]
                adv_days[jour][tag] = [
                {
                    "base": v["base"],
                    "segment": v["segment"],
                    "score": v["score"]
                }
                for v in top_values
            ]
            for jour, tags in adv_days.items():
                for tag, recos in tags.items():
                    calendar[jour].append({
                        "advertiser": adv_id,
                        "tags": tag,
                        "recommandation": recos
                    })
        result = []
        for day in self.days_map.values():
            result.append({
                "jour": day,
                "advertisers": calendar[day]
            })
        return result
            
    def generate_reporting(self):
        query=f""" SELECT DISTINCT adv_id FROM {self.table} """
        rows = self._execute_query(query)
        adv_ids=[row["adv_id"] for row in rows]
        report = self.get_calendrier(adv_ids)
        return report