from collections import defaultdict
import csv
from unittest import result
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
import math
import pandas as pd
import os
import re

import base64
from reporting.auto import data
CSV_DIR="csv_segments"
class Query2:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.analyze = analyse()
        self.table = "prod_reporting"
        self.segment_index = self.build_segment_index()
    
    def _execute_query(self, query,params=None):
        result = self.clk.query(query,parameters=params or {})
        return [dict(zip(result.column_names, r)) for r in result.result_rows]
    
    def safe_float(self,value, default=0.0):
        try:
            v = float(value)
            if math.isnan(v) or math.isinf(v):
                return default
            return v
        except Exception:
            return default

    def safe_int(self,value, default=0):
        try:
            return int(value or default)
        except Exception:
            return default
    
    def age_sort_key(self,age_range: str):
        if not age_range:
            return 9999
        age_range = str(age_range).strip()

        match_plus = re.match(r"^(\d+)\+$", age_range)
        if match_plus:
            return int(match_plus.group(1))
        match_range = re.match(r"^(\d+)\s*-\s*(\d+)$", age_range)
        if match_range:
            return int(match_range.group(1))
        return 9999
    
    def global_advertiser(self, adv_id):
        def compute_rates(sends, openers, clickers, unsubs):
            return (
                round(clickers / sends * 100, 3) if sends else 0,
                round(openers / sends * 100, 3) if sends else 0,
                round(unsubs / sends * 100, 3) if sends else 0,
                round(clickers / openers * 100, 3) if openers else 0
            )

        def push_dim(target, dim, value, sends, clicks, clickers, opens, openers, unsubs):
            if not value:
                return

            seg = target["dimensions"][dim].setdefault(value, {
                "sends": 0,
                "clicks": 0,
                "clickers": 0,
                "opens": 0,
                "openers": 0,
                "unsubs": 0
            })

            seg["sends"] += sends
            seg["clicks"] += clicks
            seg["clickers"] += clickers
            seg["opens"] += opens
            seg["openers"] += openers
            seg["unsubs"] += unsubs
            tc, to, tu, cto = compute_rates(
                seg["sends"], seg["openers"], seg["clickers"], seg["unsubs"]
            )

            seg["taux_clickers"] = tc
            seg["taux_openers"] = to
            seg["taux_unsubs"] = tu
            seg["taux_cto"] = cto
            seg["analyses"] = {
                "taux_clickers": self.analyze.analyze_click_rate(tc),
                "taux_cto": self.analyze.analyze_cto_rate(cto, seg["openers"]),
                "taux_unsubs": self.analyze.analyze_unsub_rate(tu)
            }
        query = f"""
            SELECT
            adv_id,
            database_id,
            gender,
            age_range,
            main_isp,
            dep AS departement,
            any(id_routers)        AS id_routers,
            any(tag_id)            AS tag_id,
            any(brand)             AS brand,
            any(optimized)         AS optimized,
            any(subject)           AS subject,
            any(comment)           AS comment,
            any(date_schedule)     AS date_schedule,
            sum(sends)     AS sends,
            sum(clicks)    AS clicks,
            sum(clickers)  AS clickers,
            sum(opens)     AS opens,
            sum(openers)   AS openers,
            sum(unsubs)    AS unsubs,
            max(ca) AS ca,
            groupUniqArray(segmentId) AS segmentId,
            groupUniqArray(ListId) AS ListId,
            groupUniqArray(ListName) AS ListName
        FROM {self.table}
        WHERE adv_id = %(adv_id)s
        GROUP BY
            adv_id,
            database_id,
            gender,
            age_range,
            main_isp,
            dep
        """
        rows = self._execute_query(query, {"adv_id": adv_id})

        if not rows:
            return {"advertiser_id": str(adv_id), "globales": {}, "bases": []}

        bases = {}
        analyse_dep = {}
        total = dict(sends=0, clicks=0, clickers=0, opens=0, openers=0, unsubs=0, ca=0)
        for r in rows:

            base = bases.setdefault(r["database_id"], {
                "database_id": r["database_id"],
                "subject": r["subject"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "brands": [],
                "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
            })

            sends = r["sends"] or 0
            clicks = r["clicks"] or 0
            clickers = r["clickers"] or 0
            opens = r["opens"] or 0
            openers = r["openers"] or 0
            unsubs = r["unsubs"] or 0
            ca = r["ca"] or 0

            base["sends"] += sends
            base["clicks"] += clicks
            base["clickers"] += clickers
            base["opens"] += opens
            base["openers"] += openers
            base["unsubs"] += unsubs

            push_dim(base, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(base, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(base, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)

            dep = r.get("departement") or "others"
            if dep == "dep_vide":
                dep = "others"

            d = analyse_dep.setdefault(dep, {"sends": 0,"clickers": 0, "openers": 0, "unsubs": 0})
            d["sends"] += sends
            d["clickers"] += clickers
            d["openers"] += openers
            d["unsubs"] += unsubs

            brand_key = (r["brand"], r["id_routers"], r["tag_id"])

            brand = next(
                (b for b in base["brands"]
                if (b["name"], b["id_routers"], b["tag_id"]) == brand_key),
                None
            )
            listid = r.get("ListId") or []
            listName = r.get("ListName") or []
            segmentids = r.get("segmentId") or []
            if isinstance(segmentids, str):
                segmentids = [segmentids]
            elif isinstance(segmentids, int):
                segmentids = [segmentids]
            elif segmentids is None:
                segmentids = []
            elif not isinstance(segmentids, list):
                segmentids = list(segmentids)
            segmentids = [s for s in segmentids if s not in (0,"0","",None)]
                
            if not brand:
                brand = {
                    "name": r["brand"],
                    "id_routers": r["id_routers"],
                    "tag_id": r["tag_id"],
                    "creativities": r.get("optimized") or "",
                    "comment": (r.get("comment") or "").replace("None", ""),
                    "subject": r["subject"],
                    "segment_id": list(set(segmentids)),
                    "ListId":list(set(listid)),
                    "ListName": list(set(listName)),
                    "date_schedule": r.get("date_schedule"),
                    "sends": 0,
                    "clicks": 0,
                    "clickers": 0,
                    "opens": 0,
                    "openers": 0,
                    "unsubs": 0,
                    "ca": 0,
                    "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
                }
                base["brands"].append(brand)
            existing = set(brand.get("segment_id") or [])
            new_segments = set(segmentids)
            brand["segment_id"] = list(existing | new_segments)
            brand["sends"] += sends
            brand["clicks"] += clicks
            brand["clickers"] += clickers
            brand["opens"] += opens
            brand["openers"] += openers
            brand["unsubs"] += unsubs
            brand["ca"] = max(brand["ca"], ca)
            brand["taux_clickers"] = round(brand["clickers"] / brand["sends"] * 100, 5) if brand["sends"] else 0
            brand["taux_openers"] = round(brand["opens"] / brand["sends"] * 100, 5) if brand["sends"] else 0
            brand["taux_unsubs"] = round(brand["unsubs"] / brand["sends"] * 100, 5) if brand["sends"] else 0
            brand["taux_cto"] = round(brand["clickers"] / brand["opens"] * 100, 5) if brand["opens"] else 0
            brand["ecpm"] = round(brand["ca"] / brand["sends"] * 1000, 3) if brand["sends"] else 0
                        
            push_dim(brand, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)

            total["sends"] += sends
            total["clicks"] += clicks
            total["clickers"] += clickers
            total["opens"] += opens
            total["openers"] += openers
            total["unsubs"] += unsubs

            total_sends = total["sends"]

            for dep, stats in analyse_dep.items():
                stats["taux_clickers"] = round(stats.get("clickers", 0) / total_sends * 100, 4) if total_sends else 0
                stats["taux_openers"] = round(stats.get("openers", 0) / total_sends * 100, 4) if total_sends else 0
                stats["taux_unsubs"] = round(stats.get("unsubs", 0) / total_sends * 100, 4) if total_sends else 0


        for base in bases.values():
            base["ca"] = sum(b["ca"] for b in base["brands"])

        total["ca"] = sum(
            b["ca"]
            for base in bases.values()
            for b in base["brands"]
        )

        result = []
        for base in bases.values():

            tc, to, tu, cto = compute_rates(
                base["sends"], base["openers"], base["clickers"], base["unsubs"]
            )

            ecpm = round(base["ca"] / base["sends"] * 1000, 3) if base["sends"] else 0
            classification = self.analyze.classify_advertiser(ecpm, tc)
            result.append({
                "database_id": base["database_id"],
                "brands": base["brands"],
                "sends": base["sends"],
                "clicks": base["clicks"],
                "clickers": base["clickers"],
                "opens": base["opens"],
                "openers": base["openers"],
                "unsubs": base["unsubs"],
                "taux_clickers": tc,
                "taux_openers": to,
                "taux_unsubs": tu,
                "taux_cto": cto,
                "ecpm": ecpm,
                "ca": base["ca"],
                "classification": classification,
                "dimensions": base["dimensions"]
            })

        tc_g, to_g, tu_g, cto_g = compute_rates(
            total["sends"], total["openers"], total["clickers"], total["unsubs"]
        )

        ecpm_g = round(total["ca"] / total["sends"] * 1000, 3) if total["sends"] else 0

        return {
            "advertiser_id": str(adv_id),
            "globales": {
                **total,
                "ecpm": ecpm_g,
                "taux_clickers": tc_g,
                "taux_openers": to_g,
                "taux_unsubs": tu_g,
                "taux_cto": cto_g,
                "analyses": {
                    "taux_clickers": self.analyze.analyze_click_rate(tc_g),
                    "taux_cto": self.analyze.analyze_cto_rate(cto_g, total["openers"]),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(tu_g)
                },
                "analyse_dep": analyse_dep
            },
            "bases": sorted(result, key=lambda x: (x["clickers"], x["ecpm"]), reverse=True)
        }
    
    def global_base(self, db_id):
        def compute_rates(sends, openers, clickers, unsubs):
            return (
                round(clickers / sends * 100, 3) if sends else 0,
                round(openers / sends * 100, 3) if sends else 0,
                round(unsubs / sends * 100, 3) if sends else 0,
                round(clickers / openers * 100, 3) if openers else 0
            )
        def push_dim(target, dim, value, sends, clicks, clickers, opens, openers, unsubs):
            if not value:
                return

            seg = target["dimensions"][dim].setdefault(value, {
                "sends": 0,
                "clicks": 0,
                "clickers": 0,
                "opens": 0,
                "openers": 0,
                "unsubs": 0
            })

            seg["sends"] += sends
            seg["clicks"] += clicks
            seg["clickers"] += clickers
            seg["opens"] += opens
            seg["openers"] += openers
            seg["unsubs"] += unsubs
            tc, to, tu, cto = compute_rates(
                seg["sends"], seg["openers"], seg["clickers"], seg["unsubs"]
            )

            seg["taux_clickers"] = tc
            seg["taux_openers"] = to
            seg["taux_unsubs"] = tu
            seg["taux_cto"] = cto
            seg["analyses"] = {
                "taux_clickers": self.analyze.analyze_click_rate(tc),
                "taux_cto": self.analyze.analyze_cto_rate(cto, seg["openers"]),
                "taux_unsubs": self.analyze.analyze_unsub_rate(tu)
            }
        query = f"""
           SELECT
            adv_id,
            database_id,
            gender,
            age_range,
            main_isp,
            dep AS departement,
            any(id_routers)        AS id_routers,
            any(tag_id)            AS tag_id,
            any(brand)             AS brand,
            any(optimized)         AS optimized,
            any(subject)           AS subject,
            any(comment)           AS comment,
            any(date_schedule)     AS date_schedule,
            sum(sends)     AS sends,
            sum(clicks)    AS clicks,
            sum(clickers)  AS clickers,
            sum(opens)     AS opens,
            sum(openers)   AS openers,
            sum(unsubs)    AS unsubs,
            max(ca) AS ca,
            groupUniqArray(segmentId) AS segmentId,
            groupUniqArray(ListId) AS ListId,
            groupUniqArray(ListName) AS ListName
        FROM {self.table}
        WHERE database_id = %(db_id)s
        GROUP BY
            adv_id,
            database_id,
            gender,
            age_range,
            main_isp,
            dep
        """
        rows = self._execute_query(query, {"db_id": db_id})
        if not rows:
            return {"database_id": str(db_id), "globales": {}, "advertisers": []}
        advertisers = {}
        analyse_dep = {}    
        total = dict(sends=0, clicks=0, clickers=0, opens=0, openers=0, unsubs=0, ca=0)
        for r in rows:
            adv = advertisers.setdefault(r["adv_id"], {
                "advertiser_id": r["adv_id"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "brands": [],
                "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
            })
            sends = r["sends"] or 0
            clicks = r["clicks"] or 0
            clickers = r["clickers"] or 0
            opens = r["opens"] or 0 
            openers = r["openers"] or 0
            unsubs = r["unsubs"] or 0
            ca = r["ca"] or 0
            adv["sends"] += sends
            adv["clicks"] += clicks
            adv["clickers"] += clickers
            adv["opens"] += opens
            adv["openers"] += openers
            adv["unsubs"] += unsubs
            push_dim(adv, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(adv, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(adv, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)

            dep = r.get("departement") or "others"
            if dep == "dep_vide":
                dep = "others"
            d = analyse_dep.setdefault(dep, {"sends": 0,"clickers": 0, "openers": 0, "unsubs": 0})
            d["sends"] += sends
            d["clickers"] += clickers
            d["openers"] += openers
            d["unsubs"] += unsubs
            brand_key = (r["brand"], r["id_routers"], r["tag_id"])
            brand = next(
                (b for b in adv["brands"]
                if (b["name"], b["id_routers"], b["tag_id"]) == brand_key),
                None
            )
            listid = r.get("ListId") or []
            listname = r.get("ListName") or []
            segmentids = r.get("segmentId") or []
            if isinstance(segmentids, str):
                segmentids = [segmentids]
            elif isinstance(segmentids, int):
                segmentids = [segmentids]
            elif segmentids is None:
                segmentids = []
            elif not isinstance(segmentids, list):
                segmentids = list(segmentids)
            segmentids = [s for s in segmentids if s not in (0,"0","",None)]
            if not brand:
                brand = {
                    "name": r["brand"],
                    "id_routers": r["id_routers"],
                    "tag_id": r["tag_id"],
                    "creativities": r.get("optimized") or "",
                    "comment": (r.get("comment") or "").replace("None", ""),
                    "subject": r["subject"],
                    "segment_id": list(set(segmentids)),
                    "ListId":list(set(listid)),
                    "ListName": list(set(listname)),
                    "date_schedule": r.get("date_schedule"),
                    "sends": 0,
                    "clicks": 0,
                    "clickers": 0,
                    "opens": 0,
                    "openers": 0,
                    "unsubs": 0,
                    "ca": 0,
                    "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
                }

                adv["brands"].append(brand)
            existing = set(brand.get("segment_id") or [])
            new_segments = set(segmentids)
            brand["segment_id"] = list(existing | new_segments)
            brand["sends"] += sends
            brand["clicks"] += clicks
            brand["clickers"] += clickers
            brand["opens"] += opens
            brand["openers"] += openers
            brand["unsubs"] += unsubs
            brand["ca"] = max(brand["ca"], ca)
            brand["taux_clickers"] = round(brand["clickers"] / brand["sends"] * 100, 5) if brand["sends"] else 0
            brand["taux_openers"] = round(brand["opens"] / brand["sends"] * 100, 5) if brand["sends"] else 0
            brand["taux_unsubs"] = round(brand["unsubs"] / brand["sends"] * 100, 5) if brand["sends"] else 0
            brand["taux_cto"] = round(brand["clickers"] / brand["opens"] * 100, 5) if brand["opens"] else 0
            brand["ecpm"] = round(brand["ca"] / brand["sends"] * 1000, 3) if brand["sends"] else 0
            push_dim(brand, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)
            total["sends"] += sends 
            total["clicks"] += clicks
            total["clickers"] += clickers
            total["opens"] += opens
            total["openers"] += openers
            total["unsubs"] += unsubs

            total_sends = total["sends"]
            for dep, stats in analyse_dep.items():
                stats["taux_clickers"] = round(stats.get("clickers", 0) / total_sends * 100, 4) if total_sends else 0
                stats["taux_openers"] = round(stats.get("openers", 0) / total_sends * 100, 4) if total_sends else 0
                stats["taux_unsubs"] = round(stats.get("unsubs", 0) / total_sends * 100, 4) if total_sends else 0
        for adv in advertisers.values():
            adv["ca"] = sum(b["ca"] for b in adv["brands"])
        total["ca"] = sum(
            adv["ca"]
            for adv in advertisers.values()
        )
        result = []
        for adv in advertisers.values():
            tc, to, tu, cto = compute_rates(
                adv["sends"], adv["openers"], adv["clickers"], adv["unsubs"]
            )
            ecpm = round(adv["ca"] / adv["sends"] * 1000, 3) if adv["sends"] else 0
            adv["classification"] = self.analyze.classify_advertiser(ecpm, tc)
            result.append({
                "advertiser_id": str(adv["advertiser_id"]),
                "sends": adv["sends"],
                "clicks": adv["clicks"],
                "clickers": adv["clickers"],
                "opens": adv["opens"],
                "openers": adv["openers"],
                "unsubs": adv["unsubs"],
                "classification": adv["classification"],
                "taux_clickers": tc,
                "taux_openers": to,
                "taux_unsubs": tu,
                "taux_cto": cto,
                "ecpm": ecpm,
                "ca": adv["ca"],
                "dimensions": adv["dimensions"],
                "brands": adv["brands"]
            })
        tc_g, to_g, tu_g, cto_g = compute_rates(
            total["sends"], total["openers"], total["clickers"], total["unsubs"]
        )
        ecpm_g = round(total["ca"] / total["sends"] * 1000, 3) if total["sends"] else 0
        return {
            "database_id": str(db_id),
            "globales": {
                **total,
                "ecpm": ecpm_g,
                "taux_clickers": tc_g,
                "taux_openers": to_g,
                "taux_unsubs": tu_g,
                "taux_cto": cto_g,
                "analyses": {
                    "taux_clickers": self.analyze.analyze_click_rate(tc_g),
                    "taux_cto": self.analyze.analyze_cto_rate(cto_g, total["openers"]),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(tu_g)
                },
                "analyse_dep": analyse_dep
            },
            "advertisers": sorted(result, key=lambda x: (x["clickers"], x["ecpm"]), reverse=True)
        }

    def all_advertisers(self, date_schedule=None, date_start=None, date_end=None):
        conditions = []
        if date_schedule:
            conditions.append(f"has(r.date_schedule, '{date_schedule}')")
        if date_start and date_end:
            conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', r.date_schedule)"
            )
        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        query = f"""
        WITH stats AS (
            SELECT 
                r.adv_id,
                r.tag_id,
                r.id_routers,
                r.database_id,
                SUM(r.sends) AS sends,
                SUM(r.openers) AS openers,
                SUM(r.clickers) AS clickers,
                SUM(r.unsubs) AS unsubs
            FROM {self.table} r
            {where_clause}
            GROUP BY r.adv_id, r.tag_id, r.id_routers, r.database_id
        ),
        ca_unique AS (
            SELECT 
                adv_id,
                SUM(ca) AS ca_global
            FROM (
                SELECT 
                    adv_id,
                    id_routers,
                    database_id,
                    MAX(ca) AS ca
                FROM {self.table} r
                {where_clause}
                GROUP BY adv_id, id_routers, database_id
            )
            GROUP BY adv_id
        )

        SELECT 
            s.adv_id AS adv_id,
            a.name AS advertiser_name,
            s.tag_id AS tag_id,
            SUM(s.sends) AS sends,
            SUM(s.openers) AS openers,
            SUM(s.clickers) AS clickers,
            SUM(s.unsubs) AS unsubs,
            cu.ca_global AS ca,
            ROUND(cu.ca_global / NULLIF(SUM(s.sends), 0) * 1000, 3) AS ecpm,
            ROUND(SUM(s.clickers) / NULLIF(SUM(s.openers), 0) * 100, 3) AS taux_cto,
            ROUND(SUM(s.openers) / NULLIF(SUM(s.sends), 0) * 100, 3) AS taux_openers,
            ROUND(SUM(s.clickers) / NULLIF(SUM(s.sends), 0) * 100, 3) AS taux_clickers,
            ROUND(SUM(s.unsubs) / NULLIF(SUM(s.sends), 0) * 100, 3) AS taux_unsubs
        FROM stats s
        JOIN ca_unique cu ON s.adv_id = cu.adv_id
        JOIN advertiser a ON a.id = s.adv_id
        GROUP BY 
            s.adv_id,
            s.tag_id,
            a.name,
            cu.ca_global
        """
        rows = self._execute_query(query)
        result = []
        for row in rows:
            sends = row["sends"] or 0
            openers = row["openers"] or 0
            clickers = row["clickers"] or 0
            unsubs = row["unsubs"] or 0
            taux_cto = row["taux_cto"] or 0.0
            taux_clickers = row["taux_clickers"] or 0.0
            taux_openers = row["taux_openers"] or 0.0
            taux_unsubs = row["taux_unsubs"] or 0.0
            result.append({
                "advertiser_id": str(row["adv_id"]),
                "advertiser_name": row["advertiser_name"],
                "tag_id":row["tag_id"],
                "globales": {
                    "sends": sends,
                    "openers": openers,
                    "clickers": clickers,
                    "unsubs": unsubs,
                    "ca": row["ca"] or 0.0,
                    "ecpm": row["ecpm"] or 0.0,
                    "taux_openers": taux_openers,
                    "taux_clickers": taux_clickers,
                    "taux_unsubs": taux_unsubs,
                    "analyse": {
                        "taux_clickers": self.analyze.analyze_click_rate(taux_clickers),
                        "taux_cto": self.analyze.analyze_cto_rate(taux_cto, openers),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(taux_unsubs)
                    }
                }
            })

        return result
    
    def all_bases(self, country=None, tags=None, date_schedule=None, date_start=None, date_end=None):

        joins = []
        conditions = []
        if tags:
            if isinstance(tags, str):
                tags = [tags]
            joins.append("JOIN tags t ON r.tag_id = t.id")
            tags_value = ",".join(f"'{t}'" for t in tags)
            conditions.append(f"t.tag IN ({tags_value})")

        if country:
            if isinstance(country, str):
                country = [country]
            joins.append("JOIN country c ON r.country = c.id")
            country_values = ",".join(f"'{c}'" for c in country)
            conditions.append(f"c.dwh_name IN ({country_values})")

        if date_schedule:
            conditions.append(f"has(r.date_schedule, '{date_schedule}')")

        if date_start and date_end:
            conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', r.date_schedule)"
            )

        join_clause = " ".join(joins)
        where_clause = "WHERE 1=1"
        if conditions:
            where_clause += " AND " + " AND ".join(conditions)
        query = f"""
        WITH advertisers AS (
            SELECT
                r.brand,
                r.id_routers,
                r.database_id,
                SUM(r.sends)    AS sends,
                SUM(r.clicks)   AS clicks,
                SUM(r.clickers) AS clickers,
                SUM(r.openers)  AS openers,
                SUM(r.unsubs)   AS unsubs,
                MAX(r.ca) AS ca
            FROM {self.table} r
            {join_clause}
            {where_clause}
            GROUP BY r.brand, r.id_routers,r.database_id
        )
        SELECT
            a.database_id,
            d.basename,
            SUM(a.sends) AS sends,
            SUM(a.clicks) AS clicks,
            SUM(a.clickers) AS clickers,
            SUM(a.openers) AS openers,
            SUM(a.unsubs)  AS unsubs,
            SUM(a.ca) AS ca_global,
            ROUND(SUM(a.openers)  / NULLIF(SUM(a.sends),0) * 100, 3) AS taux_openers,
            ROUND(SUM(a.clickers) / NULLIF(SUM(a.sends),0) * 100, 3) AS taux_clickers,
            ROUND(SUM(a.clickers) / NULLIF(SUM(a.openers),0) * 100, 3) AS taux_cto,
            ROUND(SUM(a.unsubs)   / NULLIF(SUM(a.sends),0) * 100, 3) AS taux_unsubs,
            ROUND(SUM(a.ca) / NULLIF(SUM(a.sends),0) * 1000, 3) AS ecpm
        FROM advertisers a
        JOIN databases d ON a.database_id = d.id
        GROUP BY a.database_id, d.basename
        ORDER BY sends DESC
        """
        rows = self._execute_query(query)
        result = []
        for row in rows:
            sends = row["sends"] or 0
            openers = row["openers"] or 0
            clickers = row["clickers"] or 0
            unsubs = row["unsubs"] or 0

            result.append({
                "database_id": row["database_id"],
                "database_name": row["basename"],

                "globales": {
                    "sends": sends,
                    "openers": openers,
                    "clickers": clickers,
                    "unsubs": unsubs,

                    "ca": row["ca_global"] or 0.0,
                    "ecpm": row["ecpm"] or 0.0,

                    "taux_openers": row["taux_openers"] or 0.0,
                    "taux_clickers": row["taux_clickers"] or 0.0,
                    "taux_cto": row["taux_cto"] or 0.0,
                    "taux_unsubs": row["taux_unsubs"] or 0.0,

                    "analyse": {
                        "taux_clickers": self.analyze.analyze_click_rate(row["taux_clickers"] or 0.0),
                        "taux_cto": self.analyze.analyze_cto_rate(row["taux_cto"] or 0.0, openers),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(row["taux_unsubs"] or 0.0)
                    }
                }
            })

        return result
    
    def build_segment_index(self):
        index = {}
        for f in os.listdir(CSV_DIR):
            if f.endswith(".csv"):
                path = os.path.join(CSV_DIR, f)
                try:
                    df = pd.read_csv(path, sep=';', usecols=['id_segment'])
                except Exception as e:
                    continue
                for seg_id in df['id_segment'].tolist():
                    if seg_id not in index:
                        index[seg_id] = []
                    index[seg_id].append(path)
        return index
    
    def load_all_csv(self):
        dfs = []
        for f in os.listdir(CSV_DIR):
            if f.endswith(".csv"):
                path = os.path.join(CSV_DIR, f)
                try:
                    dfs.append(pd.read_csv(path, sep=';'))
                except Exception as e:
                    print(f"Erreur lecture {path}: {e}")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def get_segment(self, id_segment=None, database_id=None):
        if id_segment is None and database_id is None:
            df = self.load_all_csv()
            if df.empty:
                return {"message": "Données vides"}
            df.drop(columns=["updated_at"],inplace=True)
            return {
                "total":len(df),
                "segments":df.to_dict(orient='records')
            }
        if id_segment is not None:
            csv_files = self.segment_index.get(id_segment)
            if database_id is not None:
                csv_files = [f for f in csv_files if f.endswith(f"segments_base_{database_id}.csv")]
            if not csv_files:
                return {"message": "id_segment non trouvé"}
            dfs = []
            for file in csv_files:
                df = pd.read_csv(file, sep=';')
                dfs.append(df[df["id_segment"] == id_segment])
            df_filtered = pd.concat(dfs, ignore_index=True)
        else:
            csv_file = os.path.join(CSV_DIR, f"segments_base_{database_id}.csv")
            if not os.path.exists(csv_file):
                return {"message": "database_id non trouvé"}
            df_filtered = pd.read_csv(csv_file, sep=';')
        if database_id is not None:
            df_filtered = df_filtered[df_filtered["database_id"] == database_id]
        if df_filtered.empty:
            return {"message": "Aucun résultats"}
        return df_filtered[
            ["id_segment","segment_name","database_id"]
        ].to_dict(orient='records')