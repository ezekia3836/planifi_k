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
        self.table = "prod_reporting_test"
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
    WITH ca_by_focus AS (
        SELECT
            id_focus,
            max(ca) AS ca
        FROM {self.table}
        WHERE adv_id = %(adv_id)s
        GROUP BY id_focus
    )
    SELECT
        t.adv_id,
        t.database_id,
        groupUniqArray(t.id_routers) AS id_routers,
        t.tag_id,
        max(t.clicks_val) AS clicks_val,
        max(t.leads_val) AS leads_val,
        max(t.cpm_val) AS volume_val,
        t.brand,
        groupUniqArray(tuple(t.model, t.payvalue, t.comment)) AS models,
        t.client_id,
        t.optimized,
        t.subject,
        t.date_schedule,
        t.gender,
        t.age_range,
        t.main_isp,
        t.id_focus,
        t.dep AS departement,
        sum(t.sends)    AS sends,
        sum(t.clicks)   AS clicks,
        sum(t.clickers) AS clickers,
        sum(t.opens)    AS opens,
        sum(t.openers)  AS openers,
        sum(t.unsubs)   AS unsubs,
        any(f.ca)       AS ca,
        groupUniqArray(t.segmentId) AS segmentId,
        groupUniqArray(t.ListId)    AS ListId,
        groupUniqArray(t.ListName)  AS ListName
    FROM {self.table} t
    LEFT JOIN ca_by_focus f ON f.id_focus = t.id_focus
    WHERE t.adv_id = %(adv_id)s 
    GROUP BY
        t.adv_id, t.database_id, t.tag_id,t.date_schedule,
        t.brand,t.client_id, t.optimized, t.subject,
        t.gender, t.age_range, t.main_isp, t.id_focus, t.dep
    HAVING sends>0
"""
        rows = self._execute_query(query, {"adv_id": adv_id})
        if not rows:
            return {"advertiser_id": str(adv_id), "globales": {}, "bases": []}

        bases = {}
        analyse_dep = {}
        total = dict(sends=0, clicks=0, clickers=0, opens=0, openers=0, unsubs=0, ca=0)
        seen_focus = {} 
        for r in rows:

            db_id = r["database_id"]

            base = bases.setdefault(db_id, {
                "database_id": db_id,
                "subject": r["subject"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "brands": {},
                "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
            })

            sends = r["sends"] or 0
            clicks = r["clicks"] or 0
            clickers = r["clickers"] or 0
            opens = r["opens"] or 0
            openers = r["openers"] or 0
            unsubs = r["unsubs"] or 0

            base["sends"] += sends
            base["clicks"] += clicks
            base["clickers"] += clickers
            base["opens"] += opens
            base["openers"] += openers
            base["unsubs"] += unsubs
            push_dim(base, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(base, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(base, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)
            brand_key = (r["brand"],r["tag_id"],r["client_id"])
            brands_map = base["brands"]

            if brand_key not in brands_map:
                brands_map[brand_key] = {
                    "name": r["brand"],
                    "id_routers": set(),
                    "tag_id": r["tag_id"],
                    "agence": r["client_id"],
                    "creativities": r.get("optimized") or "",
                    "subject": r["subject"],
                    "models":[],
                    "segment_id": set(),
                    "ListId": set(),
                    "ListName": set(),
                    "date_schedule": set(),
                    "sends": 0,
                    "clicks": 0,
                    "clickers": 0,
                    "opens": 0,
                    "openers": 0,
                    "unsubs": 0,
                    "clicks_val":0,
                    "leads_val":0,
                    "volume_val":0,
                    "ca": 0,
                    "_ca_focus": {},
                    "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
                }

            brand = brands_map[brand_key]
            brand["clicks_val"] = r.get("clicks_val") or brand["clicks_val"] or 0
            brand["leads_val"] = r.get("leads_val") or brand["leads_val"] or 0
            brand["volume_val"] = r.get("volume_val") or brand["volume_val"] or 0
            models = r.get("models") or []
            seen_models = set()
            clean_models = []
            for m in models:
                if not m:
                    continue
                key = (m[0], m[1], m[2])
                if key in seen_models:
                    continue
                seen_models.add(key)
                clean_models.append({
                    "model": m[0],
                    "payvalue": m[1],
                    "comment": m[2] if m[2] not in (None, "None") else ""
                })

            brand["models"] = clean_models
            brand["id_routers"].update(r.get("id_routers") or [])
            brand["date_schedule"].update(r.get("date_schedule") or [])
            segmentids = r.get("segmentId") or []
            if isinstance(segmentids, (int, str)):
                segmentids = [segmentids]

            brand["segment_id"].update(s for s in segmentids if s not in (0, "0", "", None))
            brand["ListId"].update(r.get("ListId") or [])
            brand["ListName"].update(r.get("ListName") or [])
            
            brand["sends"] += sends
            brand["clicks"] += clicks
            brand["clickers"] += clickers
            brand["opens"] += opens
            brand["openers"] += openers
            brand["unsubs"] += unsubs
            push_dim(brand, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)
            id_focus = r.get("id_focus")
            ca = r.get("ca") or 0

            if id_focus and id_focus not in brand["_ca_focus"]:
                brand["_ca_focus"][id_focus] = ca
                brand["ca"] += ca
            if id_focus and id_focus not in seen_focus:
                seen_focus[id_focus] = ca
            dep = r.get("departement") or "others"
            if dep == "dep_vide":
                dep = "others"

            d = analyse_dep.setdefault(dep, {"sends": 0, "clickers": 0, "openers": 0, "unsubs": 0})
            d["sends"] += sends
            d["clickers"] += clickers
            d["openers"] += openers
            d["unsubs"] += unsubs

            total["sends"] += sends
            total["clicks"] += clicks
            total["clickers"] += clickers
            total["opens"] += opens
            total["openers"] += openers
            total["unsubs"] += unsubs
        result = []
        for base in bases.values():
            brands_list = []
            for brand in base["brands"].values():
                tc, to, tu, cto = compute_rates(
                    brand["sends"], brand["openers"], brand["clickers"], brand["unsubs"])
                brand["taux_clickers"] = tc
                brand["taux_openers"] = to
                brand["taux_unsubs"] = tu
                brand["taux_cto"] = cto
                brand["ecpm"] = round(brand["ca"] / brand["sends"] * 1000, 3) if brand["sends"] else 0
                brand["id_routers"] = list(brand["id_routers"])
                brand["segment_id"] = list(brand["segment_id"])
                brand["ListId"] = list(brand["ListId"])
                brand["ListName"] = list(brand["ListName"])
                brands_list.append(brand)
            base["brands"] = brands_list
            base["ca"] = sum(
                sum(b["_ca_focus"].values())
                for b in brands_list)
            for b in brands_list:
                b.pop("_ca_focus", None)
           
            tc, to, tu, cto = compute_rates(
                base["sends"], base["openers"], base["clickers"], base["unsubs"])
            ecpm = round(base["ca"] / base["sends"] * 1000, 3) if base["sends"] else 0
            result.append({
                "database_id": base["database_id"],
                "brands": brands_list,
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
                "classification": self.analyze.classify_advertiser(ecpm, tc),
                "dimensions": base["dimensions"]})
        total["ca"] = sum(seen_focus.values())
        total_sends = total["sends"]
        for dep, stats in analyse_dep.items():
                stats["taux_clickers"] = round(stats["clickers"] / total_sends * 100, 4) if total_sends else 0
                stats["taux_openers"]  = round(stats["openers"]  / total_sends * 100, 4) if total_sends else 0
                stats["taux_unsubs"]   = round(stats["unsubs"]   / total_sends * 100, 3) if total_sends else 0

        analyse_dep = {
                dep: stats
                for dep, stats in analyse_dep.items()
                if stats["taux_clickers"] != 0
                or stats["taux_openers"]  != 0
                or stats["taux_unsubs"]   != 0
            }

        tc_g, to_g, tu_g, cto_g = compute_rates(
            total["sends"], total["openers"], total["clickers"], total["unsubs"])
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
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0
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
        WITH ca_by_focus AS (
            SELECT id_focus, max(ca) AS ca
            FROM {self.table}
            WHERE database_id = %(db_id)s
            GROUP BY id_focus
        )
        SELECT
            t.adv_id,
            t.database_id,
            groupUniqArray(t.id_routers) AS id_routers,
            max(t.clicks_val) AS clicks_val,
            max(t.leads_val) AS leads_val,
            max(t.cpm_val) AS volume_val,
            groupUniqArray(tuple(t.model, t.payvalue, t.comment)) AS models,
            t.tag_id,
            t.brand,
            t.optimized,
            t.subject,
            t.date_schedule,
            t.gender,
            t.age_range,
            t.main_isp,
            t.id_focus,
            t.dep AS departement,
            sum(t.sends)    AS sends,
            sum(t.clicks)   AS clicks,
            sum(t.clickers) AS clickers,
            sum(t.opens)    AS opens,
            sum(t.openers)  AS openers,
            sum(t.unsubs)   AS unsubs,
            max(f.ca)       AS ca,
            groupUniqArray(t.segmentId) AS segmentId,
            groupUniqArray(t.ListId)    AS ListId,
            groupUniqArray(t.ListName)  AS ListName
        FROM {self.table} t
        LEFT JOIN ca_by_focus f ON f.id_focus = t.id_focus
        WHERE t.database_id = %(db_id)s 
        GROUP BY
            t.adv_id, t.database_id, t.tag_id, t.date_schedule,
            t.brand, t.optimized, t.subject, t.gender, t.age_range, t.main_isp, t.id_focus, t.dep
        HAVING sends > 0
        """

        rows = self._execute_query(query, {"db_id": db_id})

        if not rows:
            return {"database_id": str(db_id), "globales": {}, "advertisers": []}

        advertisers = {}
        analyse_dep = {}
        focus_ca_map = {}

        total = {
            "sends": 0, "clicks": 0, "clickers": 0,
            "opens": 0, "openers": 0, "unsubs": 0,
            "ca": 0
        }

        seen_focus_global = set()

        for r in rows:

            adv_id = r.get("adv_id") or "unknown"

            advertiser = advertisers.setdefault(adv_id, {
                "advertiser_id": adv_id,
                "subject": r["subject"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "ca": 0,
                "brands": {},
                "dimensions": {"age_range": {}, "gender": {}, "isp": {}},
                "_seen_focus": set()
            })

            sends = r["sends"] or 0
            clicks = r["clicks"] or 0
            clickers = r["clickers"] or 0
            opens = r["opens"] or 0
            openers = r["openers"] or 0
            unsubs = r["unsubs"] or 0

            advertiser["sends"] += sends
            advertiser["clicks"] += clicks
            advertiser["clickers"] += clickers
            advertiser["opens"] += opens
            advertiser["openers"] += openers
            advertiser["unsubs"] += unsubs

            push_dim(advertiser, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(advertiser, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(advertiser, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)

            brand_key = (r["brand"], r["tag_id"])

            brand = advertiser["brands"].setdefault(brand_key, {
                "name": r["brand"],
                "tag_id": r["tag_id"],
                "creativities": r.get("optimized") or "",
                "subject": r["subject"],
                "id_routers": set(),
                "models":[],
                "segment_id": set(),
                "ListId": set(),
                "ListName": set(),
                "date_schedule": set(),
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "clicks_val":0,
                "leads_val":0,
                "volume_val":0,
                "ca": 0,
                "dimensions": {"age_range": {}, "gender": {}, "isp": {}},
                "_seen_focus": set()
            })
            brand["clicks_val"] = r.get("clicks_val") or brand["clicks_val"] or 0
            brand["leads_val"] = r.get("leads_val") or brand["leads_val"] or 0
            brand["volume_val"] = r.get("volume_val") or brand["volume_val"] or 0
            models = r.get("models") or []
            seen_models = set()
            clean_models = []
            for m in models:
                if not m:
                    continue
                key = (m[0], m[1], m[2])
                if key in seen_models:
                    continue
                seen_models.add(key)
                clean_models.append({
                    "model": m[0],
                    "payvalue": m[1],
                    "comment": m[2] if m[2] not in (None, "None") else ""
                })
            brand["models"] = clean_models
            brand["id_routers"].update(r.get("id_routers") or [])
            brand["date_schedule"].update(r.get("date_schedule") or [])
            brand["segment_id"].update(r.get("segmentId") or [])
            brand["ListId"].update(r.get("ListId") or [])
            brand["ListName"].update(r.get("ListName") or [])
            brand["sends"] += sends
            brand["clicks"] += clicks
            brand["clickers"] += clickers
            brand["opens"] += opens
            brand["openers"] += openers
            brand["unsubs"] += unsubs

            push_dim(brand, "age_range", r["age_range"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "gender", r["gender"], sends, clicks, clickers, opens, openers, unsubs)
            push_dim(brand, "isp", r["main_isp"], sends, clicks, clickers, opens, openers, unsubs)

            id_focus = r.get("id_focus")
            ca = r.get("ca") or 0

            if id_focus:

                focus_ca_map[id_focus] = ca
                if id_focus not in advertiser["_seen_focus"]:
                    advertiser["ca"] += ca
                    advertiser["_seen_focus"].add(id_focus)
                if id_focus not in brand["_seen_focus"]:
                    brand["ca"] += ca
                    brand["_seen_focus"].add(id_focus)

                if id_focus not in seen_focus_global:
                    total["ca"] += ca
                    seen_focus_global.add(id_focus)
            dep = r.get("departement") or "others"
            if dep == "dep_vide":
                dep = "others"

            d = analyse_dep.setdefault(dep, {"sends": 0, "clickers": 0, "openers": 0, "unsubs": 0})
            d["sends"] += sends
            d["clickers"] += clickers
            d["openers"] += openers
            d["unsubs"] += unsubs

            total["sends"] += sends
            total["clicks"] += clicks
            total["clickers"] += clickers
            total["opens"] += opens
            total["openers"] += openers
            total["unsubs"] += unsubs

        result = []

        for advertiser in advertisers.values():

            brands_list = []

            for brand in advertiser["brands"].values():

                tc, to, tu, cto = compute_rates(
                    brand["sends"], brand["openers"], brand["clickers"], brand["unsubs"]
                )

                brand["taux_clickers"] = tc
                brand["taux_openers"] = to
                brand["taux_unsubs"] = tu
                brand["taux_cto"] = cto

                brand["ecpm"] = round(
                    brand["ca"] / brand["sends"] * 1000, 3
                ) if brand["sends"] else 0

                brand["id_routers"] = list(brand["id_routers"])
                brand["segment_id"] = list(brand["segment_id"])
                brand["ListId"] = list(brand["ListId"])
                brand["ListName"] = list(brand["ListName"])

                brand.pop("_seen_focus", None)

                brands_list.append(brand)

            advertiser["brands"] = brands_list

            tc, to, tu, cto = compute_rates(
                advertiser["sends"],
                advertiser["openers"],
                advertiser["clickers"],
                advertiser["unsubs"]
            )

            ecpm = round(
                advertiser["ca"] / advertiser["sends"] * 1000, 3
            ) if advertiser["sends"] else 0

            advertiser.pop("_seen_focus", None)

            result.append({
                "advertiser_id": advertiser["advertiser_id"],
                "brands": brands_list,
                "sends": advertiser["sends"],
                "clicks": advertiser["clicks"],
                "clickers": advertiser["clickers"],
                "opens": advertiser["opens"],
                "openers": advertiser["openers"],
                "unsubs": advertiser["unsubs"],
                "taux_clickers": tc,
                "taux_openers": to,
                "taux_unsubs": tu,
                "taux_cto": cto,
                "ecpm": ecpm,
                "ca": advertiser["ca"],
                "classification": self.analyze.classify_advertiser(ecpm, tc),
                "dimensions": advertiser["dimensions"]
            })

        tc_g, to_g, tu_g, cto_g = compute_rates(
            total["sends"],
            total["openers"],
            total["clickers"],
            total["unsubs"]
        )

        ecpm_g = round(
            total["ca"] / total["sends"] * 1000, 3
        ) if total["sends"] else 0

        total_sends = total["sends"]

        for dep, stats in analyse_dep.items():
            stats["taux_clickers"] = round(stats["clickers"] / total_sends * 100, 4) if total_sends else 0
            stats["taux_openers"] = round(stats["openers"] / total_sends * 100, 4) if total_sends else 0
            stats["taux_unsubs"] = round(stats["unsubs"] / total_sends * 100, 4) if total_sends else 0
        analyse_dep = {
            dep:stats 
            for dep,stats in analyse_dep.items()
            if stats["taux_clickers"]!=0
            or stats["taux_openers"]!=0
            or stats["taux_unsubs"] !=0
        }

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
            "advertisers": sorted(
                result,
                key=lambda x: (x["clickers"], x["ecpm"]),
                reverse=True
            )
        }
    
    def all_advertisers(self, date_schedule=None, date_start=None, date_end=None):
        conditions = []
        if date_schedule:
            conditions.append(f"has(date_schedule, '{date_schedule}')")
        if date_start and date_end:
            conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', date_schedule)"
            )
        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        query = f"""
        WITH base AS (
            SELECT
                adv_id,
                tag_id,
                id_focus,
                sends,
                openers,
                clickers,
                unsubs,
                ca
            FROM {self.table}
            {where_clause}
        ),
        focus_agg AS (
            SELECT
                adv_id,
                tag_id,
                id_focus,
                sum(sends)    AS sends,
                sum(openers)  AS openers,
                sum(clickers) AS clickers,
                sum(unsubs)   AS unsubs,
                max(ca)       AS ca
            FROM base
            GROUP BY adv_id, tag_id, id_focus
            HAVING sends > 0
        ),
        stats AS (
            SELECT
                adv_id,
                tag_id,
                sum(sends)    AS sends,
                sum(openers)  AS openers,
                sum(clickers) AS clickers,
                sum(unsubs)   AS unsubs
            FROM focus_agg
            GROUP BY adv_id, tag_id
        ),

        ca_final AS (
            SELECT
                adv_id,
                tag_id,
                sum(ca) AS ca_global
            FROM focus_agg
            GROUP BY adv_id, tag_id
        )

        SELECT
            s.adv_id AS adv_id,
            a.name AS advertiser_name,
            s.tag_id AS tag_id,

            s.sends,
            s.openers,
            s.clickers,
            s.unsubs,
            coalesce(c.ca_global, 0) AS ca,
            round(s.openers / nullIf(s.sends,0) * 100, 3) AS taux_openers,
            round(s.clickers / nullIf(s.sends,0) * 100, 3) AS taux_clickers,
            round(s.clickers / nullIf(s.openers,0) * 100, 3) AS taux_cto,
            round(s.unsubs / nullIf(s.sends,0) * 100, 3) AS taux_unsubs,
            round(coalesce(c.ca_global,0) / nullIf(s.sends,0) * 1000, 3) AS ecpm

        FROM stats s
        LEFT JOIN ca_final c
            ON s.adv_id = c.adv_id AND s.tag_id = c.tag_id
        JOIN advertiser a
            ON a.id = s.adv_id

        ORDER BY s.sends DESC
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
                    "ca": round(row["ca"],3) or 0.0,
                    "ecpm": round(row["ecpm"],3) if row["ecpm"] is not None else 0.0,
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
        WITH filtered AS (
            SELECT
                r.database_id,
                r.id_focus,
                r.sends,
                r.clicks,
                r.clickers,
                r.openers,
                r.unsubs
            FROM {self.table} r
            {join_clause}
            {where_clause}
        ),
        stats AS (
            SELECT
                database_id,
                SUM(sends)    AS sends,
                SUM(clicks)   AS clicks,
                SUM(clickers) AS clickers,
                SUM(openers)  AS openers,
                SUM(unsubs)   AS unsubs
            FROM filtered
            GROUP BY database_id
            HAVING sends > 0
        ),
        ca_by_focus AS (
            SELECT
                database_id,
                id_focus,
                max(ca) AS ca
            FROM {self.table}
            GROUP BY database_id, id_focus
            HAVING sum(sends) > 0 
        ),
        ca_unique AS (
            SELECT
                database_id,
                SUM(ca) AS ca_global
            FROM ca_by_focus
            GROUP BY database_id
        )
        SELECT
            s.database_id AS database_id,
            d.basename,
            s.sends,
            s.clicks,
            s.clickers,
            s.openers,
            s.unsubs,
            COALESCE(cu.ca_global, 0)  AS ca_global,
            ROUND(s.openers  / nullIf(s.sends,   0) * 100,  3) AS taux_openers,
            ROUND(s.clickers / nullIf(s.sends,   0) * 100,  3) AS taux_clickers,
            ROUND(s.clickers / nullIf(s.openers, 0) * 100,  3) AS taux_cto,
            ROUND(s.unsubs   / nullIf(s.sends,   0) * 100,  3) AS taux_unsubs,
            ROUND(COALESCE(cu.ca_global,0) / nullIf(s.sends,0) * 1000, 3)  AS ecpm
        FROM stats s
        LEFT JOIN ca_unique cu ON cu.database_id = s.database_id
        JOIN databases d ON d.id = s.database_id
        ORDER BY s.sends DESC
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

                    "ca": round(row["ca_global"],3) or 0.0,
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


    def get_agences(self, agence_id=None):
        query = "SELECT id, name FROM agences"
        params = {}

        if agence_id is not None:
             query += f" WHERE id = {int(agence_id)}"

        rows = self._execute_query(query, params)

        return [
            {"agence_id": r["id"], "agence_name": r["name"]}
            for r in rows
        ]

    def get_tags(self, tags_id=None):
        query = "SELECT id, tag FROM tags"
        params = {}

        if tags_id is not None:
            query += f" WHERE id = {int(tags_id)}"

        rows = self._execute_query(query, params)

        return [
            {"tag_id": r["id"], "tag_name": r["tag"]}
            for r in rows
        ]