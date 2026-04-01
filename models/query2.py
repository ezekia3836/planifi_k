from collections import defaultdict
import csv
from unittest import result
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
import math
import pandas as pd
import os
import re
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
        query = f"""
            SELECT
                database_id,
                id_routers,
                tag_id,
                age_range,
                gender,
                brand,
                optimized,
                subject,
                comment,
                dep AS departement,
                main_isp,
                date_schedule,
                SUM(sends) AS sends,
                SUM(clicks) AS clicks,
                SUM(clickers) AS clickers,
                SUM(opens) AS opens,
                SUM(openers) AS openers,
                SUM(unsubs) AS unsubs,
                MAX(ca) AS ca,
                groupUniqArray(segmentId) AS segmentId
            FROM {self.table}
            WHERE adv_id = %(adv_id)s
            GROUP BY
                database_id, id_routers, tag_id,
                age_range, gender, main_isp,
                brand, optimized, date_schedule, subject, dep,comment
        """

        rows = self._execute_query(query, {"adv_id": adv_id})
        if not rows:
            return {"advertiser_id": str(adv_id), "globales": {}, "bases": []}

        bases = {}
        base_ca = {}   
        router_ca = {} 
        total = dict(sends=0, clicks=0, clickers=0, opens=0, openers=0, unsubs=0, ca=0)

        analyse_dep = {}

        for r in rows:
            base_key = (r["database_id"], r["id_routers"], r["tag_id"])
            base = bases.setdefault(base_key, {
                "database_id": r["database_id"],
                "id_routers": r["id_routers"],
                "subject": r["subject"],
                "tag_id": r["tag_id"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "SegmentIds": set(),
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

            router_key = r["id_routers"]
            router_ca[router_key] = max(router_ca.get(router_key, 0), ca)
            base_ca[base_key] = max(base_ca.get(base_key, 0), ca)

            def push_dim(dim, value):
                if not value:
                    return
                seg = base["dimensions"][dim].setdefault(value, {
                    "sends": 0, "clicks": 0, "clickers": 0,
                    "opens": 0, "openers": 0, "unsubs": 0
                })
                seg["sends"] += sends
                seg["clicks"] += clicks
                seg["clickers"] += clickers
                seg["opens"] += opens
                seg["openers"] += openers
                seg["unsubs"] += unsubs

            push_dim("age_range", r["age_range"])
            push_dim("gender", r["gender"])
            push_dim("isp", r["main_isp"])

            dep = r.get("departement") or "others"
            if dep=='dep_vide':
                dep="others"
            dep_stat = analyse_dep.setdefault(dep, {"clickers": 0, "openers": 0, "sends": 0,"unsubs":0})
            dep_stat["clickers"] += clickers
            dep_stat["openers"] += openers
            dep_stat["sends"] += sends
            dep_stat["unsubs"] +=unsubs

            brand_name = r["brand"]
            optimized = r.get("optimized") or ""
            brand_list = base["brands"]
            segment_id = r.get("segmentId", [None])[0] if r.get("segmentId") else None
            date_schedule = r.get("date_schedule")
            comment = r.get("comment") or ""
            if comment =='None':
                comment=''
            brand = next((b for b in brand_list if b["name"] == brand_name), None)
            if not brand:
                brand = {
                    "name": brand_name,
                    "creativities": optimized,
                    "comment":comment,
                    "sends": 0, "clicks": 0, "clickers": 0,
                    "opens": 0, "openers": 0, "unsubs": 0,
                    "subject": r["subject"], 
                    "segment_id": segment_id,
                    "date_schedule":date_schedule

                }
                brand_list.append(brand)

            brand["sends"] += sends
            brand["clicks"] += clicks
            brand["clickers"] += clickers
            brand["opens"] += opens
            brand["openers"] += openers
            brand["unsubs"] += unsubs

            total["sends"] += sends
            total["clicks"] += clicks
            total["clickers"] += clickers
            total["opens"] += opens
            total["openers"] += openers
            total["unsubs"] += unsubs

        for key, b in bases.items():
            b["ca"] = base_ca.get(key, 0)
        total["ca"] = sum(router_ca.values())

        def compute_rates(sends, openers, clickers, unsubs):
            return (
                round(clickers / sends * 100, 3) if sends else 0,
                round(openers / sends * 100, 3) if sends else 0,
                round(unsubs / sends * 100, 3) if sends else 0,
                round(clickers / openers * 100, 3) if openers else 0
            )
        total_sends = total["sends"]  
        for dep, stats in analyse_dep.items():
            stats["taux_clickers"] = round(stats.get("clickers", 0) / total_sends * 100, 3) if total_sends else 0
            stats["taux_openers"] = round(stats.get("openers", 0) / total_sends * 100, 3) if total_sends else 0
            stats["taux_unsubs"] = round(stats.get("unsubs", 0) / total_sends * 100, 3) if total_sends else 0

            if "sends" in stats:
                del stats["sends"]

        result_bases = []
        for base in bases.values():
            tc, to, tu, cto = compute_rates(base["sends"], base["openers"], base["clickers"], base["unsubs"])
            ecpm = round(base["ca"] / base["sends"] * 1000, 3) if base["sends"] else 0
            classification = self.analyze.classify_advertiser(ecpm, tc)

            for b in base["brands"]:
                tc_b, to_b, tu_b, cto_b = compute_rates(b["sends"], b["openers"], b["clickers"], b["unsubs"])
                b.update({
                    "taux_clickers": tc_b,
                    "taux_openers": to_b,
                    "taux_unsubs": tu_b,
                    "taux_cto": cto_b,
                    "analyses": {
                        "taux_clickers": self.analyze.analyze_click_rate(tc_b),
                        "taux_cto": self.analyze.analyze_cto_rate(cto_b, b["openers"]),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(tu_b)
                    }
                })
            for dim_name, dim_vals in base["dimensions"].items():
                for seg in dim_vals.values(): 
                    tc_b, to_b, tu_b, cto_b = compute_rates(seg["sends"], seg["openers"], seg["clickers"], seg["unsubs"])
                    seg.update({
                        "taux_clickers": tc_b,
                        "taux_openers": to_b,
                        "taux_unsubs": tu_b,
                        "taux_cto": cto_b,
                        "analyses": {
                            "taux_clickers": self.analyze.analyze_click_rate(tc_b),
                            "taux_cto": self.analyze.analyze_cto_rate(cto_b, seg["openers"]),
                            "taux_unsubs": self.analyze.analyze_unsub_rate(tu_b)
                        }
                    })
            result_bases.append({
                "database_id": base["database_id"],
                "id_routers": base["id_routers"],
                "tag_id": base["tag_id"],
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
                "analyses": {
                    "taux_clickers": self.analyze.analyze_click_rate(tc),
                    "taux_cto": self.analyze.analyze_cto_rate(cto, base["openers"]),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(tu)
                },
                "dimensions": base["dimensions"]
            })
        tc_g, to_g, tu_g, cto_g = compute_rates(total["sends"], total["openers"], total["clickers"], total["unsubs"])
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
            "bases": sorted(result_bases, key=lambda x: (x["clickers"], x["ecpm"]), reverse=True)
        }
        
    def global_base(self, db_id):
        query = f"""
            SELECT
                adv_id, id_routers, tag_id, age_range, gender, main_isp,
                brand, optimized,
                subject,
                dep AS departement,
                date_schedule,
                SUM(sends) AS sends,
                SUM(clicks) AS clicks,
                SUM(clickers) AS clickers,
                SUM(opens) AS opens,
                SUM(openers) AS openers,
                SUM(unsubs) AS unsubs,
                MAX(ca) AS ca,
                groupUniqArray(segmentId) AS segmentId,
                comment
            FROM {self.table}
            WHERE database_id = %(db_id)s
            GROUP BY adv_id, id_routers, tag_id, age_range, gender,
                    main_isp, brand, optimized, subject, dep,date_schedule,comment
        """
        rows = self._execute_query(query, {"db_id": db_id})

        result = {
            "database_id": str(db_id),
            "globales": {"sends": 0, "clicks": 0, "clickers": 0,
                        "opens": 0, "openers": 0, "unsubs": 0, "ca": 0.0,
                        "analyse_dep": {}},
            "advertisers": []
        }

        advertisers = {}
        ca_per_routers = {}

        def compute_rates(sends, openers, clickers, unsubs):
            return (
                round(clickers / sends * 100, 3) if sends else 0,
                round(openers / sends * 100, 3) if sends else 0,
                round(unsubs / sends * 100, 3) if sends else 0,
                round(clickers / openers * 100, 3) if openers else 0
            )

        for r in rows:
            adv_id = str(r["adv_id"])
            id_routers = int(r["id_routers"])
            sends = r["sends"] or 0
            clicks = r["clicks"] or 0
            clickers = r["clickers"] or 0
            opens = r["opens"] or 0
            openers = r["openers"] or 0
            unsubs = r["unsubs"] or 0
            ca = r["ca"] or 0
            dep = r.get("departement") or "others"
            if dep=='dep_vide':
                dep="others"

            ca_per_routers[(adv_id, id_routers)] = max(ca_per_routers.get((adv_id, id_routers), 0), ca)
            g = result["globales"]
            g["sends"] += sends
            g["clicks"] += clicks
            g["clickers"] += clickers
            g["opens"] += opens
            g["openers"] += openers
            g["unsubs"] += unsubs
            dep_stats = g["analyse_dep"].setdefault(dep, {"clickers": 0, "openers": 0, "sends": 0,"unsubs":0})
            dep_stats["clickers"] += clickers
            dep_stats["openers"] += openers
            dep_stats["sends"] += sends
            dep_stats["unsubs"]+=unsubs

            adv = advertisers.setdefault(adv_id, {
                "advertiser_id": adv_id,
                "id_routers_list": set(),
                "brands_map": {},
                "brands": [],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "ca": 0.0,
                "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
            })

            adv["id_routers_list"].add(id_routers)
            adv["sends"] += sends
            adv["clicks"] += clicks
            adv["clickers"] += clickers
            adv["opens"] += opens
            adv["openers"] += openers
            adv["unsubs"] += unsubs

            dims = {
                "age_range": r.get("age_range") or "O_age",
                "gender": r.get("gender") or "O_gender",
                "isp": r.get("main_isp") or "O_isp",
            }
            for dim, val in dims.items():
                seg = adv["dimensions"][dim].setdefault(val, {
                    "sends": 0, "clicks": 0, "clickers": 0,
                    "opens": 0, "openers": 0, "unsubs": 0
                })
                seg["sends"] += sends
                seg["clicks"] += clicks
                seg["clickers"] += clickers
                seg["opens"] += opens
                seg["openers"] += openers
                seg["unsubs"] += unsubs

            brand_name =r["brand"]
            segment_id = r.get("segmentId", [None])[0] if r.get("segmentId") else None
            
            optimized_url = r.get("optimized") or ""
            subject = r["subject"] or ""
            date_schedule = r.get("date_schedule")
            comment = r.get("comment")
            if comment=='None':
                comment=''
            brand = adv["brands_map"].get(brand_name)
            if not brand:
                brand = {
                    "name": brand_name,
                    "creativities": optimized_url,
                    "subject": subject,
                    "segment_id":segment_id,
                    "comment":comment,
                    "date_schedule":date_schedule,
                    "sends": 0, "clicks": 0, "clickers": 0,
                    "opens": 0, "openers": 0, "unsubs": 0,
                    "ca": 0
                }
                adv["brands_map"][brand_name] = brand
                adv["brands"].append(brand)

            brand["sends"] += sends
            brand["clicks"] += clicks
            brand["clickers"] += clickers
            brand["opens"] += opens
            brand["openers"] += openers
            brand["unsubs"] += unsubs
            brand["ca"] += ca
            brand["creativities"] = optimized_url
            brand["subject"] = subject

        for adv_id, adv in advertisers.items():
            adv["ca"] = sum(ca_per_routers[(adv_id, r)] for r in adv["id_routers_list"])
            result["globales"]["ca"] += adv["ca"]

            adv["id_routers"] = int(next(iter(adv["id_routers_list"])))
            adv.pop("id_routers_list")
            tc, to, tu, cto = compute_rates(adv["sends"], adv["openers"], adv["clickers"], adv["unsubs"])
            adv["ecpm"] = round((adv["ca"] / adv["sends"]) * 1000, 3) if adv["sends"] else 0
            adv["taux_clickers"] = tc
            adv["taux_openers"] = to
            adv["taux_unsubs"] = tu
            adv["taux_cto"] = cto
            adv["classification"] = self.analyze.classify_advertiser(adv["ecpm"], tc)

            adv["analyses"] = {
                "taux_clickers": self.analyze.analyze_click_rate(tc),
                "taux_cto": self.analyze.analyze_cto_rate(cto, adv["openers"]),
                "taux_unsubs": self.analyze.analyze_unsub_rate(tu)
            }

            for dim in adv["dimensions"].values():
                for seg in dim.values():
                    tc_d, to_d, tu_d, cto_d = compute_rates(seg["sends"], seg["openers"], seg["clickers"], seg["unsubs"])
                    seg.update({
                        "taux_clickers": tc_d,
                        "taux_openers": to_d,
                        "taux_unsubs": tu_d,
                        "taux_cto": cto_d,
                        "analyses": {
                            "taux_clickers": self.analyze.analyze_click_rate(tc_d),
                            "taux_cto": self.analyze.analyze_cto_rate(cto_d, seg["openers"]),
                            "taux_unsubs": self.analyze.analyze_unsub_rate(tu_d)
                        }
                    })

            for b in adv["brands"]:
                tc_b, to_b, tu_b, cto_b = compute_rates(b["sends"], b["openers"], b["clickers"], b["unsubs"])
                b.update({
                    "taux_clickers": tc_b,
                    "taux_openers": to_b,
                    "taux_unsubs": tu_b,
                    "taux_cto": cto_b,
                    "analyses": {
                        "taux_clickers": self.analyze.analyze_click_rate(tc_b),
                        "taux_cto": self.analyze.analyze_cto_rate(cto_b, b["openers"]),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(tu_b)
                    }
                })
            adv.pop("brands_map")
            result["advertisers"].append(adv)

        g = result["globales"]
        tc, to, tu, cto = compute_rates(g["sends"], g["openers"], g["clickers"], g["unsubs"])
        g["ecpm"] = round((g["ca"] / g["sends"]) * 1000, 3) if g["sends"] else 0
        g["taux_clickers"] = tc
        g["taux_openers"] = to
        g["taux_unsubs"] = tu
        g["taux_cto"] = cto
        g["analyses"] = {
            "taux_clickers": self.analyze.analyze_click_rate(tc),
            "taux_cto": self.analyze.analyze_cto_rate(cto, g["openers"]),
            "taux_unsubs": self.analyze.analyze_unsub_rate(tu)
        }
        total_sends = g["sends"] 
        for dep, stats in g["analyse_dep"].items():
            stats["taux_clickers"] = round(stats["clickers"] / total_sends * 100, 3) if total_sends else 0
            stats["taux_openers"] = round(stats["openers"] / total_sends * 100, 3) if total_sends else 0
            stats["taux_unsubs"] = round(stats.get("unsubs", 0) / total_sends * 100, 3) if total_sends else 0
        return result
    
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
                SUM(r.sends) AS sends,
                SUM(r.openers) AS openers,
                SUM(r.clickers) AS clickers,
                SUM(r.unsubs) AS unsubs
            FROM {self.table} r
            {where_clause}
            GROUP BY r.adv_id, r.tag_id
        ),
        ca_unique AS (
            SELECT 
                adv_id,
                SUM(ca) AS ca_global
            FROM (
                SELECT 
                    r.adv_id,
                    r.id_routers,
                    MAX(r.ca) AS ca
                FROM {self.table} r
                {where_clause}
                GROUP BY r.adv_id, r.id_routers
            )
            GROUP BY adv_id
        )
        SELECT 
            s.adv_id AS adv_id,
            s.tag_id AS tag_id,
            a.name AS advertiser_name,
            s.sends,
            s.openers,
            s.clickers,
            s.unsubs,
            cu.ca_global,
            ROUND(cu.ca_global / NULLIF(s.sends, 0) * 1000, 3) AS ecpm,
            ROUND(s.clickers / NULLIF(s.openers,0) * 100,3) AS taux_cto,
            ROUND(s.openers / NULLIF(s.sends,0) * 100,3) AS taux_openers, 
            ROUND(s.clickers / NULLIF(s.sends,0) * 100,3) AS taux_clickers,
            ROUND(s.unsubs / NULLIF(s.sends,0) * 100,3) AS taux_unsubs
        FROM stats s
        JOIN ca_unique cu ON s.adv_id = cu.adv_id
        JOIN advertiser a ON s.adv_id = a.id"""
        #JOIN tags t ON s.tag_id = t.id

        rows = self._execute_query(query)

        result = []
        for row in rows:
            clickers = row["clickers"] or 0.0
            sends = row["sends"] or 0.0
            unsubs = row["unsubs"] or 0.0
            cto = row["taux_cto"] or 0.0
            openers = row["openers"] or 0.0
            taux_clickers = row["taux_clickers"] or 0.0
            taux_unsubs = row["taux_unsubs"] or 0.0
            taux_openers = row["taux_openers"] or 0.0
            result.append({
                "advertiser_id": row["adv_id"],
                "advertiser_name": row["advertiser_name"],
                "tag_id": row["tag_id"],
                "globales": {
                    "sends": sends,
                    "openers": openers,
                    "clickers": clickers,
                    "unsubs": unsubs,
                    "ca": row["ca_global"] or 0.0,
                    "ecpm": row["ecpm"] or 0.0,
                    "taux_openers": taux_openers,
                    "taux_clickers": taux_clickers,
                    "taux_unsubs": taux_unsubs,
                    "analyse": {
                        "taux_clickers": self.analyze.analyze_click_rate(taux_clickers or 0.0),
                        "taux_cto": self.analyze.analyze_cto_rate(cto, openers ),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(taux_unsubs or 0.0)
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
            tags_value = ",".join((f"'{t}'" for t in tags))
            conditions.append(f"t.tag IN ({tags_value})")

        if country:
            if isinstance(country, str):
                country = [country]
            joins.append("JOIN country c ON r.country = c.id")
            country_values = ",".join((f"'{c}'" for c in country))
            conditions.append(f"c.dwh_name IN ({country_values})")

        if date_schedule:
            conditions.append(f"has(r.date_schedule, '{date_schedule}')")

        if date_start and date_end:
            conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', r.date_schedule)")
        join_clause = " ".join(joins)
        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)

        query = f"""
        WITH stats AS (
            SELECT 
                r.database_id,
                SUM(r.sends) AS sends,
                SUM(r.openers) AS openers,
                SUM(r.clickers) AS clickers,
                SUM(r.unsubs) AS unsubs
            FROM {self.table} r
            {join_clause}
            {where_clause}
            GROUP BY r.database_id
        ),
        ca_unique AS (
            SELECT 
                database_id,
                SUM(ca) AS ca_global
            FROM (
                SELECT 
                    r.database_id,
                    r.id_routers,
                    MAX(r.ca) AS ca
                FROM {self.table} r
                {join_clause}
                {where_clause}
                GROUP BY r.database_id, r.id_routers
            )
            GROUP BY database_id
        )
        SELECT 
            s.database_id AS database_id,
            d.basename,
            s.sends,
            s.openers,
            s.clickers,
            s.unsubs,
            cu.ca_global,
            ROUND(cu.ca_global / NULLIF(s.sends, 0) * 1000, 3) AS ecpm,
            ROUND(s.openers / NULLIF(s.sends,0) * 100,3) AS taux_openers, 
            ROUND(s.clickers / NULLIF(s.openers,0) * 100,3) AS taux_cto,
            ROUND(s.clickers / NULLIF(s.sends,0) * 100,3) AS taux_clickers,
            ROUND(s.unsubs / NULLIF(s.sends,0) * 100,3) AS taux_unsubs
        FROM stats s
        JOIN ca_unique cu ON s.database_id = cu.database_id
        JOIN databases d ON s.database_id = d.id
        """
        rows = self._execute_query(query)
        result = []
        for row in rows:
            clickers = row["clickers"] or 0.0
            sends = row["sends"] or 0.0
            unsubs = row["unsubs"] or 0.0
            cto = row["taux_cto"] or 0.0
            openers = row["openers"] or 0.0
            taux_clickers = row["taux_clickers"] or 0.0
            taux_unsubs = row["taux_unsubs"] or 0.0
            taux_openers = row["taux_openers"] or 0.0
            result.append({
                "database_id": row["database_id"],
                "database_name": row["basename"],
                "globales": {
                    "sends":sends,
                    "openers": openers,
                    "clickers": clickers,
                    "usubs": unsubs,
                    "ca": row["ca_global"] or 0.0,
                    "ecpm": row["ecpm"] or 0.0,
                    "taux_openers": taux_openers,
                    "taux_clickers": taux_clickers,
                    "taux_unsubs": taux_unsubs,
                    "analyse": {
                        "taux_clickers": self.analyze.analyze_click_rate(taux_clickers if taux_clickers else 0.0),
                        "taux_cto": self.analyze.analyze_cto_rate(cto, openers),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(taux_unsubs if taux_unsubs else 0.0)
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