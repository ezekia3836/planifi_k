
from unittest import result

from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
import math
import pandas as pd
import os
import json
from reporting.auto import data
CSV_DIR="csv_segments"
DIMENSIONS=(
    ("age_range","age_range"),
    ("gender","gender"),
    ("isp","main_isp")
)
METRICS_KEY=("sends","clicks","clickers","opens","openers","unsubs")
class Query2:
    def __init__(self):
        self.analyze = analyse()
        self.table = "clean_reporting"
        self.segment_index = self.build_segment_index()
    
    def _execute_query(self, query, params=None):
        clk = ClickHouseConfig().getClient_prod()
        result = clk.query(query, parameters=params or {})
        return [dict(zip(result.column_names, r)) for r in result.result_rows]
    def _stream_query(self, query, params=None):
        clk = ClickHouseConfig().getClient_prod()

        with clk.query_row_block_stream(
            query,
            parameters=params or {}
        ) as stream:

            columns = stream.source.column_names

            for block in stream:
                for row in block:
                    yield dict(zip(columns, row))
    def _get_analyse_dep(self, params: dict, id_col: str, date_filter: str) -> dict:

        param = "%(adv_id)s" if id_col == "adv_id" else "%(db_id)s"
        query = f"""
            SELECT dep,
                sum(clickers)  AS clickers,
                sum(openers)   AS openers,
                sum(unsubs)    AS unsubs,
                sum(sends)     AS sends
            FROM {self.table}
            WHERE {id_col} = {param} {date_filter}
            GROUP BY dep HAVING sends > 0
        """
        result = {}
        for r in self._stream_query(query, params):
            dep   = (r.get("dep") or "").strip()
            dep   = dep if (len(dep) == 2 and dep.isdigit()) else "others"
            sends = r["sends"] or 0
            stats = {
                "sends":         sends,
                "clickers":      r["clickers"],
                "taux_clickers": round(r["clickers"] / sends * 100, 4) if sends else 0.0,
                "taux_openers":  round(r["openers"]  / sends * 100, 4) if sends else 0.0,
                "taux_unsubs":   round(r["unsubs"]    / sends * 100, 3) if sends else 0.0,
            }
            if stats["clickers"] or stats["taux_openers"] or stats["taux_unsubs"]:
                result[dep] = stats
        return result
    def build_dimension_analysis(self, dim_data: dict) -> dict:
        empty = {"privilégier": None, "éviter": None}
        if not dim_data:
            return empty
        ranked = []

        for key, val in dim_data.items():
            sends = val.get("sends", 0) or 0
            clickers = val.get("clickers", 0) or 0
            if sends <= 0:
                continue
            ctr = (clickers / sends) * 100
            fiabilite = sends / (sends + 100)
            score = ctr * math.log10(sends + 1) * fiabilite

            ranked.append({
                "value": key,
                "ctr": round(ctr, 4),
                "sends": sends,
                "score": score
            })

        if not ranked:
            return empty

        ranked.sort(key=lambda x: x["score"], reverse=True)
        MIN_CTR = 0.50
        MIN_SENDS = 20

        good = [
            x for x in ranked
            if x["ctr"] >= MIN_CTR and x["sends"] >= MIN_SENDS
        ]
        if good:
            top = good[:2]
        else:
            top = ranked[:1]
        
        top_value = {t["value"] for t in top}
        bad_candidates = [x for x in ranked if x["value"] not in top_value]

        bad = []
        for x in bad_candidates:
            low_ctr = x["ctr"] < MIN_CTR
            low_volume = x["sends"] < MIN_SENDS
            low_score = x["score"] < (ranked[-1]["score"] * 1.2)

            if (low_ctr and low_volume) or low_score:
                bad.append(x)

        if bad:
            worst = min(bad, key=lambda x: x["ctr"])
        elif bad_candidates:
            worst = min(bad_candidates, key=lambda x: x["ctr"])
        else:
            worst = None

        return {
            "privilegier": [
                {"value": x["value"], "ctr": round(x["ctr"], 4)}
                for x in top
            ],
            "eviter": (
                [
                    {
                        "value": worst["value"],
                        "ctr": round(worst["ctr"], 4)
                    }
                ] if worst else None
            )
        }
    
    def _safe_analyse_dimensions(self, dimensions: dict) -> dict:
        return {
            "age_range": self.build_dimension_analysis(
                dimensions.get("age_range") or {}
            ),
            "gender": self.build_dimension_analysis(
                dimensions.get("gender") or {}
            ),
            "isp": self.build_dimension_analysis(
                dimensions.get("isp") or {}
            ),
        }
    def _run_global(self, query, params, pivot_key, group_label):
        def acc_all_dimensions(target, row, metrics):
            dims = target["dimensions"]
            age = row["age_range"]
            if age:
                seg = dims["age_range"].setdefault(
                    age,
                    {
                        "sends": 0,
                        "clicks": 0,
                        "clickers": 0,
                        "opens": 0,
                        "openers": 0,
                        "unsubs": 0,
                    }
                )
                for k, v in metrics.items():
                    seg[k] += v

            gender = row["gender"]
            if gender:
                seg = dims["gender"].setdefault(
                    gender,
                    {
                        "sends": 0,
                        "clicks": 0,
                        "clickers": 0,
                        "opens": 0,
                        "openers": 0,
                        "unsubs": 0,
                    }
                )

                for k, v in metrics.items():
                    seg[k] += v

            isp = row["main_isp"]
            if isp:
                seg = dims["isp"].setdefault(
                    isp,
                    {
                        "sends": 0,
                        "clicks": 0,
                        "clickers": 0,
                        "opens": 0,
                        "openers": 0,
                        "unsubs": 0,
                    }
                )

                for k, v in metrics.items():
                    seg[k] += v
        def compute_rates(sends, openers, clickers, unsubs):
            return (
                round(clickers / sends   * 100, 3) if sends   else 0,
                round(openers  / sends   * 100, 3) if sends   else 0,
                round(unsubs   / sends   * 100, 3) if sends   else 0,
                round(clickers / openers * 100, 3) if openers else 0,
            )

        def acc_dim(target, dim, value, metrics):
            if not value:
                return

            seg = target["dimensions"][dim].setdefault(
                value,
                {
                    "sends": 0,
                    "clicks": 0,
                    "clickers": 0,
                    "opens": 0,
                    "openers": 0,
                    "unsubs": 0,
                }
            )

            for k, v in metrics.items():
                seg[k] += v

        def finalize_dim(seg):
            tc, to, tu, cto = compute_rates(
                seg["sends"], seg["openers"], seg["clickers"], seg["unsubs"])
            seg.update(taux_clickers=tc, taux_openers=to,
                    taux_unsubs=tu,  taux_cto=cto,
                    analyses={
                        "taux_clickers": self.analyze.analyze_click_rate(tc),
                        "taux_cto":      self.analyze.analyze_cto_rate(cto, seg["openers"]),
                        "taux_unsubs":   self.analyze.analyze_unsub_rate(tu),
                    })

        def parse_models(raw):
            out = []
            for i, m in enumerate(raw or []):
                if isinstance(m, str):
                    try:    m = json.loads(m)
                    except: continue
                if isinstance(m, dict):
                    out.append({"model":    str(m.get("model") or ""),
                                "payvalue": float(m.get("payvalue") or 0),
                                "comment":  str(m.get("comment") or "")})
                elif isinstance(m, (list, tuple)) and len(m) >= 3:
                    out.append({"model":    str(m[0] or ""),
                                "payvalue": float(m[1] or 0),
                                "comment":  str(m[2] or "")})
            return out

        def empty_dims():
            return {"age_range": {}, "gender": {}, "isp": {}}
        def finalize_dimensions(dimensions):
            for dim in dimensions.values():
                for seg in dim.values():
                    finalize_dim(seg)

        groups  = {} 
        total  = dict(sends=0, clicks=0, clickers=0,opens=0, openers=0, unsubs=0, ca=0)
        seen_focus_global = set()
        seen_dwh_global   = set()
        found_any   = False
        meta  = {} 
        for r in self._stream_query(query, params):
            found_any = True
            pivot_val = r[pivot_key]
            group = groups.setdefault(pivot_val, {
                pivot_key:    pivot_val,
                "subject":    r["subject"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "ca": 0,
                "brands":     {},
                "dimensions": empty_dims(),
                "_seen_focus": set(),
                "advertiser_name": r.get("advertiser_name"),   
                "advertiser_id":   r.get("adv_id"),            
            })

            if "advertiser_name" not in meta and r.get("advertiser_name"):
                meta["advertiser_name"] = r["advertiser_name"]

            sends = r["sends"]    or 0
            clicks  = r["clicks"]   or 0
            clickers = r["clickers"] or 0
            opens = r["opens"]    or 0
            openers  = r["openers"]  or 0
            unsubs = r["unsubs"]   or 0
            metrics = {
                "sends": sends,
                "clicks": clicks,
                "clickers": clickers,
                "opens": opens,
                "openers": openers,
                "unsubs": unsubs,
            }
            dwh_id = r.get("dwh_id")
            if dwh_id and dwh_id not in seen_dwh_global:
                seen_dwh_global.add(dwh_id)
                for k, v in metrics.items():
                    total[k] += v
            acc_all_dimensions(group, r, metrics)
            brand_key = (r["brand"], r["tag_id"], r["client_id"], r["id_focus"])
            brand = group["brands"].setdefault(brand_key, {
                "name":         r["brand"],
                "tag_id":       r["tag_id"],
                "agence_id":    r["client_id"],
                "creativities": r.get("optimized") or "",
                "subject":      r["subject"],
                "id_routers":   set(), "segment_id": set(),
                "ListId":       set(), "ListName":   set(),
                "date_schedule": set(),
                "models":       [], "_models_set": False,
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "clicks_val": 0, "leads_val": 0, "volume_val": 0,
                "ca": 0,
                "dimensions":   empty_dims(),
                "_seen_focus":  set()
            })

            brand["clicks_val"] = r.get("clicks_val") or brand["clicks_val"] or 0
            brand["leads_val"]  = r.get("leads_val")  or brand["leads_val"]  or 0
            brand["volume_val"] = r.get("volume_val") or brand["volume_val"] or 0

            if not brand["_models_set"]:
                brand["models"] = parse_models(r.get("models"))
                brand["_models_set"] = True

            brand["id_routers"].update(r.get("id_routers") or [])
            dates = r.get("date_schedule") or []
            if dates and isinstance(dates[0], (list, tuple)):
                dates = [d for sub in dates for d in sub]
            brand["date_schedule"].update(dates)
            segs = r.get("segmentId") or []
            if isinstance(segs, (int, str)):
                segs = [segs]
            brand["segment_id"].update(s for s in segs if s not in (0, "0", "", None))
            brand["ListId"].update(r.get("ListId") or [])
            brand["ListName"].update(r.get("ListName") or [])
            for k,v in metrics.items():
                brand[k] += v
            acc_all_dimensions(brand, r, metrics)
            id_focus = r.get("id_focus")
            ca  = r.get("ca") or 0
            if id_focus:
                if id_focus not in brand["_seen_focus"]:
                    brand["_seen_focus"].add(id_focus)
                    brand["ca"] += ca
                if id_focus not in group["_seen_focus"]:
                    group["_seen_focus"].add(id_focus)
                    group["ca"] += ca
                if id_focus not in seen_focus_global:
                    seen_focus_global.add(id_focus)
                    total["ca"] += ca
            for k, v in metrics.items():
                total[k] += v
        if not found_any:
            top_key = "advertiser_id" if pivot_key == "database_id" else "database_id"
            top_val = str(params.get("adv_id") or params.get("db_id") or "")
            return {top_key: top_val, "globales": {}, group_label: []}

        result = []
        for group in groups.values():
            brands_list = []
            for brand in group["brands"].values():
                finalize_dimensions(brand["dimensions"])

                tc, to, tu, cto = compute_rates(
                    brand["sends"], brand["openers"], brand["clickers"], brand["unsubs"])
                brand.update(
                    taux_clickers=tc, taux_openers=to,
                    taux_unsubs=tu,   taux_cto=cto,
                    ecpm=round(brand["ca"] / brand["sends"] * 1000, 3) if brand["sends"] else 0,
                    id_routers   =list(brand["id_routers"]),
                    segment_id   =list(brand["segment_id"]),
                    ListId        =list(brand["ListId"]),
                    ListName      =list(brand["ListName"]),
                    date_schedule =sorted(brand["date_schedule"]),
                )
                brand.pop("_seen_focus",  None)
                brand.pop("_models_set",  None)
                brands_list.append(brand)

            finalize_dimensions(group["dimensions"])
            tc, to, tu, cto = compute_rates(
                group["sends"], group["openers"], group["clickers"], group["unsubs"])
            ecpm = round(group["ca"] / group["sends"] * 1000, 3) if group["sends"] else 0

            entry = {
                pivot_key:   group[pivot_key],
                "brands":    brands_list,
                "sends":     group["sends"],    "clicks":   group["clicks"],
                "clickers":  group["clickers"], "opens":    group["opens"],
                "openers":   group["openers"],  "unsubs":   group["unsubs"],
                "taux_clickers": tc, "taux_openers": to,
                "taux_unsubs":   tu, "taux_cto":     cto,
                "ecpm":      ecpm,
                "ca":        group["ca"],
                "classification": self.analyze.classify_advertiser(ecpm, tc),
                "dimensions": group["dimensions"],
                "recommendation_segments": self._safe_analyse_dimensions(group["dimensions"]),
            }
            if group.get("advertiser_name"):
                entry["advertiser_name"] = group["advertiser_name"]
            if group.get("advertiser_id"):
                entry["advertiser_id"] = str(group["advertiser_id"])

            group.pop("_seen_focus", None)
            result.append(entry)
        tc_g, to_g, tu_g, cto_g = compute_rates(
            total["sends"], total["openers"], total["clickers"], total["unsubs"])
        ecpm_g = round(total["ca"] / total["sends"] * 1000, 3) if total["sends"] else 0

        top_key = "advertiser_id" if pivot_key == "database_id" else "database_id"
        top_val = str(params.get("adv_id") or params.get("db_id") or "")

        return {
            top_key: top_val,
            **( {"advertiser_name": meta.get("advertiser_name")} if "advertiser_name" in meta else {} ),
            "globales": {
                **total,
                "ecpm":          ecpm_g,
                "taux_clickers": tc_g,
                "taux_openers":  to_g,
                "taux_unsubs":   tu_g,
                "taux_cto":      cto_g,
                "analyses": {
                    "taux_clickers": self.analyze.analyze_click_rate(tc_g),
                    "taux_cto":      self.analyze.analyze_cto_rate(cto_g, total["openers"]),
                    "taux_unsubs":   self.analyze.analyze_unsub_rate(tu_g),
                },
                "analyse_dep": {},
            },
            group_label: sorted(result, key=lambda x: (x["clickers"], x["ecpm"]), reverse=True),
        }
    def global_advertiser(self, adv_id, date_schedule=None, date_start=None, date_end=None):
        if date_schedule:
            date_filter = f"AND has(t.date_schedule, '{date_schedule}')"
        elif date_start and date_end:
            date_filter = f"AND arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', t.date_schedule)"
        else:
            date_filter = f"""
                AND toStartOfMonth(date_schedule_max) >= toStartOfMonth(
                    subtractMonths(
                        (SELECT max(date_schedule_max) FROM {self.table} WHERE adv_id = %(adv_id)s),
                        2
                    )
                )
            """
        query = f"""
            WITH ca_by_focus AS (
                SELECT id_focus, max(ca) AS ca
                FROM {self.table} WHERE adv_id = %(adv_id)s GROUP BY id_focus
            ),
            base AS (
                SELECT t.*, a.name AS advertiser_name,
                    startsWith(ifNull(t.ListName,''),'Z-DWH') AS is_zdwh
                FROM {self.table} t
                LEFT JOIN advertiser a ON a.id = t.adv_id
                WHERE t.adv_id = %(adv_id)s {date_filter}
            )
            SELECT t.adv_id, t.database_id,
                groupUniqArray(t.id_routers) AS id_routers,
                t.tag_id, max(t.clicks_val) AS clicks_val,
                max(t.leads_val) AS leads_val, max(t.cpm_val) AS volume_val,
                t.brand, any(t.model) AS models, t.client_id, t.optimized,
                t.subject,groupUniqArray(t.date_schedule) AS date_schedule, t.gender, t.age_range, t.main_isp,
                t.id_focus, t.dep AS departement,
                sum(t.sends) AS sends, sum(t.clicks) AS clicks,
                sum(t.clickers) AS clickers,
                sum(t.opens) AS opens,
                sum(t.openers) AS openers,
                countIf(t.unsubs > 0) AS unsubs,
                any(f.ca) AS ca, any(t.advertiser_name) AS advertiser_name,
                groupUniqArray(t.segmentId) AS segmentId,
                groupUniqArrayIf(t.ListId,   t.is_zdwh) AS ListId,
                groupUniqArrayIf(t.ListName, t.is_zdwh) AS ListName
            FROM base t
            LEFT JOIN ca_by_focus f ON f.id_focus = t.id_focus
            GROUP BY t.adv_id, t.database_id, t.tag_id,
                    t.brand, t.client_id, t.optimized, t.subject,
                    t.gender, t.age_range, t.main_isp, t.id_focus, t.dep
            HAVING sends > 0
        """
        result = self._run_global(query, {"adv_id": adv_id}, pivot_key="database_id", group_label="bases")
        result["globales"]["analyse_dep"] = self._get_analyse_dep({"adv_id": adv_id}, "adv_id", date_filter)
        return result


    def global_base(self, db_id, date_schedule=None, date_start=None, date_end=None):
        if date_schedule:
            date_filter = f"AND has(t.date_schedule, '{date_schedule}')"
        elif date_start and date_end:
            date_filter = f"AND arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', t.date_schedule)"
        else:
            date_filter = f"""
                AND date_schedule_max >= (
                    SELECT toStartOfMonth(
                        subtractMonths(max(date_schedule_max), 2)
                    )
                    FROM {self.table}
                )
            """

        query = f"""
            WITH ca_by_focus AS (
                SELECT id_focus, max(ca) AS ca
                FROM {self.table} WHERE database_id = %(db_id)s GROUP BY id_focus
            ),
            base AS (
                SELECT t.*, a.name AS advertiser_name,
                    startsWith(ifNull(t.ListName,''),'Z-DWH') AS is_zdwh
                FROM {self.table} t
                LEFT JOIN advertiser a ON a.id = t.adv_id
                WHERE t.database_id = %(db_id)s
                AND t.adv_id != 0
                {date_filter}
            )
            SELECT t.adv_id, t.database_id,
                groupUniqArray(t.id_routers) AS id_routers,
                t.tag_id, any(t.model) AS models, t.brand, t.client_id,
                t.optimized, t.subject,  groupUniqArray(t.date_schedule) AS date_schedule, t.gender,
                t.age_range, t.main_isp, t.id_focus, t.dep AS departement,
                sum(t.sends) AS sends, 
                sum(t.clicks) AS clicks,
                sum(t.clickers)  AS clickers,
                sum(t.openers)  AS openers,
                countIf(t.unsubs > 0)  AS unsubs,
                sum(t.opens) AS opens,
                any(f.ca) AS ca,
                MAX(t.clicks_val) AS clicks_val, MAX(t.leads_val) AS leads_val,
                MAX(t.cpm_val) AS volume_val,
                any(t.advertiser_name) AS advertiser_name,
                groupUniqArray(t.segmentId) AS segmentId,
                groupUniqArrayIf(t.ListId,   t.is_zdwh) AS ListId,
                groupUniqArrayIf(t.ListName, t.is_zdwh) AS ListName
            FROM base t
            LEFT JOIN ca_by_focus f ON f.id_focus = t.id_focus
            GROUP BY t.adv_id, t.database_id, t.tag_id,
                    t.brand, t.client_id, t.optimized, t.subject,
                    t.gender, t.age_range, t.main_isp, t.id_focus, t.dep
            HAVING sends > 0
        """
        result = self._run_global(query, {"db_id": db_id}, pivot_key="adv_id", group_label="advertisers")
        result["globales"]["analyse_dep"] = self._get_analyse_dep({"db_id": db_id}, "database_id", date_filter)
        return result
    
    def all_advertisers(self, date_schedule=None, date_start=None, date_end=None):
        conditions = []

        if date_schedule:
            conditions.append(f"has(date_schedule, '{date_schedule}')")

        if date_start and date_end:
            conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', date_schedule)"
            )

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        else:
            where_clause = f"""
                WHERE adv_id IS NOT NULL AND adv_id != 0
                AND date_schedule_max >= (
                    SELECT toStartOfMonth(
                        subtractMonths(max(date_schedule_max), 2)
                    )
                    FROM {self.table}
                )"""
        query = f"""
            WITH base AS (
                SELECT
                    adv_id,
                    tag_id,
                    id_focus,
                    dwh_id,
                    sends,
                    opens,
                    clicks,
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
                    sum(sends)                           AS sends,
                    countIf(opens > 0)  AS openers,
                    countIf(clicks > 0) AS clickers,
                    countIf(unsubs > 0) AS unsubs,
                    max(ca)                               AS ca
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
                s.adv_id                                                            AS adv_id,
                a.name                                                              AS advertiser_name,
                s.tag_id                                                            AS tag_id,
                s.sends                                                             AS sends,
                s.openers                                                           AS openers,
                s.clickers                                                          AS clickers,
                s.unsubs                                                            AS unsubs,
                coalesce(c.ca_global, 0)                                            AS ca,
                round(s.openers  / nullIf(s.sends,   0) * 100, 3)                  AS taux_openers,
                round(s.clickers / nullIf(s.sends,   0) * 100, 3)                  AS taux_clickers,
                round(s.clickers / nullIf(s.openers, 0) * 100, 3)                  AS taux_cto,
                round(s.unsubs   / nullIf(s.sends,   0) * 100, 3)                  AS taux_unsubs,
                round(coalesce(c.ca_global, 0) / nullIf(s.sends, 0) * 1000, 3)     AS ecpm
            FROM stats s
            LEFT JOIN ca_final c ON s.adv_id = c.adv_id AND s.tag_id = c.tag_id
            JOIN advertiser a    ON a.id = s.adv_id
            ORDER BY s.sends DESC
        """

        rows = self._execute_query(query)
        result = []
        for row in rows:
            sends         = row["sends"]         or 0
            openers       = row["openers"]       or 0
            clickers      = row["clickers"]      or 0
            unsubs        = row["unsubs"]        or 0
            taux_cto      = row["taux_cto"]      or 0.0
            taux_clickers = row["taux_clickers"] or 0.0
            taux_openers  = row["taux_openers"]  or 0.0
            taux_unsubs   = row["taux_unsubs"]   or 0.0

            result.append({
                "advertiser_id":   str(row["adv_id"]),
                "advertiser_name": row["advertiser_name"],
                "tag_id":          row["tag_id"],
                "globales": {
                    "sends":          sends,
                    "openers":        openers,
                    "clickers":       clickers,
                    "unsubs":         unsubs,
                    "ca":             round(row["ca"], 3) or 0.0,
                    "ecpm":           round(row["ecpm"], 3) if row["ecpm"] is not None else 0.0,
                    "taux_openers":   taux_openers,
                    "taux_clickers":  taux_clickers,
                    "taux_unsubs":    taux_unsubs,
                    "analyse": {
                        "taux_clickers": self.analyze.analyze_click_rate(taux_clickers),
                        "taux_cto":      self.analyze.analyze_cto_rate(taux_cto, openers),
                        "taux_unsubs":   self.analyze.analyze_unsub_rate(taux_unsubs),
                    },
                },
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
        if date_schedule:
            conditions.append(f"has(date_schedule, '{date_schedule}')")

        if date_start and date_end:
            conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', date_schedule)"
            )
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        else:
             where_clause = f"""
                WHERE r.adv_id IS NOT NULL AND r.adv_id != 0
                AND date_schedule_max >= (
                    SELECT toStartOfMonth(
                        subtractMonths(max(date_schedule_max), 2)
                    )
                    FROM {self.table}
                )"""
             
        query = f"""
            WITH filtered AS (
                SELECT
                    r.database_id,
                    r.id_focus,
                    r.dwh_id,
                    r.sends,
                    r.clicks,
                    r.clickers,
                    r.openers,
                    r.opens,
                    r.unsubs
                FROM {self.table} r
                {join_clause}
                {where_clause}
                AND r.sends > 0
            ),
            stats AS (
                SELECT
                    f.database_id,
                    sum(f.sends)         AS sends,
                    sum(f.clickers)      AS clickers,
                    sum(f.openers)       AS openers,
                    countIf(f.unsubs > 0)  AS unsubs
                FROM filtered f
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
                HAVING max(sends) > 0
            ),
            ca_unique AS (
                SELECT
                    database_id,
                    sum(ca) AS ca_global
                FROM ca_by_focus
                GROUP BY database_id
            )
            SELECT
                s.database_id                                                        AS database_id,
                d.basename                                                           AS basename,
                s.sends                                                              AS sends,
                s.clickers                                                           AS clickers,
                s.openers                                                            AS openers,
                s.unsubs                                                             AS unsubs,
                coalesce(cu.ca_global, 0)                                            AS ca_global,
                round(s.openers  / nullIf(s.sends,   0) * 100, 3)                   AS taux_openers,
                round(s.clickers / nullIf(s.sends,   0) * 100, 3)                   AS taux_clickers,
                round(s.clickers / nullIf(s.openers, 0) * 100, 3)                   AS taux_cto,
                round(s.unsubs   / nullIf(s.sends,   0) * 100, 3)                   AS taux_unsubs,
                round(coalesce(cu.ca_global, 0) / nullIf(s.sends, 0) * 1000, 3)     AS ecpm
            FROM stats s
            LEFT JOIN ca_unique cu ON cu.database_id = s.database_id
            JOIN databases d   ON d.id = s.database_id
            ORDER BY s.sends DESC
        """

        rows = self._execute_query(query)
        result = []
        for row in rows:
            sends    = row["sends"]    or 0
            openers  = row["openers"]  or 0
            clickers = row["clickers"] or 0
            unsubs   = row["unsubs"]   or 0

            result.append({
                "database_id":   row["database_id"],
                "database_name": row["basename"],
                "globales": {
                    "sends":          sends,
                    "openers":        openers,
                    "clickers":       clickers,
                    "unsubs":         unsubs,
                    "ca":             round(row["ca_global"], 3) or 0.0,
                    "ecpm":           row["ecpm"] or 0.0,
                    "taux_openers":   row["taux_openers"]  or 0.0,
                    "taux_clickers":  row["taux_clickers"] or 0.0,
                    "taux_cto":       row["taux_cto"]      or 0.0,
                    "taux_unsubs":    row["taux_unsubs"]   or 0.0,
                    "analyse": {
                        "taux_clickers": self.analyze.analyze_click_rate(row["taux_clickers"] or 0.0),
                        "taux_cto":      self.analyze.analyze_cto_rate(row["taux_cto"] or 0.0, openers),
                        "taux_unsubs":   self.analyze.analyze_unsub_rate(row["taux_unsubs"] or 0.0),
                    },
                },
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
    def get_databases(self, database_id=None):
        query = "SELECT id, basename FROM databases"
        params = {}

        if database_id is not None:
            query += f" WHERE id = {int(database_id)}"

        rows = self._execute_query(query, params)

        return [
            {"database_id": r["id"], "database_name": r["basename"]}
            for r in rows
        ]
    def filter_by_tags(self, tag_id, database_id):
        date_filter = f"""
            AND toStartOfMonth(date_schedule_max) >= toStartOfMonth(
                subtractMonths(
                    (SELECT max(date_schedule_max) FROM {self.table}
                    WHERE tag_id = %(tag_id)s AND database_id = %(database_id)s),
                    2
                )
            )"""
        query = f"""
            SELECT
                dep,
                sum(t.clickers) AS clickers,
                sum(t.openers)  AS openers,
                sum(t.unsubs)   AS unsubs,
                sum(t.sends)    AS sends
            FROM {self.table} t
            WHERE tag_id      = %(tag_id)s
            AND database_id = %(database_id)s
            {date_filter}
            GROUP BY dep
            HAVING sends > 0
        """
        params = {"tag_id": int(tag_id), "database_id": int(database_id)}
        result = {}
        for r in self._stream_query(query, params):
            dep = (r.get("dep") or "").strip()
            if len(dep) != 2 or not dep.isdigit(): 
                dep="Others"
            sends = r["sends"] or 0
            entry = {
                "sends": r["sends"] if r["sends"] is not None else 0,
                "clickers": r["clickers"] if r["clickers"] is not None else 0,
                "taux_clickers": round(r["clickers"] / sends * 100, 4) if sends else 0.0,
                "taux_openers":  round(r["openers"]  / sends * 100, 4) if sends else 0.0,
                "taux_unsubs":   round(r["unsubs"]    / sends * 100, 3) if sends else 0.0,
            }
            if r["clickers"] or r["openers"] or r["unsubs"]:
                result[dep] = entry
        return result