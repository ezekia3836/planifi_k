
from unittest import result
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
import math
import pandas as pd
import statistics
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
    def _get_analyse_dep(self, params: dict, id_col: str, date_filter: str, 
                     tag_id=None,
                     include_o_age=True, include_o_gender=True, include_o_isp=True) -> dict:
        param = "%(adv_id)s" if id_col == "adv_id" else "%(db_id)s"
        
        tag_filter = "AND tag_id = %(tag_id)s" if tag_id is not None else ""
        if tag_id is not None:
            params = {**params, "tag_id": tag_id}

        # Filtres O_xxx
        exclude_filters = []
        if not include_o_age:
            exclude_filters.append("age_range != 'O_age'")
        if not include_o_gender:
            exclude_filters.append("gender != 'O_gender'")
        if not include_o_isp:
            exclude_filters.append("main_isp != 'O_isp'")
        
        other_filter = ('AND ' + ' AND '.join(exclude_filters)) if exclude_filters else ''

        query = f"""
            SELECT dep,
                sum(clickers)  AS clickers,
                sum(openers)   AS openers,
                sum(unsubs)    AS unsubs,
                sum(sends)     AS sends
            FROM {self.table}
            WHERE {id_col} = {param}
            {tag_filter}
            {date_filter}
            {other_filter}
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
    
    def build_dimension_analysis(self, dim_data: dict, dim_name: str = "") -> dict:
        def _empty_values(value:str=""):
            return {
                "value":value,
                "sends":0,
                "clickers":0,
                "openers":0,
                "unsubs":0,
                "taux_clickers":0.0,
                "taux_openers":0.0,
                "taux_unsubs":0.0,
                "taux_cto":0.0,
                "score":0,
                "score_z":0,
            }
        EXCLUDE = {"age_range": {"0-18", "O_age"},"isp":  {"O_isp"},}
        empty = {"privilegier": [_empty_values()], "eviter": [_empty_values()]}
        if not dim_data:
            return empty

        excluded = EXCLUDE.get(dim_name, set())
        rows = []
        for key, val in dim_data.items():
            if key in excluded:
                continue

            sends    = val.get("sends",    0) or 0
            clickers = val.get("clickers", 0) or 0
            openers  = val.get("openers",  0) or 0
            unsubs   = val.get("unsubs",   0) or 0

            if sends <= 0:
                continue

            # if clickers > sends or openers > sends or unsubs > sends:
                # continue

            openers  = min(openers,  sends)
            clickers = min(clickers, openers) if openers else min(clickers, sends)
            unsubs   = min(unsubs,   sends)

            ctr     = clickers / sends
            ctor    = clickers / openers if openers else 0
            open_r  = openers  / sends
            unsub_r = unsubs   / sends

            rows.append({
                "value":    key,
                "sends":    sends,
                "clickers": clickers,
                "openers":  openers,
                "unsubs":   unsubs,
                "ctr":      ctr,
                "ctor":     ctor,
                "open_r":   open_r,
                "unsub_r":  unsub_r,
            })

        if not rows:
            return empty

        # ── 2. Médiane de référence ───────────────────────────────────────────────
        MIN_SENDS = 500
        valid = [r for r in rows if r["sends"] >= MIN_SENDS] or rows

        ctr_values  = [r["ctr"]  for r in valid] or [0.005]
        ctor_values = [r["ctor"] for r in valid if r["ctor"] > 0] or [0.05]
        ctr_median  = statistics.median(ctr_values)
        ctor_median = statistics.median(ctor_values)

        # ── 3. Scoring composite normalisé ───────────────────────────────────────
        K               = 200
        W_CTR           = 0.55
        W_CTOR          = 0.45
        UNSUB_THRESHOLD = 0.003   # 0.30%
        UNSUB_HARD_CAP  = 0.005   # 0.50%

        for r in rows:
            ctr_smooth  = (r["sends"] * r["ctr"]  + K * ctr_median)  / (r["sends"] + K)
            ctor_smooth = (r["sends"] * r["ctor"] + K * ctor_median) / (r["sends"] + K)

            ctr_norm  = ctr_smooth  / ctr_median  if ctr_median  > 0 else 1.0
            ctor_norm = ctor_smooth / ctor_median if ctor_median > 0 else 1.0

            if r["unsub_r"] <= UNSUB_THRESHOLD:
                unsub_penalty = 0.0
            else:
                excess        = r["unsub_r"] - UNSUB_THRESHOLD
                cap           = UNSUB_HARD_CAP - UNSUB_THRESHOLD
                unsub_penalty = min(excess / cap, 1.0) * 1.5

            r["score"] = W_CTR * ctr_norm + W_CTOR * ctor_norm - unsub_penalty

        # ── 4. Tri et score_z ────────────────────────────────────────────────────
        ranked       = sorted(rows, key=lambda x: x["score"], reverse=True)
        scores       = [r["score"] for r in ranked]
        score_median = statistics.median(scores)
        score_std    = statistics.pstdev(scores) or 0.1

        for r in ranked:
            r["score_z"] = (r["score"] - score_median) / score_std

        # ── 5. privilegier — fallback progressif ─────────────────────────────────
        # Niveau 1 : critères stricts
        good = [
            r for r in ranked
            if r["score_z"] >= 0.5
            and r["unsub_r"] < UNSUB_THRESHOLD
            and r["sends"]   >= MIN_SENDS
        ]
        # Niveau 2 : relâcher score_z
        if not good:
            good = [
                r for r in ranked
                if r["unsub_r"] < UNSUB_THRESHOLD
                and r["sends"]  >= MIN_SENDS
            ][:2]
        # Niveau 3 : relâcher unsub
        if not good:
            good = [
                r for r in ranked
                if r["sends"] >= MIN_SENDS
            ][:2]
        # Niveau 4 : tout relâcher
        if not good:
            good = ranked[:1]

        top        = good[:1]
        top_values = {t["value"] for t in top}

        bad = [
            r for r in ranked
            if r["value"] not in top_values
            and (
                r["score_z"] <= -0.5
                or r["unsub_r"] >= UNSUB_HARD_CAP
            )
        ]
       
        if not bad and len(ranked) >= 2:
            bad = [r for r in ranked if r["value"] not in top_values]

        if bad:
            worst = max(bad, key=lambda x: (x["unsub_r"], -x["score"]))
        else:
            worst = None

        def _fmt(r):
            return {
                "value":         r["value"],
                "sends":         r["sends"],
                "clickers":      r["clickers"],
                "openers":       r["openers"],
                "unsubs":        r["unsubs"],
                "taux_clickers": round(r["ctr"]     * 100, 4),
                "taux_openers":  round(r["open_r"]  * 100, 4),
                "taux_unsubs":   round(r["unsub_r"] * 100, 3),
                "taux_cto":      round(r["ctor"]    * 100, 3),
                "score":         round(r["score"],   4),
                "score_z":       round(r["score_z"], 2),
            }

        return {
            "privilegier": [_fmt(r) for r in top] or [_empty_values()],
            "eviter": [_fmt(worst)]  if worst else [_empty_values()],
        }
    def _safe_analyse_dimensions(self, dimensions: dict) -> dict:
        return {
            "age_range": self.build_dimension_analysis(
                dimensions.get("age_range") or {}, dim_name="age_range" 
            ),
            "gender": self.build_dimension_analysis(
                dimensions.get("gender") or {}, dim_name="gender"
            ),
            "isp": self.build_dimension_analysis(
                dimensions.get("isp") or {}, dim_name="isp"
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

        groups            = {}
        analyse_dep       = {}
        total             = dict(sends=0, clicks=0, clickers=0, opens=0, openers=0, unsubs=0, ca=0)
        seen_focus_global = set()
        found_any         = False
        meta              = {}

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

            sends    = r["sends"]    or 0
            clicks   = r["clicks"]   or 0
            clickers = r["clickers"] or 0
            opens    = r["opens"]    or 0
            openers  = r["openers"]  or 0
            unsubs   = r["unsubs"]   or 0
            metrics  = {
                "sends":    sends,
                "clicks":   clicks,
                "clickers": clickers,
                "opens":    opens,
                "openers":  openers,
                "unsubs":   unsubs,
            }

            for k, v in metrics.items():
                group[k] += v
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
                "dimensions":  empty_dims(),
                "_seen_focus": set(),
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

            for k, v in metrics.items():
                brand[k] += v
            acc_all_dimensions(brand, r, metrics)

            id_focus = r.get("id_focus")
            ca = r.get("ca") or 0
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

            raw_dep = str(r.get("departement") or "").strip()
            dep = raw_dep if (len(raw_dep) == 2 and raw_dep.isdigit()) else "others"
            d = analyse_dep.setdefault(dep, {"sends": 0, "clickers": 0, "openers": 0, "unsubs": 0})
            d["sends"]    += metrics["sends"]
            d["clickers"] += metrics["clickers"]
            d["openers"]  += metrics["openers"]
            d["unsubs"]   += metrics["unsubs"]

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
                    ListId =list(brand["ListId"]),
                    ListName =list(brand["ListName"]),
                    date_schedule =sorted(brand["date_schedule"]),
                )
                brand.pop("_seen_focus", None)
                brand.pop("_models_set", None)
                brands_list.append(brand)

            finalize_dimensions(group["dimensions"])
            tc, to, tu, cto = compute_rates(
                group["sends"], group["openers"], group["clickers"], group["unsubs"])
            ecpm = round(group["ca"] / group["sends"] * 1000, 3) if group["sends"] else 0

            entry = {
                pivot_key:       group[pivot_key],
                "brands":        brands_list,
                "sends":         group["sends"],    "clicks":   group["clicks"],
                "clickers":      group["clickers"], "opens":    group["opens"],
                "openers":       group["openers"],  "unsubs":   group["unsubs"],
                "taux_clickers": tc, "taux_openers": to,
                "taux_unsubs":   tu, "taux_cto":     cto,
                "ecpm":          ecpm,
                "ca":            group["ca"],
                "classification": self.analyze.classify_advertiser(ecpm, tc),
                "dimensions":    group["dimensions"],
            }
            if group.get("advertiser_name"):
                entry["advertiser_name"] = group["advertiser_name"]
            if group.get("advertiser_id"):
                entry["advertiser_id"] = str(group["advertiser_id"])

            group.pop("_seen_focus", None)
            result.append(entry)

        global_dimensions = empty_dims()
        for group in groups.values():
            for dim_name, dim_data in group["dimensions"].items():
                for seg_key, seg_vals in dim_data.items():
                    target = global_dimensions[dim_name].setdefault(seg_key, {
                        "sends": 0, "clicks": 0, "clickers": 0,
                        "opens": 0, "openers": 0, "unsubs": 0,
                    })
                    for k in ("sends", "clicks", "clickers", "opens", "openers", "unsubs"):
                        target[k] += seg_vals.get(k, 0)

        for stats in analyse_dep.values():
            stats["taux_clickers"] = round(stats["clickers"] / stats["sends"] * 100, 4) if stats["sends"] else 0
            stats["taux_openers"]  = round(stats["openers"]  / stats["sends"] * 100, 4) if stats["sends"] else 0
            stats["taux_unsubs"]   = round(stats["unsubs"]   / stats["sends"] * 100, 3) if stats["sends"] else 0
        analyse_dep = {
            dep: s for dep, s in analyse_dep.items()
            if s["taux_clickers"] or s["taux_openers"] or s["taux_unsubs"]
        }

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
                "analyse_dep": analyse_dep,
                "recommendation_segments": self._safe_analyse_dimensions(global_dimensions),
            },
            group_label: sorted(result, key=lambda x: (x["clickers"], x["ecpm"]), reverse=True),
        }
    def global_advertiser(self, adv_id, tag_id=None, date_schedule=None, 
                      date_start=None, date_end=None,
                      include_o_age=True, include_o_gender=True, include_o_isp=True,min_sends=100):
        tag_filtre = ''
        if tag_id:
            tag_filtre = f'AND t.tag_id=%(tag_id)s'
        if date_schedule:
            date_filter = f"AND date_schedule_max='{date_schedule}'"
        elif date_start and date_end:
            date_filter = f"AND date_schedule_max BETWEEN '{date_start}' AND '{date_end}'"
        else:
            date_filter = ''

        exclude_filters = []
        if not include_o_age:
            exclude_filters.append("t.age_range != 'O_age'")
        if not include_o_gender:
            exclude_filters.append("t.gender != 'O_gender'")
        if not include_o_isp:
            exclude_filters.append("t.main_isp != 'O_isp'")
        
        other_filter = ('AND ' + ' AND '.join(exclude_filters)) if exclude_filters else ''

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
        WHERE t.adv_id = %(adv_id)s {tag_filtre} {date_filter} {other_filter}
    )
    SELECT t.adv_id, t.database_id,
        groupUniqArray(t.id_routers) AS id_routers,
        any(t.tag_id) AS tag_id, max(t.clicks_val) AS clicks_val,
        max(t.leads_val) AS leads_val, max(t.cpm_val) AS volume_val,
        t.brand, any(t.model) AS models, any(t.client_id) AS client_id, any(t.optimized) AS optimized,
        any(t.subject) AS subject, groupUniqArray(t.date_schedule) AS date_schedule, t.gender, t.age_range, t.main_isp,
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
    GROUP BY t.adv_id, t.database_id, t.brand, t.gender, t.age_range, t.main_isp, t.id_focus, t.dep
    HAVING sum(t.sends)>={min_sends}
"""
        result = self._run_global(query, {"adv_id": adv_id, "tag_id": tag_id}, 
                                pivot_key="database_id", group_label="bases")
        result["globales"]["analyse_dep"] = self._get_analyse_dep(
                {"adv_id": adv_id}, "adv_id", date_filter, tag_id=tag_id,
                include_o_age=include_o_age,
                include_o_gender=include_o_gender,
                include_o_isp=include_o_isp
            )
        return result

    def global_base(self, db_id, date_schedule=None, date_start=None, date_end=None,include_o_age=True,include_o_gender=True,include_o_isp=True, min_sends=100):
        if date_schedule:
            date_filter = f"AND date_schedule_max = '{date_schedule}'"
        elif date_start and date_end:
            date_filter = f"AND date_schedule_max BETWEEN '{date_start}' AND '{date_end}'"
        else:
            date_filter = ''
        exclude_filters = []
        if not include_o_age:
            exclude_filters.append("t.age_range != 'O_age'")
        if not include_o_gender:
            exclude_filters.append("t.gender != 'O_gender'")
        if not include_o_isp:
            exclude_filters.append("t.main_isp != 'O_isp'")
        
        other_filter = ('AND ' + ' AND '.join(exclude_filters)) if exclude_filters else ''
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
                {date_filter} {other_filter}
            )
            SELECT t.adv_id, t.database_id,
                groupUniqArray(t.id_routers) AS id_routers,
                any(t.tag_id) AS tag_id, any(t.model) AS models, t.brand, any(t.client_id) AS client_id,
                any(t.optimized) AS optimized, any(t.subject) AS subject,  groupUniqArray(t.date_schedule) AS date_schedule, t.gender,
                t.age_range, t.main_isp, t.id_focus, t.dep AS departement,
                sum(t.sends) AS sends, 
                sum(t.clicks) AS clicks,
                countIf(t.clicks > 0) AS clickers,
                sum(t.opens) AS opens,
                countIf(t.opens > 0)  AS openers,
                countIf(t.unsubs > 0) AS unsubs,
                any(f.ca) AS ca,
                MAX(t.clicks_val) AS clicks_val, MAX(t.leads_val) AS leads_val,
                MAX(t.cpm_val) AS volume_val,
                any(t.advertiser_name) AS advertiser_name,
                groupUniqArray(t.segmentId) AS segmentId,
                groupUniqArrayIf(t.ListId,   t.is_zdwh) AS ListId,
                groupUniqArrayIf(t.ListName, t.is_zdwh) AS ListName
            FROM base t
            LEFT JOIN ca_by_focus f ON f.id_focus = t.id_focus
            GROUP BY t.adv_id, t.database_id,t.brand,t.gender, t.age_range, t.main_isp, t.id_focus, t.dep 
            HAVING sum(t.sends)>={min_sends}
        """
        result = self._run_global(query, {"db_id": db_id}, pivot_key="adv_id", group_label="advertisers")
        result["globales"]["analyse_dep"] = self._get_analyse_dep({"db_id": db_id}, "database_id", date_filter)
        return result
    
    def all_advertisers(self, date_schedule=None, date_start=None, date_end=None, country=None):
        conditions = []

        if date_schedule:
            conditions.append(f"date_schedule_max = '{date_schedule}'")

        if date_start and date_end:
            conditions.append(
                f"date_schedule_max BETWEEN '{date_start}' AND '{date_end}'"
            )

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        if country:
            if isinstance(country, str):
                country = [country]
            country_values = ",".join(f"'{c}'" for c in country)
            country_filter = f"""
                AND s.adv_id IN (
                    SELECT r.adv_id
                    FROM {self.table} r
                    JOIN country c ON r.country = c.id
                    WHERE c.country_code IN ({country_values})
                )
            """
        else:
            country_filter = f"""
                AND s.adv_id IN (
                    SELECT r.adv_id
                    FROM {self.table} r
                    JOIN country c ON r.country = c.id
                    WHERE c.country_code = 'FR'
                )
            """

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
                FROM {self.table} r
                {where_clause}
            ),
            focus_agg AS (
                SELECT
                    adv_id,
                    tag_id,
                    id_focus,
                    sum(sends)          AS sends,
                    countIf(opens > 0)  AS openers,
                    countIf(clicks > 0) AS clickers,
                    countIf(unsubs > 0) AS unsubs,
                    max(ca)             AS ca
                FROM base
                GROUP BY adv_id, tag_id, id_focus
                
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
            WHERE 1=1
            {country_filter}
            HAVING s.sends>0
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
    
    def all_bases(self, tags=None, date_schedule=None, date_start=None, date_end=None, country=None):
        joins = []
        conditions = []
        if tags:
            if isinstance(tags, str):
                tags = [tags]
            joins.append("JOIN tags t ON r.tag_id = t.id")
            tags_value = ",".join(f"'{t}'" for t in tags)
            conditions.append(f"t.tag IN ({tags_value})")

        if date_schedule:
            conditions.append(f"r.date_schedule_max = '{date_schedule}'")

        if date_start and date_end:
            conditions.append(
                f"date_schedule_max BETWEEN '{date_start}' AND '{date_end}'"
            )

        join_clause = " ".join(joins)
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        if country:
            if isinstance(country, str):
                country = [country]
            country_values = ",".join(f"'{c}'" for c in country)
            country_filter = f"""
                AND s.database_id IN (
                    SELECT r.database_id
                    FROM {self.table} r
                    JOIN country c ON r.country = c.id
                    WHERE c.dwh_name IN ({country_values})
                )
            """
        else:
            country_filter = f"""
                AND s.database_id IN (
                    SELECT r.database_id
                    FROM {self.table} r
                    JOIN country c ON r.country = c.id
                    WHERE c.dwh_name = 'FR'
                )
            """

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
                    r.unsubs,
                    r.ca
                FROM {self.table} r
                {join_clause}
                {where_clause}
            ),
            stats AS (
                SELECT
                    database_id,
                    sum(sends) AS sends,
                    sum(clickers) AS clickers,
                    sum(openers) AS openers,
                    countIf(unsubs > 0) AS unsubs
                FROM filtered
                GROUP BY database_id
                HAVING sends > 0
            ),

            ca_by_focus AS (
                SELECT
                    database_id,
                    id_focus,
                    max(ca) AS ca
                FROM filtered
                GROUP BY
                    database_id,
                    id_focus
            ),

            ca_unique AS (
                SELECT
                    database_id,
                    sum(ca) AS ca_global
                FROM ca_by_focus
                GROUP BY database_id
            )

            SELECT
                s.database_id                                                    AS database_id,
                d.basename                                                       AS basename,
                s.sends                                                          AS sends,
                s.clickers                                                       AS clickers,
                s.openers                                                        AS openers,
                s.unsubs                                                         AS unsubs,
                coalesce(cu.ca_global, 0)                                        AS ca_global,
                round(s.openers / nullIf(s.sends, 0) * 100, 3)                  AS taux_openers,
                round(s.clickers / nullIf(s.sends, 0) * 100, 3)                 AS taux_clickers,
                round(s.clickers / nullIf(s.openers, 0) * 100, 3)               AS taux_cto,
                round(s.unsubs / nullIf(s.sends, 0) * 100, 3)                   AS taux_unsubs,
                round(coalesce(cu.ca_global, 0) / nullIf(s.sends, 0) * 1000, 3) AS ecpm
            FROM stats s
            LEFT JOIN ca_unique cu
                ON cu.database_id = s.database_id
            JOIN databases d
                ON d.id = s.database_id
            WHERE 1=1
            {country_filter}
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
    def filter_by_tags(self, tag_id, database_id,date_start=None,date_end=None):
        date_filter = f"""
            AND date_schedule_max BETWEEN '{date_start}' AND '{date_end}'"""
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

    def get_country(self,country_id=None):
        query = f""" SELECT id, name,country_code FROM country """
        params={}
        if country_id is not None:
            query += f" WHERE id = {int(country_id)}"
        
        rows = self._execute_query(query, params)
        
        return [
                {"country_id": r["id"], "name": r["name"], "code":r["country_code"]}
                    for r in rows
                ]

    def top_10_bases(self, tag_id=None, date_start: str = None, date_end: str = None, min_sends: int = 1000):

        # --- Conditions WHERE sur la table principale ---
        where_conditions = []

        if tag_id:
            if isinstance(tag_id, list):
                tag_values = ", ".join(str(t) for t in tag_id)
                where_conditions.append(f"t.tag_id IN ({tag_values})")
            else:
                where_conditions.append(f"t.tag_id = {tag_id}")

        if date_start:
            where_conditions.append(f"t.date_schedule_max >= toDate('{date_start}')")
        if date_end:
            where_conditions.append(f"t.date_schedule_max <= toDate('{date_end}')")

        where_conditions.append("t.tag_id != 0")
        where_conditions.append("t.database_id != 0")

        where_clause = "WHERE " + " AND ".join(where_conditions)

        query = f"""
        WITH stats AS (
            SELECT
                t.tag_id,
                t.database_id,
                any(d.basename)  AS base_name,
                sum(t.sends)     AS sends,
                sum(t.clickers)  AS clickers
            FROM {self.table} t
            LEFT JOIN databases d ON d.id = t.database_id
            {where_clause}
            GROUP BY t.tag_id, t.database_id
            HAVING sends >= {min_sends}
        )
        SELECT
            tag_id,
            database_id,
            base_name,
            sends,
            clickers,
            round(clickers * 100.0 / nullIf(sends, 0), 2)                      AS click_rate,
            round((clickers * 100.0 / nullIf(sends, 0)) * log10(sends + 1), 2) AS weighted_score
        FROM stats
        ORDER BY weighted_score DESC, sends DESC
        LIMIT 10
        """

        result = self._execute_query(query)
        return [
            {
                "tag_id":        r["tag_id"],
                "database_id":   r["database_id"],
                "base_name":     r["base_name"],
                "sends":         r["sends"],
                "clickers":      r["clickers"],
                "taux_clickers": r["click_rate"],
                "score":         r["weighted_score"],
            }
            for r in result
        ]
    
    def top_advertisers_by_tag(self, tag_id=None, date_start=None, date_end=None, sort_by="ecpm"):
        ALLOWED_SORT_FIELDS = {
            "weighted_score": "weighted_score DESC, click_rate DESC",
            "clicks":     "click_rate DESC, weighted_score DESC",
            "ca":             "total_ca DESC, click_rate DESC",
            "ecpm":           "ecpm DESC, click_rate DESC",
        }

        if sort_by not in ALLOWED_SORT_FIELDS:
            raise ValueError(
                f"sort_by invalide : '{sort_by}'. "
                f"Valeurs acceptées : {list(ALLOWED_SORT_FIELDS.keys())}"
            )

        order_clause = ALLOWED_SORT_FIELDS[sort_by]
        where_tag = f"AND tag_id = {tag_id}" if tag_id else ""

        query = f"""
        WITH focus_agg AS (
            SELECT
                tag_id,
                adv_id,
                id_focus,
                toMonth(date_schedule_max)                                     AS month,
                sum(sends)                                                     AS sends,
                countIf(opens > 0)                                             AS openers,
                countIf(clickers > 0)                                          AS clickers,
                countIf(unsubs > 0)                                            AS unsubs,
                max(ca)                                                        AS ca,
                groupUniqArray(date_schedule_max)                              AS send_days,
                groupUniqArray(toDayOfWeek(date_schedule_max))                 AS send_weekdays,
                groupUniqArray(
                    intDiv(toDayOfMonth(date_schedule_max) - 1, 7) + 1
                )                                                              AS send_weeks
            FROM {self.table}
            WHERE date_schedule_max BETWEEN '{date_start}' AND '{date_end}'
                AND tag_id != 0
                {where_tag}
            GROUP BY tag_id, adv_id, id_focus, month
        ),

        -- Stats globales par (tag, month) — directo depuis focus_agg
        tag_stats AS (
            SELECT
                tag_id,
                month,
                sum(f.sends)    AS sends,
                sum(f.openers)  AS openers,
                sum(f.clickers) AS clickers,
                sum(f.unsubs)   AS unsubs,
                sum(f.ca)       AS total_ca,
                round(sum(f.openers)  / nullIf(sum(f.sends), 0) * 100,  2) AS open_rate,
                round(sum(f.clickers) / nullIf(sum(f.sends), 0) * 100,  2) AS click_rate,
                round(sum(f.clickers) / nullIf(sum(f.openers), 0) * 100, 2) AS taux_cto,
                round(sum(f.unsubs)   / nullIf(sum(f.sends), 0) * 100,  2) AS taux_unsubs,
                round(sum(f.ca)       / nullIf(sum(f.sends), 0) * 1000, 2) AS ecpm
            FROM focus_agg f
            GROUP BY tag_id, month
        ),

        -- Stats par (tag, adv, month) — directo depuis focus_agg
        adv_stats AS (
            SELECT
                tag_id,
                adv_id,
                month,
                sum(f.sends)    AS sends,
                sum(f.openers)  AS openers,
                sum(f.clickers) AS clickers,
                sum(f.unsubs)   AS unsubs,
                sum(f.ca)       AS total_ca,
                arraySort(arrayDistinct(arrayFlatten(groupArray(send_days))))     AS send_days,
                arraySort(arrayDistinct(arrayFlatten(groupArray(send_weekdays)))) AS send_weekdays,
                arraySort(arrayDistinct(arrayFlatten(groupArray(send_weeks))))    AS send_weeks,
                round(sum(f.openers)  / nullIf(sum(f.sends), 0) * 100,  2) AS open_rate,
                round(sum(f.clickers) / nullIf(sum(f.sends), 0) * 100,  2) AS click_rate,
                round(sum(f.clickers) / nullIf(sum(f.openers), 0) * 100, 2) AS taux_cto,
                round(sum(f.unsubs)   / nullIf(sum(f.sends), 0) * 100,  2) AS taux_unsubs,
                round(sum(f.ca)       / nullIf(sum(f.sends), 0) * 1000, 2) AS ecpm,
                round(
                    (sum(f.ca) / nullIf(sum(f.sends), 0) * 1000) * log10(sum(f.clickers) + 1),
                    2
                ) AS weighted_score
            FROM focus_agg f
            GROUP BY tag_id, adv_id, month
        ),

        -- Ranking des advertisers par (tag, month)
        ranked_advs AS (
            SELECT
                a.*,
                adv.name AS adv_name,
                row_number() OVER (
                    PARTITION BY a.tag_id, a.month
                    ORDER BY {order_clause}
                ) AS rank
            FROM adv_stats a
            LEFT JOIN advertiser adv ON adv.id = a.adv_id
        )

        -- Jointure tag_stats + top5 groupé
        SELECT
            ts.tag_id AS tag_id,
            tg.tag     AS tag_name,
            ts.month AS month,
            ts.ecpm    AS global_ecpm,
            groupArrayIf(
                (
                    ra.rank,
                    ra.adv_id,
                    ra.adv_name,
                    ra.click_rate,
                    ra.ecpm,
                    ra.total_ca,
                    ra.weighted_score,
                    ra.send_days,
                    ra.send_weekdays,
                    ra.send_weeks
                ),
                ra.rank <= 5
            ) AS top5_raw
        FROM tag_stats ts
        LEFT JOIN tags tg ON tg.id = ts.tag_id
        LEFT JOIN ranked_advs ra ON ra.tag_id = ts.tag_id AND ra.month = ts.month
        GROUP BY ts.tag_id, tg.tag, ts.month, ts.ecpm
        ORDER BY ts.tag_id, ts.month
        """

        rows = self._execute_query(query)

        weekday_map = {
            1: "Lundi", 2: "Mardi", 3: "Mercredi", 4: "Jeudi",
            5: "Vendredi", 6: "Samedi", 7: "Dimanche",
        }

        output = {}

        for r in rows:
            t_id        = r["tag_id"]
            month       = r["month"]
            global_ecpm = r["global_ecpm"] or 0

            if t_id not in output:
                output[t_id] = {
                    "tag_id":   t_id,
                    "tag_name": r["tag_name"],
                    "months":   {},
                }

            # Trier le tableau top5 par rank et construire les dicts
            top5_sorted = sorted(r["top5_raw"] or [], key=lambda x: x[0])
            top5 = []
            for adv in top5_sorted:
                (
                    rank, adv_id, adv_name, click_rate, ecpm, total_ca,
                    weighted_score, send_days, send_weekdays, send_weeks
                ) = adv
                top5.append({
                    "rank":          rank,
                    "adv_id":        adv_id,
                    "adv_name":      adv_name,
                    "clickers_rate": click_rate or 0.0,
                    "ecpm":          ecpm or 0.0,
                    "ca":            total_ca or 0.0,
                    "jours_envoi":   send_days or [],
                    "jours_semaine": [
                        weekday_map.get(day, str(day))
                        for day in (send_weekdays or [])
                    ],
                    "semaines": [f"S{w}" for w in (send_weeks or [])],
                })

            output[t_id]["months"][month] = {
                "global": {
                    "ecpm":   global_ecpm,
                    "result": self.analyze.anlyse_top_advertiser(global_ecpm),
                },
                "top5":    top5,
                "sort_by": sort_by,
            }

        return list(output.values())