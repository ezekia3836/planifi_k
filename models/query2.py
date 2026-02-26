from collections import defaultdict
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
import base64
import math
class Query2:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.analyze = analyse()
        self.table = "dev_reporting_agg"

    def _execute_query(self, query):
        result = self.clk.query(query)
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
                main_isp,
                date_shedule,
                SUM(sends) AS sends,
                SUM(clicks) AS clicks,
                SUM(clickers) AS clickers,
                SUM(opens) AS opens,
                SUM(openers) AS openers,
                SUM(unsubs) AS unsubs,
                MAX(ca) AS ca,
                groupUniqArray(segmentId) AS segmentId
            FROM {self.table}
            WHERE adv_id = {adv_id}
            GROUP BY
                database_id, id_routers, tag_id,
                age_range, gender, main_isp,
                brand, optimized, date_shedule
        """

        rows = self._execute_query(query)
        if not rows:
            return {"advertiser_id": str(adv_id), "globales": {}, "bases": []}

        bases = {}

        total = dict(sends=0, clicks=0, clickers=0, opens=0, openers=0, unsubs=0, ca=0)


        def compute_rates(sends,openers, clickers, unsubs):
            taux_clickers = round(clickers / sends * 100, 3) if sends else 0
            taux_openers = round(openers / sends * 100, 3) if sends else 0
            taux_unsubs = round(unsubs / sends * 100, 3) if sends else 0
            taux_cto = round(clickers / openers * 100, 3) if openers else 0
            return taux_clickers, taux_openers, taux_unsubs, taux_cto

        for r in rows:
            base_key = (r["database_id"], r["id_routers"], r["tag_id"])

            base = bases.setdefault(base_key, {
                "database_id": r["database_id"],
                "id_routers": r["id_routers"],
                "tag_id": r["tag_id"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "ca": 0,
                "date_shedule": [],
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
            base["ca"] += ca

            if r.get("segmentId"):
                base["SegmentIds"].update(r["segmentId"])

            if r.get("date_shedule"):
                if isinstance(r["date_shedule"], list):
                    base["date_shedule"].extend(r["date_shedule"])
                else:
                    base["date_shedule"].append(r["date_shedule"])

            total["sends"] += sends
            total["clicks"] += clicks
            total["clickers"] += clickers
            total["opens"] += opens
            total["openers"] += openers
            total["unsubs"] += unsubs
            total["ca"] += ca

            def push_dim(dim, value):
                if not value:
                    return
                seg = base["dimensions"][dim].setdefault(value, {
                    "sends": 0, "clicks": 0, "clickers": 0,
                    "opens": 0, "openers": 0, "unsubs": 0,"taux_clickers": 0, "taux_openers": 0, "taux_unsubs": 0,"taux_cto": 0,"analyses": {}
                })
                seg["sends"] += sends
                seg["clicks"] += clicks
                seg["clickers"] += clickers
                seg["opens"] += opens
                seg["openers"] += openers
                seg["unsubs"] += unsubs

                seg["taux_clickers"] = round(seg["clickers"] / seg["sends"] * 100, 3) if seg["sends"] else 0
                seg["taux_openers"] = round(seg["openers"] / seg["sends"] * 100, 3) if seg["sends"] else 0
                seg["taux_unsubs"] = round(seg["unsubs"] / seg["sends"] * 100, 3) if seg["sends"] else 0
                seg["taux_cto"] = round(seg["clickers"] / seg["openers"] * 100, 3) if seg["openers"] else 0

                seg["analyses"] = {
                    "taux_clickers": self.analyze.analyze_click_rate(seg['taux_clickers']),
                    "taux_cto": self.analyze.analyze_cto_rate(seg["taux_cto"], seg["openers"]),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(seg['taux_unsubs'])
                }

            push_dim("age_range", r["age_range"])
            push_dim("gender", r["gender"])
            push_dim("isp", r["main_isp"])

            brand_name = base64.b64decode(r["brand"]).decode().strip()
            optimized = r.get("optimized") or ""

            existing = next((b for b in base["brands"] if b["name"] == brand_name), None)

            if not existing:
                existing = {
                    "name": brand_name,
                    "creativities": optimized,
                    "sends": 0, "clicks": 0, "clickers": 0,
                    "opens": 0, "openers": 0, "unsubs": 0,
                    "ca": 0,
                    "analyses": {}
                }
                base["brands"].append(existing)

            existing["sends"] += sends
            existing["clicks"] += clicks
            existing["clickers"] += clickers
            existing["opens"] += opens
            existing["openers"] += openers
            existing["unsubs"] += unsubs
            existing["ca"] += ca
            existing["creativities"] = optimized

        result_bases = []

        for base in bases.values():
            tc, to, tu, cto = compute_rates(
                base["sends"], base["openers"],
                base["clickers"], base["unsubs"]
            )

            ecpm = round(base["ca"] / base["sends"] * 1000, 3) if base["sends"] else 0
            classification=self.analyze.classify_advertiser(ecpm,tc)
            for b in base["brands"]:
                tc_b, to_b, tu_b, cto_b = compute_rates(
                    b["sends"],b["openers"],
                    b["clickers"], b["unsubs"]
                )

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
                "date_shedule": sorted(set(base["date_shedule"])),
                "SegmentIds": sorted(base["SegmentIds"]),
                "ecpm": ecpm,
                "ca": round(base["ca"], 2),
                "classification":classification,
                "analyses": {
                    "taux_clickers": self.analyze.analyze_click_rate(tc),
                    "taux_cto": self.analyze.analyze_cto_rate(cto, base["openers"]),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(tu)
                },
                "dimensions": base["dimensions"]
            })

        tc_g, to_g, tu_g, cto_g = compute_rates(
            total["sends"], total["openers"],
            total["clickers"], total["unsubs"]
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
                }
            },
            "bases": sorted(result_bases, key=lambda x: (x["clickers"], x["ecpm"]), reverse=True)
        }
    
    def global_base(self, db_id):

        def safe(value, default):
            return value if value not in ("", None) else default
        query = f"""
            SELECT
                adv_id,
                id_routers,
                age_range,
                gender,
                main_isp,
                brand,
                optimized,
                sum(sends)     AS sends,
                sum(clicks)    AS clicks,
                sum(clickers)  AS clickers,
                sum(opens)     AS opens,
                sum(openers)   AS openers,
                sum(unsubs)  AS unsubs,
                max(ca)        AS ca,
                tag_id
            FROM {self.table}
            WHERE database_id = {db_id}
            GROUP BY
                adv_id,
                id_routers,
                age_range,
                gender,
                main_isp,
                brand,
                optimized,
                tag_id
        """
        rows = self._execute_query(query)
        result = {
            "database_id": str(db_id),
            "globales": {
                "sends": 0,
                "clicks": 0,
                "clickers":0,
                "opens": 0,
                "openers":0,
                "unsubs": 0,
                "ca": 0.0
            },
            "advertisers": []
        }
        advertisers = {}
        for r in rows:
            adv_id = str(r["adv_id"])
            sends = r["sends"]
            clicks = r["clicks"]
            clickers = r["clickers"]
            opens = r["opens"]
            openers = r["openers"]
            unsubs = r["unsubs"]
            ca = r["ca"]
            brand_name = base64.b64decode(r["brand"]).decode("utf-8").strip()
            optimized_url = r.get("optimized") or "O_opt"
            g = result["globales"]
            g["sends"] += sends
            g["clicks"] += clicks
            g["clickers"] += clickers
            g["opens"] += opens
            g["openers"] += openers
            g["unsubs"] += unsubs
            g["ca"] += ca
            adv = advertisers.setdefault(adv_id, {
                "advertiser_id": adv_id,
                "routers_seen": set(),
                "id_routers":r["id_routers"],
                "tag": r['tag_id'],
                "brands": [],
                "sends": 0,
                "clicks": 0,
                "clickers": 0,
                "opens": 0,
                "openers": 0,
                "unsubs": 0,
                "ca": 0.0,
                "dimensions": {
                    "age_range": {},
                    "gender": {},
                    "isp": {}
                }
            })
            adv["ca"] += ca
            adv["sends"] += sends
            adv["clicks"] += clicks
            adv["clickers"] += clickers
            adv["opens"] += opens
            adv["openers"] += openers
            adv["unsubs"] += unsubs

            dims = {
                "age_range": safe(r["age_range"], "O_age"),
                "gender": safe(r["gender"], "O_gender"),
                "isp": safe(r["main_isp"], "O_isp"),
            }
            for dim, val in dims.items():
                seg = adv["dimensions"][dim].setdefault(val, {
                    "sends": 0,
                    "clicks":0,
                    "clickers": 0,
                    "opens":0,
                    "openers": 0,
                    "unsubs": 0
                })
                seg["sends"] += sends
                seg["clicks"] += clicks
                seg["clickers"] += clickers
                seg["opens"] += opens
                seg["openers"] += openers
                seg["unsubs"] += unsubs
                seg["taux_clickers"] = round(seg["clickers"] / seg["sends"] *100 if seg["sends"] else 0.0,3)
                seg["taux_cto"] = round(seg["clickers"] / seg["openers"] *100 if seg["openers"] else 0.0,3)
                seg["taux_unsubs"] = round(seg["unsubs"] / seg["sends"] *100 if seg["sends"] else 0.0,3)
                analyses={
                    "taux_clickers":self.analyze.analyze_click_rate(seg['taux_clickers']),
                    "taux_cto":self.analyze.analyze_cto_rate(seg["taux_cto"],seg["openers"]),
                    "taux_unsubs":self.analyze.analyze_unsub_rate(seg["taux_unsubs"])
                }
                seg["analyses"]=analyses
            existing_brand = next((b for b in adv["brands"] if b["name"] == brand_name), None)
            def compute_brand_metrics(b):
                sends_b = b["sends"]
                openers_b = b["openers"]

                b["taux_clickers"] = round(b["clickers"] / sends_b * 100 if sends_b else 0.0, 3)
                b["taux_openers"] = round(b["openers"] / sends_b * 100 if sends_b else 0.0, 3)
                b["taux_unsubs"] = round(b["unsubs"] / sends_b * 100 if sends_b else 0.0, 3)
                b["taux_cto"] = round(b["clickers"] / openers_b * 100 if openers_b else 0.0, 3)

                b["analyses"] = {
                    "taux_clickers": self.analyze.analyze_click_rate(b["taux_clickers"]),
                    "taux_cto": self.analyze.analyze_cto_rate(b["taux_cto"], openers_b),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(b["taux_unsubs"])
                }

            if not existing_brand:
                new_brand = {
                    "name": brand_name,
                    "creativities": optimized_url,
                    "sends": sends,
                    "clicks": clicks,
                    "clickers": clickers,
                    "opens": opens,
                    "openers": openers,
                    "unsubs": unsubs,
                    "ca": ca
                }
                compute_brand_metrics(new_brand)
                adv["brands"].append(new_brand)

            else:
                existing_brand["sends"] += sends
                existing_brand["clicks"] += clicks
                existing_brand["clickers"] += clickers
                existing_brand["opens"] += opens
                existing_brand["openers"] += openers
                existing_brand["unsubs"] += unsubs
                existing_brand["ca"] += ca
                existing_brand["creativities"] = optimized_url
                compute_brand_metrics(existing_brand)
        for adv in advertisers.values():
            sends = adv["sends"]
            openers = adv["openers"]
            adv["ecpm"] = round((adv["ca"] / sends) * 1000, 3) if sends else 0.0
            adv["taux_clickers"] = round(adv["clickers"] / sends * 100 if sends else 0.0, 3)
            adv["taux_openers"] = round(adv["openers"] / sends * 100 if sends else 0.0, 3)
            adv["taux_unsubs"] = round(adv["unsubs"] / sends * 100 if sends else 0.0, 3)
            adv["taux_cto"] = round(adv["clickers"] / openers * 100 if openers else 0.0, 3)
            adv["classification"]=self.analyze.classify_advertiser(adv['ecpm'],adv['taux_clickers'])
            analyses={
                    "taux_clickers":self.analyze.analyze_click_rate(adv['taux_clickers']),
                    "taux_cto":self.analyze.analyze_cto_rate(adv["taux_cto"],adv["openers"]),
                    "taux_unsubs":self.analyze.analyze_unsub_rate(adv["taux_unsubs"])
                }
            adv["analyses"] = analyses
            adv.pop("routers_seen")
            result["advertisers"].append(adv)

        result["advertisers"].sort(key=lambda x: x["ecpm"], reverse=True)
        g = result["globales"]
        sends = g["sends"]
        openers = g["openers"]
        g["ecpm"] = round((g["ca"] / sends) * 1000 if sends else 0, 3)
        g["taux_clickers"] = round(g["clickers"] / sends * 100 if sends else 0, 3)
        g["taux_openers"] = round(g["openers"] / sends * 100 if sends else 0, 3)
        g["taux_unsubs"] = round(g["unsubs"] / sends * 100 if sends else 0, 3)
        g["taux_cto"] = round(g["clickers"] / openers * 100 if openers else 0, 3)
        g["analyses"] = {
            "taux_clickers": self.analyze.analyze_click_rate(g["taux_clickers"]),
            "taux_cto": self.analyze.analyze_cto_rate(g["taux_cto"], openers),
            "taux_unsubs": self.analyze.analyze_unsub_rate(g["taux_unsubs"])
        }
        return result
    
    def lis_advertisers(self):
        try:
            query=f""" SELECT DISTINCT dev.adv_id,a.name FROM {self.table} dev JOIN advertiser a ON dev.adv_id=a.id"""
            rows=self._execute_query(query)
            return {
                "total":len(rows),
                "advertisers":rows
            }
        except Exception as e:
            print("Erreur list_advertiser",e)
    def list_bases(self):
        try:
            query=f""" SELECT DISTINCT dev.database_id,d.basename FROM {self.table} dev JOIN databases d ON dev.database_id= d.id"""
            rows=self._execute_query(query)
            return{
                "total":len(rows),
                "bases":rows
            }
        except Exception as e:
            print("Erreur list_bases",e)
