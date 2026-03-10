from collections import defaultdict
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
import base64
import math
import re
class Query2:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.analyze = analyse()
        self.table = "dev_reporting_agg"

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
                brand, optimized, date_schedule
        """

        rows = self._execute_query(query, {"adv_id": adv_id})

        if not rows:
            return {"advertiser_id": str(adv_id), "globales": {}, "bases": []}
        bases = {}
        decoded_brands = {}

        total = dict(sends=0, clicks=0, clickers=0, opens=0, openers=0, unsubs=0, ca=0)

        def decode_brand(b):
            if not b:
                return ""
            if b not in decoded_brands:
                decoded_brands[b] = base64.b64decode(b).decode().strip()
            return decoded_brands[b]

        for r in rows:
            key = (r["database_id"], r["id_routers"], r["tag_id"])

            base = bases.setdefault(key, {
                "database_id": r["database_id"],
                "id_routers": r["id_routers"],
                "tag_id": r["tag_id"],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "ca": 0,
                "date_schedule": set(),
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

            total["sends"] += sends
            total["clicks"] += clicks
            total["clickers"] += clickers
            total["opens"] += opens
            total["openers"] += openers
            total["unsubs"] += unsubs
            total["ca"] += ca

            if r.get("segmentId"):
                base["SegmentIds"].update(r["segmentId"])

            if r.get("date_schedule"):
                if isinstance(r["date_schedule"], list):
                    base["date_schedule"].update(r["date_schedule"])
                else:
                    base["date_schedule"].add(r["date_schedule"])

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

            brand_name = decode_brand(r["brand"])
            optimized = r.get("optimized") or ""

            brand_list = base["brands"]
            brand = next((b for b in brand_list if b["name"] == brand_name), None)

            if not brand:
                brand = {
                    "name": brand_name,
                    "creativities": optimized,
                    "sends": 0, "clicks": 0, "clickers": 0,
                    "opens": 0, "openers": 0, "unsubs": 0,
                    "ca": 0
                }
                brand_list.append(brand)

            brand["sends"] += sends
            brand["clicks"] += clicks
            brand["clickers"] += clickers
            brand["opens"] += opens
            brand["openers"] += openers
            brand["unsubs"] += unsubs
            brand["ca"] += ca

        def compute_rates(sends, openers, clickers, unsubs):
            return (
                round(clickers / sends * 100, 3) if sends else 0,
                round(openers / sends * 100, 3) if sends else 0,
                round(unsubs / sends * 100, 3) if sends else 0,
                round(clickers / openers * 100, 3) if openers else 0
            )

        result_bases = []

        for base in bases.values():
            tc, to, tu, cto = compute_rates(
                base["sends"], base["openers"],
                base["clickers"], base["unsubs"]
            )

            ecpm = round(base["ca"] / base["sends"] * 1000, 3) if base["sends"] else 0
            classification = self.analyze.classify_advertiser(ecpm, tc)

            for b in base["brands"]:
                tc_b, to_b, tu_b, cto_b = compute_rates(
                    b["sends"], b["openers"], b["clickers"], b["unsubs"]
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

            for dim in base["dimensions"].values():
                for seg in dim.values():
                    tc_d, to_d, tu_d, cto_d = compute_rates(
                        seg["sends"], seg["openers"],
                        seg["clickers"], seg["unsubs"]
                    )
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
                "date_schedule": sorted(base["date_schedule"]),
                "SegmentIds": sorted(base["SegmentIds"]),
                "ecpm": ecpm,
                "ca": round(base["ca"], 2),
                "classification": classification,
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

        query = f"""
            SELECT
                adv_id, id_routers, age_range,
                gender, main_isp, brand, optimized,
                sum(sends) AS sends,
                sum(clicks) AS clicks,
                sum(clickers) AS clickers,
                sum(opens) AS opens,
                sum(openers) AS openers,
                sum(unsubs) AS unsubs,
                max(ca) AS ca,
                tag_id
            FROM {self.table}
            WHERE database_id = %(db_id)s
            GROUP BY
                adv_id, id_routers, age_range, gender,
                main_isp, brand, optimized, tag_id
        """

        rows = self._execute_query(query, {"db_id": db_id})

        result = {
            "database_id": str(db_id),
            "globales": {"sends": 0, "clicks": 0, "clickers": 0, "opens": 0, "openers": 0, "unsubs": 0, "ca": 0.0},
            "advertisers": []
        }
        advertisers = {}
        decoded_brands = {}

        def decode_brand(b):
            if not b:
                return ""
            if b not in decoded_brands:
                decoded_brands[b] = base64.b64decode(b).decode("utf-8").strip()
            return decoded_brands[b]

        def safe(v, default):
            return v if v not in ("", None) else default

        for r in rows:
            adv_id = str(r["adv_id"])
            sends = r["sends"] or 0
            clicks = r["clicks"] or 0
            clickers = r["clickers"] or 0
            opens = r["opens"] or 0
            openers = r["openers"] or 0
            unsubs = r["unsubs"] or 0
            ca = r["ca"] or 0

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
                "id_routers": r["id_routers"],
                "tag": r["tag_id"],
                "brands_map": {},  
                "brands": [],
                "sends": 0, "clicks": 0, "clickers": 0,
                "opens": 0, "openers": 0, "unsubs": 0,
                "ca": 0.0,
                "dimensions": {"age_range": {}, "gender": {}, "isp": {}}
            })

            adv["sends"] += sends
            adv["clicks"] += clicks
            adv["clickers"] += clickers
            adv["opens"] += opens
            adv["openers"] += openers
            adv["unsubs"] += unsubs
            adv["ca"] += ca

            dims = {
                "age_range": safe(r["age_range"], "O_age"),
                "gender": safe(r["gender"], "O_gender"),
                "isp": safe(r["main_isp"], "O_isp"),
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

            brand_name = decode_brand(r["brand"])
            optimized_url = r.get("optimized") or "O_opt"
            brand = adv["brands_map"].get(brand_name)
            if not brand:
                brand = {
                    "name": brand_name,
                    "creativities": optimized_url,
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

        def compute_rates(sends, openers, clickers, unsubs):
            return (
                round(clickers / sends * 100, 3) if sends else 0,
                round(openers / sends * 100, 3) if sends else 0,
                round(unsubs / sends * 100, 3) if sends else 0,
                round(clickers / openers * 100, 3) if openers else 0
            )

        for adv in advertisers.values():

            tc, to, tu, cto = compute_rates(
                adv["sends"], adv["openers"],
                adv["clickers"], adv["unsubs"]
            )

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
                    tc_d, to_d,tu_d, cto_d = compute_rates(
                        seg["sends"], seg["openers"],
                        seg["clickers"], seg["unsubs"]
                    )
                    seg.update({
                        "taux_clickers": tc_d,
                        "taux_cto": cto_d,
                        "taux_unsubs": tu_d,
                        "analyses": {
                            "taux_clickers": self.analyze.analyze_click_rate(tc_d),
                            "taux_cto": self.analyze.analyze_cto_rate(cto_d, seg["openers"]),
                            "taux_unsubs": self.analyze.analyze_unsub_rate(tu_d)
                        }
                    })
            for b in adv["brands"]:
                tc_b, to_b, tu_b, cto_b = compute_rates(
                    b["sends"], b["openers"], b["clickers"], b["unsubs"]
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
            adv.pop("brands_map")
            result["advertisers"].append(adv)
        result["advertisers"].sort(key=lambda x: x["ecpm"], reverse=True)
        g = result["globales"]
        tc, to, tu, cto = compute_rates(
            g["sends"], g["openers"],
            g["clickers"], g["unsubs"]
        )
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
        return result
    def all_advertisers(self,date_schedule=None,date_start=None,date_end=None,tags=None):
        query=""" SELECT r.adv_id,a.name,SUM(r.sends) AS sends,SUM(r.openers) AS openers,SUM(clickers) AS clickers,SUM(unsubs) AS unsubs,round(SUM(r.clickers)/SUM(r.openers)*100,3) as taux_cto,
           round(SUM(r.openers)/SUM(r.sends)*100,3) AS taux_openers, round(SUM(r.clickers)/SUM(r.sends)*100,3) AS taux_clickers,round(SUM(r.unsubs)/SUM(r.sends)*100,3) AS taux_unsubs
          FROM dev_reporting_agg r JOIN advertiser a ON r.adv_id=a.id """
        joins=[]
        conditions=[]
        if tags:
            if isinstance(tags, str):
                tags = [tags]
            joins.append("JOIN tags t ON r.tag_id = t.id")
            tags_value = ",".join([f"'{t}'" for t in tags])
            conditions.append(f"t.tag IN ({tags_value})")
        if date_schedule:
            conditions.append(f"has(r.date_schedule, '{date_schedule}')")
        if date_start and date_end:
            conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', r.date_schedule)")
        if joins:
            query+=" ".join(joins)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " GROUP BY r.adv_id,a.name"
        rows= self._execute_query(query)
        result=[]
        for row in rows:
            result.append({
                "advrtiser_id":row["adv_id"],
                "advertiser_name":row["name"],
                "globales":{
                    "sends":row["sends"],
                    "openers":row["openers"],
                    "clickers":row["clickers"],
                    "unsubs":row["unsubs"],
                    "taux_openers":row["taux_openers"],
                    "taux_clickers":row["taux_clickers"],
                    "taux_unsubs":row["taux_unsubs"],
                    "analyse":{
                            "taux_clickers":self.analyze.analyze_click_rate(row['taux_clickers']),
                            "taux_cto":self.analyze.analyze_cto_rate(row["taux_cto"],row["openers"]),
                            "taux_unsubs": self.analyze.analyze_unsub_rate(row["taux_unsubs"])
                        }
                }
            })
        return result
    
    def all_bases(self,country=None,tags=None,date_schedule=None,date_start=None,date_end=None):
        query=""" 
        SELECT r.database_id,d.basename,SUM(r.sends) AS sends,SUM(r.openers) AS openers,SUM(r.clickers) AS clickers,SUM(r.unsubs) AS unsubs, 
        round(SUM(r.openers)/SUM(r.sends)*100,3) AS taux_openers, round(SUM(r.clickers)/SUM(r.openers)*100,3) as taux_cto, round(SUM(r.clickers)/SUM(r.sends)*100,3) AS taux_clickers,round(SUM(r.unsubs)/SUM(r.sends)*100,3) AS taux_unsubs
        FROM dev_reporting_agg r JOIN databases d ON r.database_id=d.id """
        joins=[]
        conditions=[]
        if tags:
            if isinstance(tags, str):
                tags=[tags]
            joins.append("JOIN tags t ON r.tag_id=t.id")
            tags_value= ",".join((f"'{t}'" for t in tags))
            conditions.append(f"t.tag IN ({tags_value})")
        if country:
            if isinstance(country, str):
                country=[country]
            joins.append("JOIN country c ON r.country=c.id")
            country_values= ",".join((f"'{c}'" for c in country))
            conditions.append(f"c.dwh_name IN ({country_values})")
        if date_schedule:
            conditions.append(f"has(r.date_schedule, '{date_schedule}')")
        if date_start and date_end:
             conditions.append(
                f"arrayExists(x -> x BETWEEN '{date_start}' AND '{date_end}', r.date_schedule)")
        if joins:
            query+=" ".join(joins)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " GROUP BY r.database_id,d.basename"
        rows= self._execute_query(query)
        result=[]
        for row in rows:
            result.append({
                "database_id":row["database_id"],
                "database_name":row["basename"],
                "globales":{
                        "sends":row["sends"],
                        "openers":row["openers"],
                        "clickers":row["clickers"],
                        "usubs":row["unsubs"],
                        "taux_openers":row["taux_openers"],
                        "taux_clickers":row["taux_clickers"],
                        "taux_unsubs":row["taux_unsubs"],
                        "analyse":{
                            "taux_clickers":self.analyze.analyze_click_rate(row['taux_clickers']),
                            "taux_cto":self.analyze.analyze_cto_rate(row["taux_cto"],row["openers"]),
                            "taux_unsubs": self.analyze.analyze_unsub_rate(row["taux_unsubs"])
                        }
                }
            })
        return result
