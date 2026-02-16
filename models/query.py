from collections import defaultdict
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
from datetime import timedelta,datetime
from decimal import Decimal, ROUND_HALF_UP
import calendar
import math
import re
import base64
from typing import Optional

class Query:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.analyze = analyse()
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
                age_civilite_isp,
                SUM(sends) AS sends,
                SUM(clicks) AS clicks,
                SUM(clickers) AS clickers,
                SUM(opens) AS opens,
                SUM(openers) AS openers,
                SUM(removals) AS removals,
                SUM(complaints) AS complaints,
                SUM(bounces) AS bounces,
                MAX(ca) AS ca,
                groupUniqArray(segmentId) AS segmentId,
                subject,
                client_id,
                id_focus,
                ktk_id,
                basename
            FROM reporting
            WHERE adv_id = {adv_id}
            GROUP BY
                database_id,
                basename,
                id_routers,
                age_range,
                gender,
                main_isp,
                age_civilite_isp,
                brand,
                optimized,
                tag_id,
                date_shedule,
                subject,
                client_id,
                id_focus,
                ktk_id
        """
        rows = self._execute_query(query)
        if not rows:
            return {"advertiser_id": str(adv_id), "globales": {}, "bases": []}

        bases = {}
        total_sends_global = total_ca_global = total_clickers_global = 0
        total_clicks_global = total_openers_global = total_opens_global = 0
        total_unsubs_global = total_complaints_global = total_bounces_global = 0

        for r in rows:
            base_key = (r["database_id"], r["ktk_id"], r["id_routers"], r['tag_id'], r['client_id'], r['id_focus'],r['basename'])
            base = bases.setdefault(base_key, {
                "database_id": r["database_id"],
                "basename":r['basename'],
                "ktk_id":r["ktk_id"],
                "id_routers": r["id_routers"],
                "tag_id": r['tag_id'],
                "client_id": r['client_id'],
                "id_focus": r['id_focus'],
                "sends": 0,
                "clicks": 0,
                "clickers": 0,
                "opens": 0,
                "openers": 0,
                "unsubs": 0,
                "complaints": 0,
                "bounces": 0,
                "ca": 0.0,
                "date_shedule": [],
                "SegmentIds":set(),
                "subject": r.get("subject"),
                "dimensions": {
                    "age_range": {},
                    "gender": {},
                    "isp": {},
                    "age_civilite_isp": {}
                },
                "brands": []
            })
            if r.get("segmentId"):
                base["SegmentIds"].update(r['segmentId'])

            sends = r["sends"]
            clickers = r["clickers"]
            clicks = r["clicks"]
            opens = r["opens"]
            openers = r["openers"]
            removals = r["removals"]
            complaints = r["complaints"]
            bounces = r["bounces"]
            ca = r["ca"]

            base["sends"] += sends
            base["clickers"] += clickers
            base["clicks"] += clicks
            base["openers"] += openers
            base["opens"] += opens
            base["unsubs"] += removals
            base["complaints"] += complaints
            base["bounces"] += bounces
            base["ca"] += ca
            base["date_shedule"] = sorted(set(base["date_shedule"] + (r["date_shedule"] or [])))

            total_sends_global += sends
            total_ca_global += ca
            total_clicks_global += clicks
            total_clickers_global += clickers
            total_opens_global += opens
            total_openers_global += openers
            total_unsubs_global += removals
            total_complaints_global += complaints
            total_bounces_global += bounces
            def push(dim, value):
                if not value:
                    return
                seg = base["dimensions"][dim].setdefault(value, {
                    "sends": 0,
                    "clicks": 0,
                    "clickers": 0,
                    "opens": 0,
                    "openers": 0,
                    "unsubs": 0,
                    "complaints": 0,
                    "bounces": 0,
                    "analyses": {}
                })
                seg["sends"] += sends
                seg['clicks'] += clicks
                seg["clickers"] += clickers
                seg["opens"] += opens
                seg["openers"] += openers
                seg["unsubs"] += removals
                seg["complaints"] += complaints
                seg["bounces"] += bounces
                taux_clickers_seg = round(seg["clickers"] / seg["sends"] * 100 if seg["sends"] else 0, 3)
                taux_cto_seg = round(seg["clickers"] / seg["openers"] * 100 if seg["openers"] else 0, 3)
                taux_unsubs_seg = round(seg["unsubs"] / seg["sends"] * 100 if seg["sends"] else 0, 3)
                seg["analyses"] = {
                    "taux_clickers": self.analyze.analyze_click_rate(taux_clickers_seg),
                    "taux_cto": self.analyze.analyze_cto_rate(taux_cto_seg, seg["openers"]),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(taux_unsubs_seg)
                }

            push("age_range", r["age_range"])
            push("gender", r["gender"])
            push("isp", r["main_isp"])
            push("age_civilite_isp", r["age_civilite_isp"])

            
            brand_name = base64.b64decode(r['brand']).decode("utf-8").strip()
            existing_brand = next((b for b in base["brands"] if b["name"] == brand_name), None)
            optimized= r.get('optimized') or "optimized vide"
            if not existing_brand:
                base["brands"].append({
                    "name": brand_name,
                    "creativities":optimized,
                    "sends": sends,
                    "clicks": clicks,
                    "clickers": clickers,
                    "opens": opens,
                    "openers": openers,
                    "unsubs": removals,
                    "complaints": complaints,
                    "bounces": bounces,
                    "ca": ca,
                    "taux_clickers": round(clickers / sends * 100 if sends else 0, 3),
                    "taux_openers": round(opens / sends * 100 if sends else 0, 3),
                    "taux_unsubs": round(removals / sends * 100 if sends else 0, 3),
                    "taux_cto": round(clickers / opens * 100 if opens else 0, 3),
                    "taux_complaints": round(complaints / sends * 100 if sends else 0, 3),
                    "taux_bounces": round(bounces / sends if sends else 0, 3),
                    "analyses": {
                        "taux_clickers": self.analyze.analyze_click_rate(round(clickers / sends * 100 if sends else 0, 3)),
                        "taux_cto": self.analyze.analyze_cto_rate(round(clickers / opens * 100 if opens else 0, 3), opens),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(round(removals / sends * 100 if sends else 0, 3))
                    }
                })
            else:
                
                existing_brand["sends"] += sends
                existing_brand["clicks"] += clicks
                existing_brand["clickers"] += clickers
                existing_brand["opens"] += opens
                existing_brand["openers"] += openers
                existing_brand["unsubs"] += removals
                existing_brand["complaints"] += complaints
                existing_brand["bounces"] += bounces
                existing_brand["ca"] += ca
                existing_brand["creativities"]=optimized
                existing_brand["taux_clickers"] = round(existing_brand["clickers"] / existing_brand["sends"] * 100, 3)
                existing_brand["taux_openers"] = round(existing_brand["opens"] / existing_brand["sends"] * 100, 3)
                existing_brand["taux_unsubs"] = round(existing_brand["unsubs"] / existing_brand["sends"] * 100, 3)
                existing_brand["taux_cto"] = round(existing_brand["clickers"] / existing_brand["openers"] * 100 if existing_brand["openers"] else 0, 3)
                existing_brand["taux_complaints"] = round(existing_brand["complaints"] / existing_brand["sends"] * 100, 3)
                existing_brand["taux_bounces"] = round(existing_brand["bounces"] / existing_brand["sends"], 3)

        taux_clickers = round(total_clickers_global / total_sends_global * 100 if total_sends_global else 0, 3)
        taux_openers = round(total_openers_global / total_sends_global * 100 if total_sends_global else 0, 3)
        taux_unsubs = round(total_unsubs_global / total_sends_global * 100 if total_sends_global else 0, 3)
        taux_cto = round(total_clickers_global / total_openers_global * 100 if total_openers_global else 0, 3)
        taux_complaints = round(total_complaints_global / total_sends_global *100 if total_sends_global else 0.0,3)
        taux_bounces = round(total_bounces_global / total_sends_global if total_sends_global else 0.0,3)
        analyses = {
            "taux_clickers": self.analyze.analyze_click_rate(taux_clickers),
            "taux_cto": self.analyze.analyze_cto_rate(taux_cto,total_openers_global),
            "taux_unsubs":self.analyze.analyze_unsub_rate(taux_unsubs)
        }

        result = {
            "advertiser_id": str(adv_id),
            "globales": {
                "sends": total_sends_global,
                "clicks": total_clicks_global,
                "clickers": total_clickers_global,
                "opens": total_opens_global,
                "openers": total_openers_global,
                "unsubs": total_unsubs_global,
                "complaints": total_complaints_global,
                "bounces": total_bounces_global,
                "ecpm": round((total_ca_global / total_sends_global * 1000) if total_sends_global else 0, 3),
                "ca": round(total_ca_global, 2),
                "taux_clickers": taux_clickers,
                "taux_openers": taux_openers,
                "taux_unsubs": taux_unsubs,
                "taux_cto": taux_cto,
                "taux_complaints": taux_complaints,
                "taux_bounces": taux_bounces,
                "analyses": analyses
            },
            "bases": []
        }

        for base in bases.values():
            if base["sends"] <= 0:
                continue
            ca_clean = round(base["ca"], 2)
            ecpm = round((ca_clean / base["sends"]) * 1000 if base["sends"] else 0, 3)

            base_analyses = {
                "taux_clickers": self.analyze.analyze_click_rate(base["clickers"] / base["sends"] * 100),
                "taux_cto": self.analyze.analyze_cto_rate(base["clickers"] / base["openers"] * 100 if base["openers"] else 0, base["openers"]),
                "taux_unsubs": self.analyze.analyze_unsub_rate(base["unsubs"] / base["sends"] * 100)
            }

            result["bases"].append({
                "database_id": base["database_id"],
                "basename":base['basename'],
                "ktk_id":base["ktk_id"],
                "id_routers": base["id_routers"],
                "tag_id": base['tag_id'],
                "client_id": base['client_id'],
                "id_focus": base['id_focus'],
                "brands": base["brands"],
                "sends": base["sends"],
                "clicks": base['clicks'],
                "clickers": base["clickers"],
                "opens": base['opens'],
                "openers": base["openers"],
                "unsubs": base["unsubs"],
                "complaints": base["complaints"],
                "bounces": base["bounces"],
                "taux_clickers": round(base['clickers'] / base['sends'] * 100 if base['sends'] else 0.0, 3),
                "taux_openers": round(base['openers'] / base['sends'] * 100 if base['sends'] else 0.0, 3),
                "taux_unsubs": round(base['unsubs'] / base['sends'] * 100 if base['sends'] else 0.0, 3),
                "taux_cto": round(base['clickers'] / base['openers'] * 100 if base['openers'] else 0.0, 3),
                "taux_complaints": round(base["complaints"] / base["sends"] if base["sends"] else 0.0, 3),
                "taux_bounces": round(base["bounces"]/base["sends"] if base["sends"] else 0.0, 3),
                "ca": ca_clean,
                "ecpm": ecpm,
                "date_shedule": base.get("date_shedule"),
                "SegmentIds": sorted(list(base["SegmentIds"])),
                "subject": base64.b64decode(base.get("subject")).decode("utf-8") if base.get("subject") else "",
                "dimensions": base["dimensions"],
                "analyses": base_analyses
            })

        result["bases"].sort(key=lambda x: (x["clickers"], x["ecpm"], x["opens"], x["ca"]), reverse=True)
        for i, b in enumerate(result["bases"], 1):
            b["rang"] = i

        return result

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
                age_civilite_isp,
                brand,
                optimized,
                sum(sends)     AS sends,
                sum(clicks)    AS clicks,
                sum(clickers)  AS clickers,
                sum(opens)     AS opens,
                sum(openers)   AS openers,
                sum(removals)  AS removals,
                sum(bounces)   AS bounces,
                sum(complaints) AS complaints,
                max(ca)        AS ca,
                client_id,
                id_focus,
                tag_id,
                basename,
                ktk_id
            FROM reporting
            WHERE database_id = {db_id}
            GROUP BY
                adv_id,
                id_routers,
                age_range,
                gender,
                main_isp,
                age_civilite_isp,
                brand,
                optimized,
                client_id,
                id_focus,
                tag_id,
                basename,
                ktk_id
        """
        rows = self._execute_query(query)
        base_name_value=None
        ktk_value=None
        for r in rows:
            if base_name_value is None and ktk_value is None:
                base_name_value=r['basename']
                ktk_value=r['ktk_id']
        result = {
            "database_id": str(db_id),
            "globales": {
                "sends": 0,
                "clicks": 0,
                "clickers":0,
                "opens": 0,
                "openers":0,
                "removals": 0,
                "bounces":0,
                "complaints":0,
                "ca": 0.0
            },
            "advertisers": []
        }

        advertisers = {}
        routers_seen_global = set()

        for r in rows:
            adv_id = str(r["adv_id"])
            router = r["id_routers"]

            sends = r["sends"]
            clicks = r["clicks"]
            clickers = r["clickers"]
            opens = r["opens"]
            openers = r["openers"]
            removals = r["removals"]
            bounces = r["bounces"]
            complaints = r["complaints"]
            ca = r["ca"]
            brand_name = base64.b64decode(r["brand"]).decode("utf-8").strip()
            optimized_url = r.get("optimized") or "O_opt"
            g = result["globales"]
            g["sends"] += sends
            g["clicks"] += clicks
            g["clickers"] += clickers
            g["opens"] += opens
            g["openers"] += openers
            g["removals"] += removals
            g["bounces"] += bounces
            g["complaints"] += complaints
            if router not in routers_seen_global:
                g["ca"] += ca
                routers_seen_global.add(router)
            adv = advertisers.setdefault(adv_id, {
                "advertiser_id": adv_id,
                "routers_seen": set(),
                "id_routers":r["id_routers"],
                "client_id": r['client_id'],
                "id_focus": r['id_focus'],
                "tag": r['tag_id'],
                "brands": [],
                "sends": 0,
                "clicks": 0,
                "clickers": 0,
                "opens": 0,
                "openers": 0,
                "unsubs": 0,
                "bounces": 0,
                "complaints": 0,
                "ca": 0.0,
                "dimensions": {
                    "age_range": {},
                    "gender": {},
                    "isp": {},
                    "age_civilite_isp": {}
                }
            })

            if router not in adv["routers_seen"]:
                adv["ca"] += ca
                adv["routers_seen"].add(router)
            adv["sends"] += sends
            adv["clicks"] += clicks
            adv["clickers"] += clickers
            adv["opens"] += opens
            adv["openers"] += openers
            adv["unsubs"] += removals
            adv["bounces"] += bounces
            adv["complaints"] += complaints
            dims = {
                "age_range": safe(r["age_range"], "O_age"),
                "gender": safe(r["gender"], "O_gender"),
                "isp": safe(r["main_isp"], "O_isp"),
                "age_civilite_isp": safe(r["age_civilite_isp"], "O_combo")
            }
            for dim, val in dims.items():
                seg = adv["dimensions"][dim].setdefault(val, {
                    "sends": 0,
                    "clicks":0,
                    "clickers": 0,
                    "opens":0,
                    "openers": 0,
                    "unsubs": 0,
                    "bounces":0,
                    "complaints":0
                })
                seg["sends"] += sends
                seg["clicks"] += clicks
                seg["clickers"] += clickers
                seg["opens"] += opens
                seg["openers"] += openers
                seg["unsubs"] += removals
                seg["bounces"] += bounces
                seg["complaints"] += complaints
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
            if not existing_brand:
                adv["brands"].append({
                    "name": brand_name,
                    "creativities": optimized_url,
                    "sends": sends,
                    "clicks": clicks,
                    "clickers": clickers,
                    "opens": opens,
                    "openers": openers,
                    "unsubs": removals,
                    "bounces": bounces,
                    "complaints": complaints,
                    "ca": ca
                })
            else:
                existing_brand["sends"] += sends
                existing_brand["clicks"] += clicks
                existing_brand["clickers"] += clickers
                existing_brand["opens"] += opens
                existing_brand["openers"] += openers
                existing_brand["unsubs"] += removals
                existing_brand["bounces"] += bounces
                existing_brand["complaints"] += complaints
                existing_brand["ca"] += ca
                existing_brand["creativities"] = optimized_url
                existing_brand["taux_clickers"]=round(existing_brand["clickers"] / existing_brand["sends"]*100 if existing_brand["sends"] else 0.0,3)
                existing_brand["taux_openers"]=round(existing_brand["openers"] / existing_brand["sends"]*100 if existing_brand["sends"] else 0.0,3)
                existing_brand["taux_unsubs"]=round(existing_brand["unsubs"] / existing_brand["sends"]*100 if existing_brand["sends"] else 0.0,3)
                existing_brand["taux_complaints"]=round(existing_brand["complaints"] / existing_brand["sends"]*100 if existing_brand["sends"] else 0.0,3)
                existing_brand["taux_bounces"]=round(existing_brand["bounces"] / existing_brand["sends"]*100 if existing_brand["sends"] else 0.0,3)
                existing_brand["taux_cto"] = round(existing_brand["clickers"] / existing_brand["openers"] * 100 if existing_brand["openers"] else 0, 3)
        for adv in advertisers.values():
            sends = adv["sends"]
            openers = adv["openers"]
            adv["ecpm"] = round((adv["ca"] / sends) * 1000, 3) if sends else 0.0
            adv["taux_clickers"] = round(adv["clickers"] / sends * 100 if sends else 0.0, 3)
            adv["taux_openers"] = round(adv["openers"] / sends * 100 if sends else 0.0, 3)
            adv["taux_unsubs"] = round(adv["unsubs"] / sends * 100 if sends else 0.0, 3)
            adv["taux_cto"] = round(adv["clickers"] / openers * 100 if openers else 0.0, 3)
            adv["taux_complaints"] = round(adv["complaints"] / sends *100 if sends else 0.0,3)
            adv["taux_bounces"] = round(adv["bounces"] / sends * 100 if sends else 0.0,3)
            adv["classe"]=self.analyze.classify_advertiser(adv['ecpm'],adv['taux_clickers'])
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
        g["taux_unsubs"] = round(g["removals"] / sends * 100 if sends else 0, 3)
        g["taux_cto"] = round(g["clickers"] / openers * 100 if openers else 0, 3)
        g["taux_complaints"] = round(g["complaints"] / sends * 100 if sends else 0, 3)
        g["taux_bounces"] = round(g["bounces"] / sends * 100 if sends else 0, 3)
        g["analyses"] = {
            "taux_clickers": self.analyze.analyze_click_rate(g["taux_clickers"]),
            "taux_cto": self.analyze.analyze_cto_rate(g["taux_cto"], openers),
            "taux_unsubs": self.analyze.analyze_unsub_rate(g["taux_unsubs"])
        }
        result['basename']=base_name_value
        result['ktk_id']=ktk_value
        return result

    def calendrier(self, adv_id):
        top_months = 3       
        seuil_ecpm = 0      

        query = f"""
            SELECT
                toMonth(date_event) AS month,
                toDayOfWeek(date_event) AS day,
                toHour(date_event) AS hour,
                SUM(sends) AS sends,
                SUM(ca_per_router) AS ca
            FROM
            (
                SELECT
                    id_routers,
                    date_event,
                    SUM(sends) AS sends,
                    MAX(ca) AS ca_per_router
                FROM reporting
                WHERE adv_id = {adv_id}
                GROUP BY id_routers, date_event
            ) AS per_router
            GROUP BY month, day, hour
            HAVING sends > 50
            ORDER BY month, day, hour
        """

        rows = self._execute_query(query)

        best_month = {}

        for r in rows:

            sends = r["sends"] or 0
            ca = r["ca"] or 0

            if sends == 0:
                continue

            ecpm = round((ca / sends) * 1000, 3)

            if ecpm < seuil_ecpm:
                continue

            month = r["month"]
            day = r["day"]
            hour = r["hour"]

            if (month not in best_month or ecpm > best_month[month]["ecpm"]):
                best_month[month] = {
                    "month": month,
                    "day": day,
                    "hour": hour,
                    "ecpm": ecpm 
                }
        top = sorted(best_month.values(),key=lambda x: x["ecpm"],reverse=True)[:top_months]

        today = datetime.now()
        results = []
        for slot in top:
            for d in range(1, 450):
                future = today + timedelta(days=d)
                if (future.month == slot["month"]and future.isoweekday() == slot["day"]):
                    lancement = future.replace(hour=slot["hour"],minute=0,second=0,microsecond=0)
                    if lancement > today:
                        results.append({
                            "mois": calendar.month_name[slot["month"]],
                            "lancement": lancement.strftime("%Y-%m-%d %H:%M")
                        })
                        break

        return results

    def best_segment(self, adv_id, min_sends=100):
        query = f"""
            SELECT age_range, gender, main_isp, age_civilite_isp, sends, ca
            FROM reporting
            WHERE adv_id={adv_id}
        """
        rows = self._execute_query(query)

        total_sends = sum(r["sends"] or 0 for r in rows)
        if total_sends == 0:
            return []

        segments = defaultdict(lambda: {"sends": 0, "ca": Decimal("0.0")})

        for r in rows:
            sends = r["sends"] or 0
            ca = Decimal(r["ca"] or 0)

            key = r["age_civilite_isp"]
            if sends == 0:
                continue

            segments[key]["sends"] += sends
            proportion = Decimal(sends) / Decimal(total_sends)
            segments[key]["ca"] += (ca * proportion).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        result = []
        for seg, v in segments.items():
            if v["sends"] < min_sends:
                continue

            ecpm = float((v["ca"] / Decimal(v["sends"]) * 1000).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            result.append({
                "segment": seg,
                "ecpm": ecpm
            })

        result.sort(key=lambda x: x["ecpm"], reverse=True)
        return [r["segment"] for r in result[:10]]

    def programmes(self,adv):
      programme = {
         "segment":self.best_segment(adv),
          "calendrier":self.calendrier(adv)
     }
     
      return programme
    def list_advertiser(self):
        try:
            query = """
                SELECT id, name
                FROM advertiser
                ORDER BY name
            """
            rows = self._execute_query(query)

            return {
                "total": len(rows),
                "advertisers": rows
            }

        except Exception as e:
            print("Erreur liste advertiser :", e)
            return {
                "total": 0,
                "advertisers": []
            }
    def list_tags(self):
            try:
                query = """
                    SELECT id, tag as name,dwtag
                    FROM tags
                    ORDER BY tag
                """
                rows = self._execute_query(query)

                return {
                    "total": len(rows),
                    "tags": rows
                }

            except Exception as e:
                print("Erreur liste advertiser :", e)
                return {
                    "total": 0,
                    "advertisers": []
                }
   
    """def top_10_objet(self):
        try:
            query = 
                SELECT DISTINCT
                    database_id,
                    subject,
                    id_routers,
                    sends,
                    clicks,
                    opens,
                    ca,
                    ROUND(ca / sends * 1000, 2) AS ecpm
                FROM (
                    SELECT DISTINCT
                        database_id,
                        subject,
                        id_routers,
                        SUM(sends) AS sends,
                        SUM(clicks) AS clicks,
                        SUM(opens) AS opens,
                        MAX(ca) AS ca
                    FROM reporting
                    GROUP BY database_id, id_routers, subject
                    HAVING sends > 0
                ) AS aggregated
                ORDER BY clicks DESC, ecpm DESC, ca DESC, opens DESC
                LIMIT 10
            
            rows = self._execute_query(query)

            top_10 = []
            base64_pattern = re.compile(r'^[A-Za-z0-9+/=]{20,}$')

            for r in rows:
                subj = r['subject']
                if subj and base64_pattern.match(subj):
                    try:
                        subj_decoded = base64.b64decode(subj).decode('utf-8')
                    except Exception:
                        subj_decoded = subj
                    top_10.append(subj_decoded)
                else:
                    top_10.append(subj)

            return {"top_10": top_10}

        except Exception as e:
            print("erreur top 10", e)
            return {"top_10": []}"""
        
    def advertiser_counts(self, adv_id: int):
        query = f"""
            SELECT gender, age_range, main_isp, COUNT() AS total
            FROM reporting
            WHERE adv_id = {adv_id}
            GROUP BY gender, age_range, main_isp
        """
        rows = self._execute_query(query)

        result = {
            "advertiser_id": adv_id,
            "totals": 0,
            "details": []
        }

        for r in rows:
            gender = r["gender"] or "O_gender"
            age_range = r["age_range"] or "O_age"
            isp = r["main_isp"] or "O_isp"
            total = r["total"] or 0
            result["totals"] += total
            result["details"].append({
                "gender": gender,
                "age_range": age_range,
                "isp": isp,
                "total": total
            })

        def filter_counts(gender: Optional[str] = None, min_age: Optional[int] = None,max_age: Optional[int] = None,isp: Optional[str] = None):
            total_filtered = 0

            for item in result["details"]:
                if gender and item.get("gender") != gender:
                    continue

                if isp and item.get("isp") != isp:
                    continue

                if min_age is not None or max_age is not None:
                    try:
                        start, end = map(int, item["age_range"].split('-'))
                    except ValueError:
                        continue
                    if min_age is not None and end < min_age:
                        continue
                    if max_age is not None and start > max_age:
                        continue

                total_filtered += item.get("total", 0)

            comptage = {
                "gender": gender,
                "min_age": min_age,
                "max_age": max_age,
                "isp": isp,
                "total": total_filtered
            }

            return {
                "adv_id": adv_id,
                "comptage": comptage
            }
        result["filter"] = filter_counts
        return result
    def liste_adv_id_reporting(self):
        try:
            query = f"""
                SELECT distinct r.adv_id,a.name FROM reporting r JOIN advertiser a ON toUInt64(r.adv_id)=toUInt64(a.id) ORDER BY r.adv_id
            """
            rows = self._execute_query(query)
            advertisers = [{"adv_id":r['adv_id'],"name":r["name"]} for r in rows]
            return {
                "total":len(rows),
                "advertisers":advertisers
            }
        except Exception as e:
            print("liste adv_id reporting",e)