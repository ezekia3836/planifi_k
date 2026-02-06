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
                main_isp,
                date_shedule,
                age_civilite_isp,
                SUM(sends) AS sends,
                SUM(clicks) AS clicks,
                SUM(opens) AS opens,
                SUM(removals) AS removals,
                SUM(complaints) AS complaints,
                MAX(ca) AS ca,
                segmentId,
                subject
            FROM reporting
            WHERE adv_id = {adv_id}
            GROUP BY
                database_id,
                id_routers,
                age_range,
                gender,
                main_isp,
                age_civilite_isp,
                brand,
                tag_id,
                date_shedule,
                segmentId,
                subject
        """
        rows = self._execute_query(query)
        if not rows:
            return {"advertiser_id": str(adv_id), "globales": {}, "bases": []}

        bases = {}
        total_sends_global = 0
        total_ca_global = 0
        total_clickers_global = 0
        total_openers_global = 0
        total_unsubs_global = 0
        total_complaints_global = 0

        for r in rows:
            base_key = (r["database_id"], r["id_routers"], r['tag_id'], r['brand'],r['segmentId'])
            base = bases.setdefault(base_key, {
                "database_id": r["database_id"],
                "id_routers": r["id_routers"],
                "tag_id": r['tag_id'],
                "brand": r['brand'],
                "sends": 0,
                "clickers": 0,
                "openers": 0,
                "unsubs": 0,
                "complaints": 0,
                "ca": 0.0,
                "date_shedule": [],
                "SegmentId": r.get("segmentId"),
                "subject": r.get("subject"),
                "dimensions": {
                    "age_range": {},
                    "gender": {},
                    "isp": {},
                    "age_civilite_isp": {}
                }
            })

            sends = r["sends"]
            clicks = r["clicks"]
            opens = r["opens"]
            removals = r["removals"]
            complaints = r["complaints"]
            ca = r["ca"]

            base["sends"] += sends
            base["clickers"] += clicks
            base["openers"] += opens
            base["unsubs"] += removals
            base["complaints"] += complaints
            base["ca"] += ca

            base["date_shedule"] = sorted(set(base["date_shedule"] + (r["date_shedule"] or [])))

            total_sends_global += sends
            total_ca_global += ca
            total_clickers_global += clicks
            total_openers_global += opens
            total_unsubs_global += removals
            total_complaints_global += complaints

            def push(dim, value):
                if not value:
                    return
                seg = base["dimensions"][dim].setdefault(value, {
                    "sends": 0,
                    "clickers": 0,
                    "openers": 0,
                    "unsubs": 0,
                    "complaints": 0
                })
                seg["sends"] += sends
                seg["clickers"] += clicks
                seg["openers"] += opens
                seg["unsubs"] += removals
                seg["complaints"] += complaints

            push("age_range", r["age_range"])
            push("gender", r["gender"])
            push("isp", r["main_isp"])
            push("age_civilite_isp", r["age_civilite_isp"])
        result = {
            "advertiser_id": str(adv_id),
            "globales": {
                "sends": total_sends_global,
                "clickers": total_clickers_global,
                "openers": total_openers_global,
                "unsubs": total_unsubs_global,
                "complaints": total_complaints_global,
                "ecpm": round((total_ca_global / total_sends_global * 1000) if total_sends_global else 0, 3),
                "ca": round(total_ca_global, 2),
                "taux_clicks": round(total_clickers_global / total_sends_global * 100 if total_sends_global else 0, 3),
                "taux_opens": round(total_openers_global / total_sends_global * 100 if total_sends_global else 0, 3),
                "taux_unsubs": round(total_unsubs_global / total_sends_global * 100 if total_sends_global else 0, 3),
                "taux_cto": round(total_clickers_global / total_openers_global * 100 if total_openers_global else 0, 3)
            },
            "bases": []
        }
        for base in bases.values():
            if base["sends"] <= 0:
                continue

            ca_clean = round(base["ca"], 2)
            ecpm = round((ca_clean / base["sends"]) * 1000 if base["sends"] else 0, 3)

            for dim_name, dim_values in base["dimensions"].items():
                for seg in dim_values.values():
                    seg["taux_clicks"] = round(seg["clickers"] / seg["sends"] * 100 if seg["sends"] else 0, 3)
                    seg["taux_cto"] = round(seg["clickers"] / seg["openers"] * 100 if seg["openers"] else 0, 3)
                    seg["taux_unsubs"] = round(seg["unsubs"] / seg["sends"] * 100 if seg["sends"] else 0, 3)
                    seg["analyses"] = {
                        "taux_clicks": self.analyze.analyze_click_rate(seg["taux_clicks"]),
                        "taux_cto": self.analyze.analyze_cto_rate(seg["taux_cto"], seg["openers"]),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(seg["taux_unsubs"])
                    }
            base["dimensions"]["age_range"] = dict(
                sorted(base["dimensions"]["age_range"].items(), key=lambda x: self.age_sort_key(x[0]))
            )

            base_analyses = {
                "taux_clicks": self.analyze.analyze_click_rate(base["clickers"] / base["sends"] * 100),
                "taux_cto": self.analyze.analyze_cto_rate(
                    base["clickers"] / base["openers"] * 100 if base["openers"] else 0, base["openers"]
                ),
                "taux_unsubs": self.analyze.analyze_unsub_rate(base["unsubs"] / base["sends"] * 100)
            }

            result["bases"].append({
                "database_id": base["database_id"],
                "id_routers": base["id_routers"],
                "tag_id": base['tag_id'],
                "brand": base64.b64decode(base['brand']).decode("utf-8"),
                "sends": base["sends"],
                "clickers": base["clickers"],
                "openers": base["openers"],
                "unsubs": base["unsubs"],
                "complaints": base["complaints"],
                "taux_clicks": round(base['clickers'] / base['sends'] * 100 if base['sends'] else 0.0, 3),
                "taux_opens": round(base['openers'] / base['sends'] * 100 if base['sends'] else 0.0, 3),
                "taux_unsubs": round(base['unsubs'] / base['sends'] * 100 if base['sends'] else 0.0, 3),
                "taux_cto": round(base['clickers'] / base['openers'] * 100 if base['openers'] else 0.0, 3),
                "ca": ca_clean,
                "ecpm": ecpm,
                "date_shedule": base.get("date_shedule"),
                "SegmentId": base.get("SegmentId"),
                "subject": base64.b64decode(base.get("subject")).decode("utf-8"),
                "dimensions": base["dimensions"],
                "analyses": base_analyses
            })

        result["bases"].sort(
            key=lambda x: (x["clickers"], x["ecpm"], x["ca"], x["openers"]),
            reverse=True
        )

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
                sum(sends)     AS sends,
                sum(clicks)    AS clicks,
                sum(opens)     AS opens,
                sum(removals)  AS removals,
                max(ca)        AS ca
            FROM reporting
            WHERE database_id = {db_id}
            GROUP BY
                adv_id,
                id_routers,
                age_range,
                gender,
                main_isp,
                age_civilite_isp
        """

        rows = self._execute_query(query)

        result = {
            "database_id": str(db_id),
            "globale_base": {
                "sends_total": 0,
                "clicks_total": 0,
                "opens_total": 0,
                "removals_total": 0,
                "ca_total": 0.0
            },
            "advertisers": []
        }

        advertisers = {}
        routers_seen = set()

        for r in rows:

            adv_id = str(r["adv_id"])
            router = r["id_routers"]

            sends = r["sends"]
            clicks = r["clicks"]
            opens = r["opens"]
            removals = r["removals"]
            ca = r["ca"]

            g = result["globale_base"]

            g["sends_total"] += sends
            g["clicks_total"] += clicks
            g["opens_total"] += opens
            g["removals_total"] += removals

            if router not in routers_seen:
                g["ca_total"] += ca
                routers_seen.add(router)

            adv = advertisers.setdefault(adv_id, {
                "advertiser_id": adv_id,
                "routers_seen": set(),
                "sends": 0,
                "clicks": 0,
                "opens": 0,
                "removals": 0,
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
            adv["opens"] += opens
            adv["removals"] += removals

            dims = {
                "age_range": safe(r["age_range"], "O_age"),
                "gender": safe(r["gender"], "O_gender"),
                "isp": safe(r["main_isp"], "O_isp"),
                "age_civilite_isp": safe(r["age_civilite_isp"], "O_combo")
            }

            for dim, val in dims.items():
                seg = adv["dimensions"][dim].setdefault(val, {
                    "sends": 0,
                    "clickers": 0,
                    "openers": 0,
                    "unsubs": 0
                })

                seg["sends"] += sends
                seg["clickers"] += clicks
                seg["openers"] += opens
                seg["unsubs"] += removals
                seg['taux_clicks'] = round(seg['clickers'] / seg['sends'] *100 if seg['sends'] else 0.0,3)
                seg['taux_cto'] = round(seg['clickers'] / seg['openers'] if seg['openers'] else 0.0 ,3)
                seg['taux_unsubs'] = round(seg['unsubs'] / seg['sends'] if seg['sends'] else 0.0,3)
                analyse_seg={
                    "taux_clicks":self.analyze.analyze_click_rate(seg['taux_clicks']),
                    "taux_cto": self.analyze.analyze_cto_rate(seg['taux_cto'],seg['openers']),
                    "taux_unsubs":self.analyze.analyze_unsub_rate(seg['unsubs'])
                }
                seg['analyses']=analyse_seg

        g = result["globale_base"]

        sends = g["sends_total"]
        opens = g["opens_total"]

        g["ecpm"] = round((g["ca_total"] / sends) * 1000, 3) if sends else 0
        g["taux_clicks"] = round(g["clicks_total"] / sends * 100, 3) if sends else 0
        g["taux_opens"] = round(g["opens_total"] / sends * 100, 3) if sends else 0
        g["taux_unsubs"] = round(g["removals_total"] / sends * 100, 3) if sends else 0
        g["taux_cto"] = round(g["clicks_total"] / opens * 100, 3) if opens else 0

        g["analyses"] = {
            "taux_clicks": self.analyze.analyze_click_rate(g["taux_clicks"]),
            "taux_cto": self.analyze.analyze_cto_rate(g["taux_cto"], opens),
            "taux_unsubs": self.analyze.analyze_unsub_rate(g["taux_unsubs"])
        }

        for adv in advertisers.values():

            sends = adv["sends"]
            opens = adv["opens"]

            adv["ecpm"] = round((adv["ca"] / sends) * 1000, 3) if sends else 0
            adv["taux_clicks"] = round(adv["clicks"] / sends * 100, 3) if sends else 0
            adv["taux_opens"] = round(adv["opens"] / sends * 100, 3) if sends else 0
            adv["taux_unsubs"] = round(adv["removals"] / sends * 100, 3) if sends else 0
            adv["taux_cto"] = round(adv["clicks"] / opens * 100, 3) if opens else 0
            adv["classe"] = self.analyze.classify_advertiser(adv['ecpm'], adv["taux_clicks"])

            adv["analyses"] = {
                "taux_clicks": self.analyze.analyze_click_rate(adv["taux_clicks"]),
                "taux_cto": self.analyze.analyze_cto_rate(adv["taux_cto"], opens),
                "taux_unsubs": self.analyze.analyze_unsub_rate(adv["taux_unsubs"])
            }

            adv.pop("routers_seen")
            result["advertisers"].append(adv)

        result["advertisers"].sort(key=lambda x: x["ecpm"], reverse=True)

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
   
    def top_10_objet(self):
        try:
            query = """
                SELECT
                    database_id,
                    subject,
                    id_routers,
                    sends,
                    clicks,
                    opens,
                    ca,
                    ROUND(ca / sends * 1000, 2) AS ecpm
                FROM (
                    SELECT
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
            """
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
            return {"top_10": []}
        
    def advertiser_counts(self, adv_id: int):
        query = f"""
            SELECT gender, age_range, main_isp, SUM(sends) AS total
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
            total = r["total"]

            result["totals"] += total
            result["details"].append({
                "gender": gender,
                "age_range": age_range,
                "isp": isp,
                "total": total
            })

        def filter_counts(
            gender: Optional[str] = None,
            min_age: Optional[int] = None,
            max_age: Optional[int] = None,
            isp: Optional[str] = None
        ):
            total_filtered = 0

            for item in result["details"]:

                if gender and item["gender"] != gender:
                    continue

                if isp and item["isp"] != isp:
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

                total_filtered += item["total"]
                GENDER_LABELS = {
                    "M": "Homme",
                    "F": "Femme",
                    "O_gender": "Inconnu"
                }
            label_parts = []

            if gender:
                label_parts.append(GENDER_LABELS.get(gender, gender))

            if min_age is not None and max_age is not None:
                label_parts.append(f"{min_age}-{max_age} ans")
            elif min_age is not None:
                label_parts.append(f"+{min_age} ans")
            elif max_age is not None:
                label_parts.append(f"-{max_age} ans")

            if isp:
                label_parts.append(isp)

            label = " ".join(label_parts) if label_parts else "Total"

            return {
                "label": label,
                "total": total_filtered
            }
        result["filter"] = filter_counts
        return result
