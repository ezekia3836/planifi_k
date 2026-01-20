from collections import defaultdict
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
from datetime import timedelta,datetime
import calendar
class Query:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient()
        self.analyze = analyse()
    def _execute_query(self, query):
        result = self.clk.query(query)
        return [dict(zip(result.column_names, r)) for r in result.result_rows]
    def global_advertiser(self, adv_id):
        query = f"""
            SELECT advertiser_name,
                database_id,
                id_routers,
                ca,
                sends,
                clicks,
                opens,
                removals,
                complains,
                dimensions,
                valuedimension
            FROM reporting
            WHERE adv_id = {adv_id}
        """
        rows = self._execute_query(query)

        if not rows:
            return {
                "advertiser_id": str(adv_id),
                "advertiser_name": None,
                "bases": []
            }

        advertiser_name = rows[0].get("advertiser_name")
        bases_dict = {}

        for r in rows:
            db_id = r.get("database_id")
            router = r.get("id_routers")
            key = (db_id, router)
            if key not in bases_dict:
                bases_dict[key] = {
                    "database_id": db_id,
                    "id_routers": router,
                    "ca": float(r.get("ca") or 0.0),
                    "sends_total": int(r.get("sends") or 0),
                    "clicks_total": int(r.get("clicks") or 0),
                    "opens_total": int(r.get("opens") or 0),
                    "removals_total": int(r.get("removals") or 0),
                    "complains_total": int(r.get("complains") or 0),
                    "dimensions": defaultdict(dict)
                }
            else:
                base = bases_dict[key]
                base["ca"] += float(r.get("ca") or 0.0)
                base["sends_total"] += int(r.get("sends") or 0)
                base["clicks_total"] += int(r.get("clicks") or 0)
                base["opens_total"] += int(r.get("opens") or 0)
                base["removals_total"] += int(r.get("removals") or 0)
                base["complains_total"] += int(r.get("complains") or 0)
                

            base = bases_dict[key]

            civilite = r.get("valuedimension") if r.get("dimensions") == "civilite" else r.get("civilite") or "O_gender"
            age = r.get("valuedimension") if r.get("dimensions") == "age_range" else r.get("age_range") or "O_age"
            isp = r.get("valuedimension") if r.get("dimensions") == "isp" else r.get("main_isp") or "O_isp"

            for dim_name, val in [
                ("civilite", civilite),
                ("age_range", age),
                ("isp", isp)
            ]:
                if val not in base["dimensions"][dim_name]:
                    base["dimensions"][dim_name][val] = {
                        "sends": int(r.get("sends") or 0),
                        "clickers": int(r.get("clicks") or 0),
                        "openers": int(r.get("opens") or 0),
                        "unsubs": int(r.get("removals") or 0),
                        "complains": int(r.get("complains") or 0)
                    }
                else:
                    m = base["dimensions"][dim_name][val]
                    m["sends"] += int(r.get("sends") or 0)
                    m["clickers"] += int(r.get("clicks") or 0)
                    m["openers"] += int(r.get("opens") or 0)
                    m["unsubs"] += int(r.get("removals") or 0)
                    m["complains"] += int(r.get("complains") or 0)

           
            combo_key = f"{age}_{civilite}_{isp}"
            if combo_key not in base["dimensions"]["age_civilite_isp"]:
                base["dimensions"]["age_civilite_isp"][combo_key] = {
                    "sends": int(r.get("sends") or 0),
                    "clickers": int(r.get("clicks") or 0),
                    "openers": int(r.get("opens") or 0),
                    "unsubs": int(r.get("removals") or 0),
                    "complains": int(r.get("complains") or 0)
                }
            else:
                m = base["dimensions"]["age_civilite_isp"][combo_key]
                m["sends"] += int(r.get("sends") or 0)
                m["clickers"] += int(r.get("clicks") or 0)
                m["openers"] += int(r.get("opens") or 0)
                m["unsubs"] += int(r.get("removals") or 0)
                m["complains"] += int(r.get("complains") or 0)
        for base in bases_dict.values():
            for dim_name, dim_values in base["dimensions"].items():
                for val, metrics in dim_values.items():
                    sends = metrics["sends"]
                    clicks = metrics["clickers"]
                    opens = metrics["openers"]
                    removals = metrics["unsubs"]

                    taux_clicks = round(clicks / sends * 100, 2) if sends else 0.0
                    taux_cto = round(clicks / opens * 100, 2) if opens else 0.0
                    taux_unsubs = round(removals / sends * 100, 2) if sends else 0.0

                    metrics["analyse"] = {
                        "taux_clicks": self.analyze.analyze_click_rate(taux_clicks),
                        "taux_cto": self.analyze.analyze_cto_rate(taux_cto, opens),
                        "taux_unsubs": self.analyze.analyze_unsub_rate(taux_unsubs)
                    }
        result = {
            "advertiser_id": str(adv_id),
            "advertiser_name": advertiser_name,
            "bases": []
        }

        for base in bases_dict.values():
            sends = base["sends_total"]
            clicks = base["clicks_total"]
            opens = base["opens_total"]
            removals = base["removals_total"]
            ca = base["ca"]

            result["bases"].append({
                "database_id": base["database_id"],
                "id_routers": base["id_routers"],
                "ca": ca,
                "ecpm": round((ca / sends * 1000), 2) if sends else 0.0,
                "sends":sends,
                "taux_clicks": round(clicks / sends * 100, 2) if sends else 0.0,
                "taux_cto": round(clicks / opens * 100, 2) if opens else 0.0,
                "taux_desabo": round(removals / sends * 100, 2) if sends else 0.0,
                "analyse_globale":{
                    "taux_clicks":self.analyze.analyze_click_rate(round(clicks / opens * 100, 2))
                },
                "dimensions": base["dimensions"]
            })

        return result

   

    def global_base(self, db_id):
        query = f"""
            SELECT advertiser_name, ca, sends, clicks, opens, removals,
                dimensions, valuedimension
            FROM reporting
            WHERE database_id = {db_id}
        """
        rows = self._execute_query(query)

        result = {
            "database_id": db_id,
            "advertisers": []
        }

        if not rows:
            return result

        advertisers_map = {}

        for r in rows:
            advertiser_name = r.get("advertiser_name", "Inconnu")

            if advertiser_name not in advertisers_map:
                advertisers_map[advertiser_name] = {
                    "advertiser_name": advertiser_name,
                    "sends_total": 0,
                    "clicks_total": 0,
                    "opens_total": 0,
                    "removals_total": 0,
                    "ca": 0.0,
                    "dimensions": defaultdict(dict)
                }

            adv_data = advertisers_map[advertiser_name]

            sends = int(r.get("sends") or 0)
            clicks = int(r.get("clicks") or 0)
            opens = int(r.get("opens") or 0)
            removals = int(r.get("removals") or 0)
            ca = float(r.get("ca") or 0.0)

            adv_data["sends_total"] += sends
            adv_data["clicks_total"] += clicks
            adv_data["opens_total"] += opens
            adv_data["removals_total"] += removals
            adv_data["ca"] += ca 
            dim_type = r.get("dimensions")
            dim_value = r.get("valuedimension")

            if dim_type == "civilite":
                dim_value = dim_value or "O_gender"
            elif dim_type == "age_range":
                dim_value = dim_value or "O_age"
            elif dim_type == "isp":
                dim_value = dim_value or "O_isp"

            if dim_type:
                metrics = adv_data["dimensions"][dim_type].setdefault(
                    dim_value, {"sends": 0, "clickers": 0, "openers": 0, "unsubs": 0}
                )
                metrics["sends"] += sends
                metrics["clickers"] += clicks
                metrics["openers"] += opens
                metrics["unsubs"] += removals

                taux_clicks = round(metrics["clickers"] / metrics["sends"] * 100, 2) if metrics["sends"] else 0.0
                taux_cto = round(metrics["clickers"] / metrics["openers"] * 100, 2) if metrics["openers"] else 0.0
                taux_unsubs = round(metrics["unsubs"] / metrics["sends"] * 100, 2) if metrics["sends"] else 0.0

                metrics["analyse"] = {
                    "taux_clicks": self.analyze.analyze_click_rate(taux_clicks),
                    "taux_cto": self.analyze.analyze_cto_rate(taux_cto, metrics["openers"]),
                    "taux_desabo": self.analyze.analyze_unsub_rate(taux_unsubs)
                }
        for adv in advertisers_map.values():
            sends = adv["sends_total"]
            clicks = adv["clicks_total"]
            opens = adv["opens_total"]
            removals = adv["removals_total"]
            ca = adv["ca"]

            adv["ecpm"] = round(ca / sends * 1000, 2) if sends else 0.0
            adv["taux_clicks"] = round(clicks / sends * 100, 2) if sends else 0.0
            adv["taux_cto"] = round(clicks / opens * 100, 2) if opens else 0.0
            adv["taux_desabo"] = round(removals / sends * 100, 2) if sends else 0.0
            adv["classification"] = self.analyze.classify_advertiser(adv["ecpm"], adv["taux_clicks"])

            result["advertisers"].append(adv)

        return result


    def calendrier(self, adv_id, month_actu=None):
        query = f"""
            SELECT advertiser_name, year, month, week, day, hour, ca, sends
            FROM reporting
            WHERE adv_id = {adv_id}
        """
        rows = self._execute_query(query)

        if not rows:
            return {
                "adv_id": adv_id,
                "advertiser_name": None,
                "proposition_date": {}
            }

        advertiser_name = rows[0].get("advertiser_name", "Inconnu")

        for r in rows:
            ca = float(r.get("ca") or 0)
            sends = int(r.get("sends") or 0)
            r["ecpm"] = round((ca / sends * 1000), 2) if sends else 0.0

        ecpm_by_month = defaultdict(float)
        for r in rows:
            ecpm_by_month[r["month"]] += r["ecpm"]

        month_names = {
            1: "janvier", 2: "février", 3: "mars", 4: "avril",
            5: "mai", 6: "juin", 7: "juillet", 8: "août",
            9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
        }

        best_ecpm = max(ecpm_by_month.values())
        sueil = 0.9 * best_ecpm 
        best_month_nums = sorted([m for m, e in ecpm_by_month.items() if e >= sueil],key=lambda m: ecpm_by_month[m],reverse=True)[:3] 
        today = datetime.today()
        current_year = today.year

        proposition_date = {}
        used_dates = set()

        for month in best_month_nums:
            rows_month = [r for r in rows if r["month"] == month and r["year"] == current_year]
            weeks_map = defaultdict(lambda: defaultdict(lambda: {"hour": None, "ecpm": -1}))

            for r in rows_month:
                week = int(r["week"])
                day = int(r["day"])
                hour = int(r["hour"])
                ecpm = r["ecpm"]
                if ecpm > weeks_map[week][day]["ecpm"]:
                    weeks_map[week][day] = {"hour": hour, "ecpm": ecpm}

            dates_list = []
            month_calendar = calendar.Calendar(firstweekday=0)
            for week in sorted(weeks_map.keys()):
                for day, data in sorted(weeks_map[week].items()):
                    hour = data["hour"]
                    day_date = None
                    for d in month_calendar.itermonthdates(current_year, month):
                        if d.weekday() == day and d.month == month:
                            potential_date = datetime(d.year, d.month, d.day, hour)
                            if potential_date >= today and potential_date.date() not in used_dates:
                                day_date = potential_date
                                used_dates.add(day_date.date())
                                break
                    if day_date:
                        dates_list.append(day_date.strftime("%Y-%m-%d %H:%M:%S"))

            proposition_date[month_names[month]] = {"date": dates_list}

        return {
            "adv_id": adv_id,
            "advertiser_name": advertiser_name,
            "proposition_date": proposition_date
        }
