from collections import defaultdict
from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
from datetime import timedelta,datetime
class Query:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient()
        self.analyze = analyse()
    def _execute_query(self, query):
        result = self.clk.query(query)
        return [dict(zip(result.column_names, r)) for r in result.result_rows]

    def global_advertiser(self, adv_id):
        query = f"SELECT * FROM reporting WHERE adv_id = {adv_id}"
        rows = self._execute_query(query)

        if not rows:
            return {"advertiser_id": str(adv_id), "advertiser_name": None, "bases": []}

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
                    "ca_max": float(r.get("ca") or 0.0),
                    "sends_total": int(r.get("sends") or 0),
                    "clicks_total": int(r.get("clicks") or 0),
                    "opens_total": int(r.get("opens") or 0),
                    "removals_total": int(r.get("removals") or 0),
                    "complains_total": int(r.get("complains") or 0),
                    "dimensions": defaultdict(dict)
                }
            else:
                base = bases_dict[key]
                base["ca_max"] = max(base["ca_max"], float(r.get("ca") or 0.0))
                base["sends_total"] += int(r.get("sends") or 0)
                base["clicks_total"] += int(r.get("clicks") or 0)
                base["opens_total"] += int(r.get("opens") or 0)
                base["removals_total"] += int(r.get("removals") or 0)
                base["complains_total"] += int(r.get("complains") or 0)

            base = bases_dict[key]
            civilite = r.get("valuedimension") if r.get("dimensions") == "civilite" else r.get("civilite") or "O_gender"
            age = r.get("valuedimension") if r.get("dimensions") == "age_range" else r.get("age_range") or "O_age"
            isp = r.get("valuedimension") if r.get("dimensions") == "isp" else r.get("main_isp") or "O_isp"

            for dim_name, val in [("civilite", civilite), ("age_range", age), ("isp", isp)]:
                if val not in base["dimensions"][dim_name]:
                    base["dimensions"][dim_name][val] = {
                        "sends": int(r.get("sends") or 0),
                        "clicks": int(r.get("clicks") or 0),
                        "opens": int(r.get("opens") or 0),
                        "removals": int(r.get("removals") or 0),
                        "complains": int(r.get("complains") or 0)
                    }
                else:
                    metrics = base["dimensions"][dim_name][val]
                    metrics["sends"] += int(r.get("sends") or 0)
                    metrics["clicks"] += int(r.get("clicks") or 0)
                    metrics["opens"] += int(r.get("opens") or 0)
                    metrics["removals"] += int(r.get("removals") or 0)
                    metrics["complains"] += int(r.get("complains") or 0)

            combo_key = f"{age}_{civilite}_{isp}"
            if combo_key not in base["dimensions"]["age_civilite_isp"]:
                base["dimensions"]["age_civilite_isp"][combo_key] = {
                    "sends": int(r.get("sends") or 0),
                    "clicks": int(r.get("clicks") or 0),
                    "opens": int(r.get("opens") or 0),
                    "removals": int(r.get("removals") or 0),
                    "complains": int(r.get("complains") or 0)
                }
            else:
                metrics = base["dimensions"]["age_civilite_isp"][combo_key]
                metrics["sends"] += int(r.get("sends") or 0)
                metrics["clicks"] += int(r.get("clicks") or 0)
                metrics["opens"] += int(r.get("opens") or 0)
                metrics["removals"] += int(r.get("removals") or 0)
                metrics["complains"] += int(r.get("complains") or 0)

        result = {"advertiser_id": str(adv_id), "advertiser_name": advertiser_name, "bases": []}

        for base in bases_dict.values():
            sends = base["sends_total"]
            clicks = base["clicks_total"]
            opens = base["opens_total"]
            removals = base["removals_total"]
            ca = base["ca_max"]

            base_entry = {
                "database_id": base["database_id"],
                "id_routers": base["id_routers"],
                "ca_max": ca,
                "ecpm": round((ca / sends * 1000), 2) if sends else 0.0,
                "taux_clicks": round((clicks / sends * 100), 2) if sends else 0.0,
                "taux_cto": round((clicks / opens * 100), 2) if opens else 0.0,
                "taux_desabo": round((removals / sends * 100), 2) if sends else 0.0,
                "dimensions": base["dimensions"]
            }
            result["bases"].append(base_entry)

        return result
    def global_base(self, db_id):
        query = f"SELECT * FROM reporting WHERE database_id = {db_id}"
        rows = self._execute_query(query)

        result = {"database_id": db_id, "advertisers": {}}
        if not rows:
            return result

        advertiser_name = rows[0].get("advertiser_name", "Inconnu")

        adv_data = {
            "advertiser_name": advertiser_name,
            "sends_total": 0,
            "clicks_total": 0,
            "opens_total": 0,
            "removals_total": 0,
            "ca_max": 0.0,
            "dimensions": defaultdict(dict)
        }

        for r in rows:
            
            sends = int(r.get("sends") or 0)
            clicks = int(r.get("clicks") or 0)
            opens = int(r.get("opens") or 0)
            removals = int(r.get("removals") or 0)
            ca = float(r.get("ca") or 0.0)

            adv_data["sends_total"] += sends
            adv_data["clicks_total"] += clicks
            adv_data["opens_total"] += opens
            adv_data["removals_total"] += removals
            adv_data["ca_max"] = max(adv_data["ca_max"], ca)

            dims_map = {
                "civilite": r.get("valuedimension") if r.get("dimensions") == "civilite" else "O_gender",
                "age_range": r.get("valuedimension") if r.get("dimensions") == "age_range" else "O_age",
                "isp": r.get("valuedimension") if r.get("dimensions") == "isp" else "O_isp"
            }

            
            for dim_name, val in dims_map.items():
                if val not in adv_data["dimensions"][dim_name]:
                    adv_data["dimensions"][dim_name][val] = {
                        "sends": sends,
                        "clicks": clicks,
                        "opens": opens,
                        "removals": removals
                    }
                else:
                    metrics = adv_data["dimensions"][dim_name][val]
                    metrics["sends"] += sends
                    metrics["clicks"] += clicks
                    metrics["opens"] += opens
                    metrics["removals"] += removals

                
                metrics = adv_data["dimensions"][dim_name][val]
                opens_dim = metrics['opens']
                taux_clicks = round(metrics["clicks"] / metrics["sends"] * 100, 2) if metrics["sends"] else 0.0
                taux_cto = round(metrics["clicks"] / metrics["opens"] * 100, 2) if metrics["opens"] else 0.0
                taux_unsubs = round(metrics["removals"] / metrics["sends"] * 100, 2) if metrics["sends"] else 0.0

                analyses = {
                    "taux_clicks": self.analyze.analyze_click_rate(taux_clicks),
                    "taux_cto": self.analyze.analyze_cto_rate(taux_cto, opens_dim),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(taux_unsubs)
                }
                metrics['analyse'] = analyses

            combo_key = f"{dims_map['age_range']}_{dims_map['civilite']}_{dims_map['isp']}"
            if combo_key not in adv_data["dimensions"]["age_civilite_isp"]:
                adv_data["dimensions"]["age_civilite_isp"][combo_key] = {
                    "sends": sends,
                    "clicks": clicks,
                    "opens": opens,
                    "removals": removals
                }
            else:
                metrics = adv_data["dimensions"]["age_civilite_isp"][combo_key]
                metrics["sends"] += sends
                metrics["clicks"] += clicks
                metrics["opens"] += opens
                metrics["removals"] += removals

                
                metrics_combo = adv_data["dimensions"]["age_civilite_isp"][combo_key]
                opens_combo = metrics_combo['opens']
                taux_clicks_combo = round(metrics_combo["clicks"] / metrics_combo["sends"] * 100, 2) if metrics_combo["sends"] else 0.0
                taux_cto_combo = round(metrics_combo["clicks"] / metrics_combo["opens"] * 100, 2) if metrics_combo["opens"] else 0.0
                taux_unsubs_combo = round(metrics_combo["removals"] / metrics_combo["sends"] * 100, 2) if metrics_combo["sends"] else 0.0

                metrics_combo['analyse'] = {
                    "taux_clicks": self.analyze.analyze_click_rate(taux_clicks_combo),
                    "taux_cto": self.analyze.analyze_cto_rate(taux_cto_combo, opens_combo),
                    "taux_unsubs": self.analyze.analyze_unsub_rate(taux_unsubs_combo)
                }

        sends_total = adv_data["sends_total"]
        clicks_total = adv_data["clicks_total"]
        opens_total = adv_data["opens_total"]
        removals_total = adv_data["removals_total"]
        ca_max = adv_data["ca_max"]

        adv_data['ecpm'] = round((ca_max / sends_total * 1000), 2) if sends_total else 0.0
        adv_data['taux_clicks'] = round((clicks_total / sends_total * 100), 2) if sends_total else 0.0
        adv_data['taux_cto'] = round((clicks_total / opens_total * 100), 2) if opens_total else 0.0
        adv_data['taux_desabo'] = round((removals_total / sends_total * 100), 2) if sends_total else 0.0
        adv_data["classification"] = self.analyze.classify_advertiser(adv_data['ecpm'], adv_data['taux_clicks'])
        result["advertisers"] = adv_data

        return result

    