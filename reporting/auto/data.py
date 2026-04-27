from config.ClickHouseConfig import ClickHouseConfig
from reporting.analyze import analyse
from collections import defaultdict
class data_auto:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.analyse = analyse()
        self.table = "prod_reporting_test"

    def _execute_query(self,query,params=None):
        result = self.clk.query(query,parameters=params or {})
        return[
            dict(zip(result.column_names,r)) for r in result.result_rows
        ]

    def get_data(self,adv_id,top_n=3):
        def calcul_score(ctr,taux_openers,taux_unsubs):
            return(ctr*0.5) + (taux_openers*0.3)-(taux_unsubs*0.8)
        def calcul_metrics(a,b):
            return (a/b)*100 if b else 0.0
        query = f"""
               SELECT
                r.adv_id,
                r.database_id,
                r.age_range AS age,
                r.gender AS gender,
                r.main_isp AS isp,
                t.tag AS tag,
                cu.name AS currency,
                c.dwh_name AS country,
                toDayOfWeek(r.ds_parsed) AS day,
                toMonth(r.ds_parsed) AS month,
                toHour(r.date_event) AS hour,
                SUM(r.sends) AS sends,
                SUM(r.openers) AS openers,
                SUM(r.clickers) AS clickers,
                SUM(r.unsubs) AS unsubs
            FROM (
                SELECT
                    r.*,
                    parseDateTimeBestEffort(arrayJoin(r.date_schedule)) AS ds_parsed
                FROM {self.table} r
                WHERE r.adv_id IN ({adv_id})
            ) AS r
            LEFT JOIN tags t ON r.tag_id = t.id
            LEFT JOIN country c ON r.country = c.id
            LEFT JOIN currency cu ON cu.id = c.id_currency
            GROUP BY
                r.adv_id,
                r.database_id,
                age,
                gender,
                isp,
                tag,
                country,
                currency,
                day,
                hour,
                month
            
        """
        rows = self._execute_query(query)
        bases={}
        tags={}
        segments={
            "age":{},
            "gender":{},
            "isp":{}
        }
        temps={
            "hour":{},
            "day":{},
            "month":{}
        }

        for r in rows:
            sends = r["sends"] or 0
            clickers = r["clickers"] or 0
            openers=r["openers"] or 0
            unsubs=r["unsubs"] or 0
            taux_clickers = calcul_metrics(clickers,sends)
            taux_openers = calcul_metrics(openers,sends)
            taux_unsubs = calcul_metrics(unsubs,sends)
            score = calcul_score(taux_clickers,taux_openers,taux_unsubs)
            base=r["database_id"]
            b = bases.setdefault(base,{
                "database_id":base,
                "country": r.get("country"),
                "currency": r.get("currency"),
                "sends":0,
                "clickers":0,
                "openers":0,
                "unsubs":0,
                "taux_clickers":0,
                "taux_openers":0,
                "taux_unsubs":0,
                "score":0
            })
            b["sends"]+=sends
            b["clickers"]+=clickers
            b["openers"]+=openers
            b["unsubs"]+=unsubs
            b["taux_clickers"]=calcul_metrics(b["clickers"],b["sends"])
            b["taux_openers"]=calcul_metrics(b["openers"],b["sends"])
            b["taux_unsubs"] = calcul_metrics(b["unsubs"],b["sends"])
            b["score"]+=score

            tg = r["tag"]
            t = tags.setdefault(tg,{
                "tag": r.get("tag"),
                "sends":0,
                 "clickers":0,
                "openers":0,
                "unsubs":0,
                "taux_clickers":0,
                "taux_openers":0,
                "taux_unsubs":0,
                "score":0})
            t["sends"]+=sends
            t["clickers"]+=clickers
            t["openers"]+=openers
            t["unsubs"]+=unsubs
            t["taux_clickers"]=calcul_metrics(t["clickers"],t["sends"])
            t["taux_openers"]=calcul_metrics(t["openers"],t["sends"])
            t["taux_unsubs"] = calcul_metrics(t["unsubs"],t["sends"])
            t["score"]+=score

            def add_segment(dim,key):
                if not key:
                    return 
                s = segments[dim].setdefault(key,{
                    "value":key,
                    "sends":0,
                    "clickers":0,
                    "openers":0,
                    "unsubs":0,
                    "taux_clickers":0,
                    "taux_openers":0,
                    "taux_unsubs":0,
                    "score":0
                })
                s["sends"]+=sends
                s["clickers"]+=clickers
                s["openers"]+=openers
                s["unsubs"]+=unsubs
                s["taux_clickers"]=calcul_metrics(s["clickers"],s["sends"])
                s["taux_openers"]=calcul_metrics(s["openers"],s["sends"])
                s["taux_unsubs"] = calcul_metrics(s["unsubs"],s["sends"])
                s["score"]+=score
                
            add_segment("age",r.get("age"))
            add_segment("gender",r.get("gender"))
            add_segment("isp",r.get("isp"))

            def add_time(dim, key):
                if key is None:
                    return
                temps[dim][key] = temps[dim].get(key, 0) + score

            add_time("day", r.get("day"))
            add_time("hour", r.get("hour"))
            add_time("month", r.get("month"))
            def top_items(items):
                items = [i for i in items.values() if i["sends"]>=100]
                items.sort(key=lambda x:x["score"],reverse=True)
                return items[:top_n] if len(items)>=top_n else items
            
            def format_time(d):
                return sorted(
                    [{"value": k, "score": v} for k, v in d.items()],
                    key=lambda x: x["score"],
                    reverse=True
                )[:top_n] if len(d) >= top_n else \
                [{"value": k, "score": v} for k, v in d.items()]
            
        return {
        "advertiser_id": str(adv_id),

        "candidates": {
            "bases": top_items(bases),
            "tags": top_items(tags),

            "segments": {
                "age": top_items(segments["age"]),
                "gender": top_items(segments["gender"]),
                "isp": top_items(segments["isp"])
            }
        },

        "time_optimization": {
            "day": format_time(temps["day"]),
            "hour": format_time(temps["hour"]),
            "month": format_time(temps["month"])
        },

        "constraints": {
            "max_unsub_rate": 0.2
        },

        "objective": {
            "primary": ["ctr"],
            "secondary": ["open_rate"],
            "penalty": ["unsub_rate"]
        }
    }
