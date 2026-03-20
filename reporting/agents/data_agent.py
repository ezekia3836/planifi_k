from datetime import datetime
from config.ClickHouseConfig import ClickHouseConfig as conn
class dataAgent:
    def __init__(self):
        self.clk = conn().getClient_prod()
        self.table="prod_reporting"
    def _execute_query(self,query,params=None):
        result = self.clk.query(query,parameters=params or {})
        return[
            dict(zip(result.column_names,r)) for r in result.result_rows
        ]
   

    def get_advertisers(self, months=6):
        now = datetime.now()
        months_years = []
        m, y = now.month, now.year
        for i in range(months):
            months_years.append((y, m))
            m -= 1
            if m == 0:
                m = 12
                y -= 1

        cond = " OR ".join([
            f"(toYear(parseDateTimeBestEffort(ds))={yr} AND toMonth(parseDateTimeBestEffort(ds))={mo})"
            for yr, mo in months_years
        ])

        query = f"""
        SELECT DISTINCT adv_id,
                        toYear(parseDateTimeBestEffort(ds)) AS year,
                        toMonth(parseDateTimeBestEffort(ds)) AS month
        FROM {self.table} dev
        ARRAY JOIN dev.date_schedule AS ds
        WHERE {cond}
        """
        rows = self._execute_query(query)
        adv_ids = list(set([r["adv_id"] for r in rows]))
        return adv_ids
    
    def get_reporting_data(self, adv_ids):
        if not adv_ids:
            return []
        ids_str = ", ".join(str(i) for i in adv_ids)

        query = f"""
        SELECT
            r.adv_id,
            r.database_id,
            r.age_range AS age,
            r.gender AS gender,
            r.main_isp AS isp,
            t.tag,
            cu.name AS currency,
            c.dwh_name AS country,
            toDayOfWeek(ds_parsed) AS day,
            toMonth(ds_parsed) AS month,
            toHour(r.date_event) AS hour,
            SUM(r.sends) AS sends,
            SUM(r.openers) AS openers,
            SUM(r.clickers) AS clickers,
            SUM(r.unsubs) AS unsubs
        FROM (
            SELECT
                r.*,
                parseDateTimeBestEffort(ds) AS ds_parsed
            FROM {self.table} r
            ARRAY JOIN r.date_schedule AS ds
            WHERE r.adv_id IN ({ids_str})
        ) AS r
        LEFT JOIN tags t ON r.tag_id = t.id
        LEFT JOIN country c ON r.country = c.id
        LEFT JOIN currency cu ON cu.id = c.id_currency
        GROUP BY
            r.adv_id, r.database_id, age, gender, isp, tag, country, currency, day, hour, month
        HAVING sends > 50
        """
        return self._execute_query(query)
    def best_send_time(self,adv_id,tag):
        query=f""" 
            SELECT r.adv_id, t.tag,
            toHour(r.date_event) AS hour,
            SUM(r.sends) AS sends,
            SUM(r.openers) AS openers,
            SUM(r.clickers) AS clickers,
            SUM(r.unsubs) AS unsubs
            FROM {self.table} r LEFT JOIN tags t ON r.tag_id = t.id WHERE r.adv_id=%(adv_id)s AND t.tag=%(tag)s 
            GROUP BY r.adv_id,t.tag,hour
         """
        return self._execute_query(query,params={"adv_id":adv_id,"tag":tag})