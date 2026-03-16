from datetime import datetime
from config.ClickHouseConfig import ClickHouseConfig as conn
class dataAgent:
    def __init__(self):
        self.clk = conn().getClient_prod()
        self.table="dev_reporting_agg"
    def _execute_query(self,query,params=None):
        result = self.clk.query(query,parameters=params or {})
        return[
            dict(zip(result.column_names,r)) for r in result.result_rows
        ]
    def get_advertisers(self):
        current_month = datetime.now().month+7
        query = f""" 
        SELECT DISTINCT adv_id FROM {self.table} dev ARRAY JOIN dev.date_schedule AS ds 
        WHERE toMonth(parseDateTimeBestEffort(ds))=%(month)s"""
        rows = self._execute_query(query,params={"month":current_month})
        return [r['adv_id'] for r in rows]
    
    def get_reporting_data(self, adv_ids):
        ids_str = ", ".join(str(i) for i in adv_ids)
        query = f"""
        SELECT
            r.adv_id,
            r.database_id,
            r.age_gender_isp AS segment,
            t.tag,
            toDayOfWeek(parseDateTimeBestEffort(ds)) AS day,
            toHour(r.date_event) AS hour,
            SUM(r.sends) AS sends,
            SUM(r.openers) AS openers,
            SUM(r.clickers) AS clickers,
            SUM(r.unsubs) AS unsubs
        FROM {self.table} r
        ARRAY JOIN r.date_schedule AS ds
        LEFT JOIN tags t ON r.tag_id = t.id
        WHERE r.adv_id IN ({ids_str})
        GROUP BY
            r.adv_id,
            r.database_id,
            segment,
            tag,
            day,
            hour
        """
        return self._execute_query(query)