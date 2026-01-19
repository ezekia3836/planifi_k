import pandas as pd
from config.ClickHouseConfig import ClickHouseConfig
from collections import defaultdict
import json
from reporting.analyze import analyse
from fastapi import HTTPException
class TagsAdvertiser:
    def __init__(self, client=None):
        """
        Classe pour gérer les tags et les advertisers dans ClickHouse.
        :param client: clickhouse_driver.Client
        """
        self.client = client if client else ClickHouseConfig().getClient()
        self.table1="tags"
        self.table2="advertiser"
        self.table3 = "reporting"
        self.analyze=analyse()
    def insert_dataframe(self,table_name, df):
        try:
            if df.empty:
                return
            self.client.insert_df(table_name, df)
        except Exception as e:
            print("error is ", e)

    def vider_table(self,table_name):
        try:
            query = f"TRUNCATE TABLE planifik.{table_name}"
            self.client.command(query)
            print("Table vide")
        except Exception as e:
            print("erreur",e)

    def verifier_table(self,table_name):
        try:
            query = f"SELECT COUNT() FROM planifik.{table_name}"
            res=self.client.query(query)
            nb = res.result_rows[0][0]
            return nb
        except Exception as e:
            print("erreur",e)
#tags
    def read_tags(self,):
        try:
            query = f"SELECT id,tag,dwtag FROM {self.table1}"
            result = self.client.query(query)
            rows=result.result_rows
            columns = result.column_names
            tags = [
               dict(zip(columns,row)) for row in rows
            ]
            return tags
        except Exception as e:
            print("erreur",e)
    def get_tags_byId(self,id):
        try:
            query = f"SELECT tag,dwtag FROM {self.table1} WHERE id={id}"
            result=self.client.query(query,parameters={"id":id})
            row=result.result_rows[0]
            columns=result.column_names
            return {
                columns[i]:row[i] for i in range(len(columns))
            }
        except Exception as e:
            print("erreur",e)

#advertiser
    def read_advertiser(self):
        try:
            query=f"SELECT id,name,desabled,created_at FROM {self.table2}"
            result=self.client.query(query)
            rows=result.result_rows
            columns = result.column_names
            advertisers = [
            dict(zip(columns, row))
            for row in rows
        ]
            return advertisers
        except Exception as e:
            print("erreur",e)

    def get_advertiser_byid(self,id):
        try:
            query =f"SELECT * FROM {self.table2} WHERE id={id}"
            result = self.client.query(query, parameters={"id":id})
            row = result.result_rows[0]
            columns=result.column_names
            return {
                columns[i]:row[i] for i in range(len(columns))
                }
        except Exception as e:
            print("erreur",e)

    def search_advertiser(self,keywords:str):
        try:
            query=f"SELECT * FROM {self.table2} WHERE position(name,%(kw)s)>0 LIMIT 100"
            result= self.client.query(query,parameters={"kw":keywords})
            return [
                dict(zip(result.column_names,row)) for row in result.result_rows
            ]
        
        except Exception as e:
            print("erreur",e)
#report
    def reporting(self,router,adv):
        try:
            query = """
                SELECT id_routers,database_id,adv_id,tag_name,data
                FROM reporting
                WHERE id_routers = %(router)s 
                AND adv_id=%(adv)s
                ORDER BY create_at DESC
            """
            result = self.client.query(query, parameters={"router": router,"adv":adv})
            return [dict(zip(result.column_names, row)) for row in result.result_rows]
          
        except Exception as e:
            print('router',e)

    def safe_str(self,s):
        if isinstance(s, bytes):
            return s.decode('utf-8', errors='replace')
        elif isinstance(s, str):
            return s.encode('utf-8', errors='replace').decode('utf-8')
        else:
            return s

    def report_advertiser(self, advertiser):
        try:
            query = """SELECT adv_id,advertiser_name,database_id FROM reporting WHERE adv_id=%(adv)s AND id_routers=0 AND database_id=0"""
            result = self.client.query(query, parameters={"adv": advertiser})
            rows = []
            for row in result.result_rows:
                rows.append({k: self.safe_str(v) for k, v in zip(result.column_names, row)})
            return rows
        except Exception as e:
            print('advertiser', e)
            return []
    def report_base(self, db_id,adv):
        try:
            query = """
                SELECT adv_id, advertiser_name, dimension, dim_content, sends, clicks, opens, removals, ca
                FROM reporting1
                WHERE database_id = %(db_id)s AND adv_id=%(adv)s
            """
            result = self.client.query(query, parameters={"db_id": db_id, "adv":adv})
            rows = [dict(zip(result.column_names, row)) for row in result.result_rows]

            advertisers_dict = {}
            dimensions_dict = {}

            for r in rows:
                adv_id = r['adv_id']
                adv_name = r['advertiser_name']

                if adv_id not in advertisers_dict:
                    total_sends = r['sends']
                    total_clicks = r['clicks']
                    total_ca = r['ca']

                    ecpm = (total_ca / total_sends * 1000) if total_sends else 0.0
                    taux_clickers = (total_clicks / total_sends * 100) if total_sends else 0.0

                    advertisers_dict[adv_id] = {
                        "name": adv_name,
                        "classe": self.analyze.classify_advertiser(ecpm, taux_clickers)
                    }

                dim = r['dimension']
                content = r['dim_content']

                if dim not in dimensions_dict:
                    dimensions_dict[dim] = {}

                if content not in dimensions_dict[dim]:
                    dimensions_dict[dim][content] = {
                        "sends": 0,
                        "clicks": 0,
                        "opens": 0,
                        "removals": 0,
                        "ca": 0
                    }

                dimensions_dict[dim][content]['sends'] += r['sends']
                dimensions_dict[dim][content]['clicks'] += r['clicks']
                dimensions_dict[dim][content]['opens'] += r['opens']
                dimensions_dict[dim][content]['removals'] += r['removals']
                dimensions_dict[dim][content]['ca'] += r['ca']

            for dim_vals in dimensions_dict.values():
                for val, metrics in dim_vals.items():
                    for k in metrics:
                        metrics[k] = float(metrics[k])

            return {
                "base_id": db_id,
                "advertisers": {
                    "analyses": list(advertisers_dict.values())
                },
                "dimensions": dimensions_dict
            }

        except Exception as e:
            print("Erreur report_base:", e)
            return {
                "base_id": db_id,
                "advertisers": {"analyses": []},
                "dimensions": {}
            }
