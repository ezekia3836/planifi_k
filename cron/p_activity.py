from models.Databases import Database
from config.config import Config as config
from config.ClickHouseConfig import ClickHouseConfig
from models.Focus import Focus
from datetime import datetime, timedelta
import pandas as pd
import json, time, requests
from io import StringIO
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.parse_mobile import *
from models.Events import Events
import numpy as np
import os 
from dateutil.relativedelta import relativedelta
from google.cloud import storage
from gcs.gcs import gcs
from models.Events import Events as events
gcs = gcs('plannifik')

class p_activity():

    def __init__(self):
        self.clk = ClickHouseConfig().getClient()
        self.db_model = Database(client=self.clk)
        self.focus_model = Focus(config.FOCUS_CONFIG)
        self.events_model = Events(client=self.clk)
        self.prefix="events"
        self.path=f"dev/{self.prefix}"
        self.chunksize=200000
        self.history_temp = 'history'
        # self.activities = ['Clicks','Opens','Removals', 'Complaints','Bounce','Subscriptions', 'Sends']
        self.activities = ['Sends','Clicks','Opens','Removals','Complaints']

    def fetch_activities(self, db_info, event, start, end):
        try:
            delta_days = (end - start).days + 1
            date_list = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta_days)]
            def fetch_single_date(date_str):
                max_attempts = 10
                for attempt in range(1, max_attempts + 1):
                    try:
                        url = f"{db_info['api_url']}Api/Activities?apiKey={db_info['api_key']}&date={date_str}&type={event}"
                        print(url)
                        response = requests.get(url, timeout=30)
                        if response.status_code == 200 and response.text.strip():
                            df = pd.read_csv(StringIO(response.text),  dtype={"MessageID": str})
                            df = df.rename(columns={"ï»¿Date": "Date"})
                            if len(df) > 0:
                                return df
                            else:
                                return None
                        else:
                            pass
                    except Exception as e:
                        pass
                    time.sleep(0.5) 
                print(f"[{date_str}] Échec après {max_attempts} tentatives.")
                return None
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(fetch_single_date, date_list))
            all_data = [df for df in results if df is not None]
            dd = pd.concat(all_data, ignore_index=True)
            return dd
        except Exception as e:
            print('error fetching activities  ', e)

    def start_activities(self):
        try:
            db_liste = self.db_model.read_all()
            json_file = "historiques.json"
            alls = []
            if os.path.exists(json_file) and os.path.getsize(json_file) > 0:
                with open(json_file, "r", encoding="utf-8") as f:
                    contents = json.load(f)
            else:
                contents = []

            for event in self.activities:
                print(f"Event : {event}")

                for db in db_liste:
                    db_id = db["id"]
                    nbre_jour = 5

                    historique = next((c for c in contents if c.get("base_id") == db_id),None)
                    if historique is None:
                        historique = {"base_id": db_id}
                        contents.append(historique)
                    if historique.get(event, 0) == 1:
                        nbre_jour = 1
                    historique[event] = 1
                    date_end = datetime.now()
                    date_start = date_end - timedelta(days=nbre_jour)
                    data = self.focus_model.extract_data(date_start, date_end, db)
                    if not data:
                        continue
                    df_router = pd.DataFrame(json.loads(data))
                    if df_router.empty:
                        continue
                    df_router = df_router[['id_router', 'tag', 'adv_id', 'brand']].copy()
                    df_router = df_router.rename(columns={'id_router': 'MessageId'})
                    df_router['MessageId'] = pd.to_numeric(df_router['MessageId'], errors='coerce').fillna(0).astype(int)

                    df_router['adv_id'] = pd.to_numeric(df_router['adv_id'], errors='coerce').fillna(0).astype(int)
                    df_event = self.fetch_activities(db_info=db,event=event,start=date_start,end=date_end)
                    if df_event is None or df_event.empty:
                        continue
                    df_event['dwh_id'] = df_event['Email'].map(lambda email: generate_id(db['id'], email, "dggf?s025mPMjdx-mMnFv") if pd.notna(email) and email else '')

                    df_event['event_type'] = event
                    df_event['removals_raison'] = (
                        df_event['Reason'] if 'Reason' in df_event.columns else ""
                    )

                    if 'MessageId' not in df_event.columns:
                        df_event['MessageId'] = 0

                    df_event = df_event.rename(columns={'Date': 'date_event'})
                    df_event['MessageId'] = pd.to_numeric(df_event['MessageId'], errors='coerce').fillna(0).astype(int)

                    df_event['database_id'] = db_id
                    df_event['date_event'] = pd.to_datetime(df_event['date_event'], errors='coerce')

                    df_event = df_event[
                        ['dwh_id', 'event_type', 'removals_raison',
                        'MessageId', 'date_event', 'database_id']
                    ]
                    df_event = df_event.merge(
                        df_router,
                        on='MessageId',
                        how='left'
                    )

                    alls.append(df_event)
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(contents, f, indent=4)

            print("Historiques sauvegardées")

            if not alls:
                print("Aucune donnée collectée")
                return
            ds = pd.concat(alls, ignore_index=True)
            df_clean = events.clean_events_df(ds)
            gcs.upload_to_gcs(chunk_size=self.chunksize,prefix=self.prefix,df=df_clean,path_gcs=self.path)
            gcs.insert_into_clickhouse(prefix=self.path,bucket_name='plannifik',table=self.prefix)
            gcs.delete_data_bucket(prefix=self.path)
            print("Activities traitées avec succès")

        except Exception as e:
            print("error activities:", e)

