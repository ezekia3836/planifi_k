from config.ClickHouseConfig import ClickHouseConfig
from config.config import Config
from models.Databases import Database
from models.Tags_advertiser import TagsAdvertiser
import requests
import os
import json
import pandas as pd

class p_tags:
    def __init__(self, config=None, path_file=None):
        self.config = config
        self.url_old = "https://stats.kontikimedia.com/publicapi"
        self.url = "https://stats.kontikimedia.com/publicapi/statsapi"
        self.url_es = "https://stats.kontikimedia.com/publicapi/esapi"
        self.clk = ClickHouseConfig().getClient()
        self.db_model = Database(client=self.clk)
        self.tags_model = TagsAdvertiser(client=self.clk)
        #self.url_stats_sms = "https://stats.kontikimedia.com/publicapi/cmapi"
        self.timeout = 10
        self.path_tags ='./Tags/stats_tags.txt'
        #self.path_country = path_file +  '/Utilities/stats_backup/stats_country.txt'
        #self.path_advertiser = path_file + '/Utilities/stats_backup/stats_advertiser.txt'
        #self.path_models =  path_file + '/Utilities/stats_backup/stats_models.txt'
        #self.path_clients =  path_file + '/Utilities/stats_backup/stats_clients.txt'
        self.stats_update = 0
        self.stats_data = []
        self.table_name="tags"
 
    def getListTags(self, apikey):
        try:
            if os.path.exists(self.path_tags):
                try:
                    with open(self.path_tags, 'r') as fic:
                        return json.load(fic)
                except Exception:
                    os.remove(self.path_tags)
            r = requests.post(self.url + "/gettags", data={"userapikey": apikey}, timeout=self.timeout)
            try:
                result = r.json()
            except Exception:
                self.logger.error(f"API returned non-JSON data: {r.text}")
                return []
            if isinstance(result, list) and result and 'auth' in result[0]:
                return []
            newlist = sorted(result, key=lambda d: d['tag'])

            with open(self.path_tags, 'w') as fic:
                json.dump(newlist, fic)

            return newlist

        except Exception as e:
            self.logger.error(f"error for getting tags list : {e}")
            return []
    def startGetTags(self):
        nombre=self.tags_model.verifier_table(self.table_name)
        if nombre >0:
            self.tags_model.vider_table(self.table_name)
        apikey=Config.TAGS_CONF["apikey"]
        tags = self.getListTags(apikey)
        df = pd.DataFrame(tags)
        self.tags_model.insert_dataframe(self.table_name,df)
        print("Insertion tags terminée!!")