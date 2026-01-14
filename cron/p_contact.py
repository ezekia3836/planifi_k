from models.Databases import Database
from config.ClickHouseConfig import ClickHouseConfig
from config.config import Config as config
import paramiko, time, gc, os
import pandas as pd
from utils.parse_mobile import *
from datetime import datetime
import hashlib
from models.Contacts import Contacts
from concurrent.futures import ThreadPoolExecutor, as_completed
from gcs.gcs import gcs
gcs = gcs('plannifik')
contact = Contacts()
class p_contact():

    def __init__(self):
        self.clk = ClickHouseConfig().getClient()
        self.db_model = Database(client=self.clk)
        self.TEMP_SL = "temp_sl"
        os.makedirs(self.TEMP_SL, exist_ok=True)
        self.chuncksize = 500000
        self.prefix="contacts"
        self.path=f"dev/{self.prefix}"
        self.sep = '|'
        self.contact_model = Contacts(client=self.clk)
        self.LISTE_FEMALE_GENDER = ["Fru",'Mrs.','Ms.','Mme','MME','Mlle','Sig.ra','MRS','Ms','Mlle.','Mademoiselle','Madame','Madmoiselle','F','Femme','Femme.','Frau','Frau.','Frau Dr.','Frau Dr', 'Dame', 'Dame.']
        self.LISTE_MALE_GENDER = ["Sr",'Mr.','Herr','Sig.','M','D','MR','MR.','Dhr.','M.','Mister','Monsieur','Mr','MR.','MISTER']
        

    def download_sftp_folder(self, db, max_retries=3, delay_seconds=5):
        attempt = 0
        while attempt < max_retries:
            try:
                transport = paramiko.Transport((config.SFTP_SEGMENT['host'], 22))
                transport.connect(
                    username=config.SFTP_SEGMENT['user'],
                    password=config.SFTP_SEGMENT['password']
                )

                sftp = paramiko.SFTPClient.from_transport(transport)

                sftp.chdir(config.SFTP_SEGMENT['remote_dir'])
                files = sftp.listdir()

                target_file = f"{db['stats_id']}.csv"

                if target_file in files:
                    print(f"Téléchargement de : {target_file}")
                    local_path = os.path.join(self.TEMP_SL, target_file)
                    sftp.get(target_file, local_path)
                sftp.close()
                transport.close()
                return local_path

            except Exception as e:
                attempt += 1
                print(f"[{attempt}/{max_retries}] Erreur lors du téléchargement : {e}")

                if attempt < max_retries:
                    print(f"Nouvelle tentative dans {delay_seconds} secondes...")
                    time.sleep(delay_seconds)
                else:
                    print("Téléchargement abandonné après plusieurs échecs.")
                return None

    def process_chunk(self, df, db):
        try:
            initial = len(df)
            df = df.rename(columns={
                "lastclickemail": "date_last_click",
                "lastopenemail": "date_last_open",
                "lastemail": "date_last_sent",
                "subscriptiondate": 'subscription_date',
                "city_ville": "city",
            })
            df['database_id'] = db['id']
            df['updated_at'] = datetime.now()
            df = strip_email_column(df)
            for col in ['firstname', 'zipcode','city']:
                if col in df.columns:
                    if col == "zipcode":
                        df[col] = df[col].apply(cleanZipcode)
                        df['dep'] = df[col].apply(lambda x: x[:2])
                    else:
                        df[col] = df[col].apply(cleanText)
            cleaned_columns = ["email","firstname","lastname"]
            for cols in cleaned_columns:
                df = clean_column(df, cols)
            df = categorize_users(df) 
            datetime_cols = ['date_last_sent', 'date_last_open', 'date_last_click', 'subscription_date', 'recency']
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # remplacer NaT par une date par défaut
                    df[col] = df[col].fillna(pd.Timestamp('1970-01-01 00:00:00'))
            df['email_md5'] = df['email'].map(lambda x: md5(x.encode()).hexdigest() if pd.notna(x) and x else '')
            df['email_sha256'] = df['email'].map(lambda x: sha256(x.encode()).hexdigest() if pd.notna(x) and x else '')
            df['dwh_id'] = df['email'].map(lambda email: generate_id(db['id'], email, "dggf?s025mPMjdx-mMnFv") if pd.notna(email) and email else '')
            df = add_age_column(df, 'birthdate')
            df = add_main_isp_column(df, 'email')
            df = makeMobile(df, "mobile", "33")
            df = makeMobile(df, "telephone_fixe", "33")
            
            gender_map = {}
            gender_map.update({c: 'F' for c in self.LISTE_FEMALE_GENDER})
            gender_map.update({c: 'M' for c in self.LISTE_MALE_GENDER})
            df['gender'] = df['civility'].apply(lambda x: gender_map.get(str(x).strip(), 'O') if pd.notna(x) and str(x).strip() != '' else 'O')
            df['optin_email'] = "1"
            df['optin_sms'] = "1"
            df['delivery'] = "delivery_ok"
            df = assign_scores(df)
            try:
                df = df[['dwh_id','isp','main_isp','email','email_md5', 'email_sha256', 'firstname', 'lastname',
                    'birthdate', 'civility', 'gender','zipcode', 'city', 'dep',  'age', 'mobile', 
                    'telephone_fixe',  'date_last_sent', 'date_last_open','date_last_click', 
                    'subscription_date', 'categorie', 'recency', 'optin_email','optin_sms','delivery','density_per_km2', 
                    'score_of_landlords', 'score_of_individual_houses', 
                    'score_median_income', 'score_of_tax_house_holds', 'score_poverty', 'csp_score', 'database_id', 'updated_at'
                ]]
                str_cols = ["zipcode", "mobile", "telephone_fixe", "email", "firstname", "lastname", "city", "civility", "gender", "delivery"]
                for col in str_cols:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) else None)
                df['birthdate'] = df['birthdate'].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None)
                # Colonnes DateTime → datetime Python natif
                date_cols = ["date_last_sent", "date_last_open", "date_last_click", "subscription_date", "recency"]
                for col in date_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
                # Exemple pour UInt8
                # df['age'] = pd.to_numeric(df['age'], errors='coerce')
                # df['age'] = df['age'].fillna(0).astype('UInt8')

                df['age'] = pd.to_numeric(df['age'], errors='coerce')
                df.loc[df['age'] < 0, 'age'] = 0
                df['age'] = df['age'].fillna(0).astype('UInt32')
                string_cols = [
                    'dwh_id','isp','main_isp','email','email_md5','email_sha256',
                    'firstname','lastname','civility','gender','zipcode','city','dep',
                    'mobile','telephone_fixe','categorie','optin_email','optin_sms','delivery'
                ]
                for col in string_cols:
                    if col in df.columns:
                        df[col] = df[col].astype(str).replace('nan','').fillna('')
                datetime_cols = [
                    'date_last_sent','date_last_open','date_last_click','subscription_date','recency','updated_at'
                ]
                for col in datetime_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
                df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype('UInt8')
                df['age'] = df['age'].fillna(0)
                df['age'] = df['age'].clip(0, 255).astype('UInt32')
                float_cols = [
                    'density_per_km2','score_of_landlords','score_of_individual_houses',
                    'score_median_income','score_of_tax_house_holds','score_poverty','csp_score'
                ]
                for col in float_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('Float32')
                df['database_id'] = pd.to_numeric(df['database_id'], errors='coerce').fillna(0).astype('UInt64')
                print(initial, ' ---> ', len(df))
            except Exception as e:
                print(e)
                #df.to_csv(f'erreur.csv', index=False, sep='|')
                #pass"""
            df_clean=contact.clean_contacts_df(df)
            gcs.upload_to_gcs(chunk_size=self.chuncksize,prefix=self.prefix,df=df_clean,path_gcs=self.path)
                #self.contact_model.insert_dataframe(df)"""
            gcs.insert_into_clickhouse(prefix=self.path,bucket_name='plannifik',table=self.prefix)
            gcs.delete_data_bucket(prefix=self.path)
        except Exception as e:
            print('error processing chunk ',e)
            pass

    def process_contact(self, db):
        try:
            today = datetime.now()
            path = self.download_sftp_folder(db)
            if path:
                dfs = pd.read_csv(path, low_memory=False, sep=self.sep, dtype={"mobile": str, "zipcode": str, "telephone_fixe": str}, chunksize=self.chuncksize)
                dir_temp = str(db["acronyms"])
                fullpath = os.path.join(self.TEMP_SL, dir_temp)
                os.makedirs(fullpath, exist_ok=True)
                for i, df in enumerate(dfs, start=1):
                    self.process_chunk(df, db)
                    del df
                # os.remove(path)
                return fullpath
        except Exception as e:
            print("Error processing contact:", e)
            return None

    def process_activities(self, db):
        try:
            pass
        except Exception as e:
            print('error processing activities ', e)
            pass

    def start_contact(self):
        db_liste = self.db_model.read_all()
        for db in db_liste:
            self.process_contact(db)
            break
        self.contact_model.optimize()
