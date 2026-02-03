import pandas as pd
from datetime import datetime
import numpy as np
import hashlib
from config.ClickHouseConfig import ClickHouseConfig

class Contacts:
    def __init__(self, client=None, table_name="contacts"):
        """
        Classe pour gérer les contacts dans ClickHouse.
        :param client: clickhouse_driver.Client
        :param table_name: nom de la table ClickHouse
        """
        self.client = client if client else ClickHouseConfig().getClient_loc()
        self.table_name = table_name


    def insert_dataframe(self, df):
        try:
            if df.empty:
                return
            # compter les lignes avant insertion
            self.client.insert_df(self.table_name, df)
        except Exception as e:
            print("error is ", e)

    
    def optimize(self):
        try:
            query = f"""
                OPTIMIZE TABLE planifik.contacts FINAL;
            """
            self.client.command(query)
        except Exception as e:
            print("error optimizing ", e)
            pass
            


    # -------------------------
    # Lire contacts
    # -------------------------
    def read_all(self):
        query = f"SELECT * FROM {self.table_name}"
        return self.client.execute(query)

    # -------------------------
    # CRUD : Update optin pour désabonnés
    # -------------------------
    def mark_unsubscribed(self, current_ids: list):
        """
        Marque optin = 0 pour tous les contacts qui ne sont pas dans current_ids
        """
        # Requête INSERT pour ReplacingMergeTree
        query = f"""
        INSERT INTO {self.table_name} (id, email, optin, updated_at)
        SELECT id, email, 0 AS optin, now()
        FROM {self.table_name}
        WHERE id NOT IN %(ids)s
        """
        self.client.execute(query, {'ids': current_ids})


    def delete_by_id(self, ids: list):
        """
        Supprime des contacts par id
        """
        query = f"ALTER TABLE {self.table_name} DELETE WHERE id IN %(ids)s"
        self.client.execute(query, {'ids': ids})

    def clean_contacts_df(self,df):
        string_cols = [
        "dwh_id", "isp", "main_isp", "email", "email_md5", "email_sha256",
        "firstname", "lastname", "birthdate", "civility", "gender",
        "zipcode", "city", "dep", "mobile", "telephone_fixe",
        "categorie", "optin_email", "optin_sms", "delivery"
        ]

        datetime_cols = [
        "date_last_sent", "date_last_open", "date_last_click",
        "subscription_date", "recency", "updated_at"
        ]

        uint8_cols = ["age"]                
        uint64_cols = ["database_id"]        

        float_cols = [
        "density_per_km2", "score_of_landlords", "score_of_individual_houses",
        "score_median_income", "score_of_tax_house_holds", "score_poverty",
        "csp_score"
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        for col in uint8_cols:
            if col in df.columns:
                df[col] = (
                df[col]
                .replace("", np.nan)
                .fillna(np.nan)
                .astype("Float64")
                .round()
                .astype("Int64")
            )
        for col in uint64_cols:
            if col in df.columns:
                df[col] = (
                df[col]
                .fillna(0)
                .astype("Int64")
            )
        for col in float_cols:
            if col in df.columns:
                df[col] = (
                df[col]
                .replace("", np.nan)
                .astype("float32")
            )
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce").fillna(pd.Timestamp.utcnow())

        """df.to_csv(
            "contacts.csv",
            sep="|",
            index=False,
            encoding="utf-8"
        )"""

        return df
