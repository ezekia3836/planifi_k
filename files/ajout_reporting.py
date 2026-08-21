import ctypes
import gc
from datetime import date, timedelta

import pandas as pd

from connexion import conn

str_cols = [
    "dwh_id", "subject", "brand", "id_routers", "ListName",
    "zipcode", "dep", "age_range", "gender", "main_isp",
]
uint_cols = [
    "sends", "opens", "clicks", "unsubs", "complaints", "bounces",
]
int_cols = [
    "database_id", "ktk_id", "country", "segmentId",
    "tag_id", "adv_id", "ListId",
]


def _release_memory():
    """Force la libération mémoire au niveau du process (glibc ne rend
    pas la RAM à l'OS spontanément après un gros pic pandas)."""
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


class AjoutReporting:

    def __init__(self):
        self.conn = conn().getClient()
        self.table = "reporting"
        self.batch_size = 30000
        self._databases_cache = None

    # ------------------------------------------------------------------
    # Cache databases
    # ------------------------------------------------------------------
    def _get_databases(self):
        if self._databases_cache is None:
            self._databases_cache = self.conn.query_df("""
                SELECT id, ktk_id, country
                FROM databases
            """)
        return self._databases_cache

    # ------------------------------------------------------------------
    # Ingestion paginée
    # ------------------------------------------------------------------
    def ajout(self, date_start, date_end):

        query = """
        SELECT
            p.database_id,
            p.dwh_id,
            p.SegmentId,
            p.MessageSubject,
            p.brand,
            p.tag,
            p.adv_id,
            p.MessageId,
            p.ListId,
            p.ListName,
            p.event_type,
            p.removals_raison,
            p.Date AS event_datetime
        FROM events_local p
        WHERE
            p.Date >= {date_start:DateTime}
            AND p.Date <= {date_end:DateTime}
            AND p.adv_id != 0
        """

        df = self.conn.query_df(
            query,
            parameters={
                "date_start": f"{date_start} 00:00:00",
                "date_end": f"{date_end} 23:59:59",
            },
        )

        print(f"{date_start} -> {date_end} : {len(df):,} lignes")

        if not df.empty:
            df = df.rename(columns={
                "SegmentId": "segmentId",
                "MessageSubject": "subject",
                "MessageId": "id_routers",
                "tag": "tag_id",
            })

            self.transform(df)

            last_row = df.iloc[-1]
            last_dwh_id = str(last_row["dwh_id"])
            last_message_id = int(last_row["id_routers"])
            last_date = (
                pd.to_datetime(last_row["event_datetime"])
                .tz_localize("UTC")
                .to_pydatetime()
            )

            print(
                f"Batch terminé: position suivante : "
                f"{last_dwh_id} / {last_message_id} / {last_date}"
            )

        del df
        _release_memory()

    # ------------------------------------------------------------------
    # Transformation + enrichissement (contacts uniquement, plus de PG ici)
    # ------------------------------------------------------------------
    def transform(self, df):

        databases = self._get_databases()
        df = df.merge(databases, left_on="database_id", right_on="id", how="inner")

        ids = df["dwh_id"].dropna().unique().tolist()
        contacts = self._fetch_contacts(ids)
        df = df.merge(contacts, on="dwh_id", how="left")
        del contacts

        bins = [0, 18, 25, 35, 45, 55, 65, 75, 200]
        labels = ["0-18", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75+"]

        df["age_range"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)
        df["age_range"] = df["age_range"].astype("object").fillna("O_age")
        df["main_isp"] = df["main_isp"].fillna("O_isp")

        df["sends"] = (df["event_type"] == "Sends").astype("uint8")
        df["opens"] = (df["event_type"] == "Opens").astype("uint8")
        df["clicks"] = (df["event_type"] == "Clicks").astype("uint8")
        df["unsubs"] = (
            (df["event_type"] == "Removals")
            & (df["removals_raison"] == "Subscriber")
        ).astype("uint8")

        df["date_event"] = pd.to_datetime(df["event_datetime"]).dt.date
        df["updated_at"] = pd.Timestamp.now(tz="UTC")

        df = df.drop_duplicates(
            subset=["dwh_id", "event_type", "id_routers", "date_event", "removals_raison"]
        )

        df = self.prepare_insert(df)
        self.conn.insert_df(self.table, df)

        del df
        _release_memory()

    def _fetch_contacts(self, ids: list) -> pd.DataFrame:
        empty_cols = ["dwh_id", "age", "gender", "zipcode", "dep", "main_isp"]

        if not ids:
            return pd.DataFrame(columns=empty_cols)

        self.conn.command("DROP TABLE IF EXISTS tmp_dwh_ids")
        self.conn.command("""
            CREATE TABLE tmp_dwh_ids (dwh_id String)
            ENGINE = MergeTree()
            ORDER BY dwh_id
        """)

        try:
            self.conn.insert("tmp_dwh_ids", [[x] for x in ids], column_names=["dwh_id"])

            contacts = self.conn.query_df("""
                SELECT dwh_id, age, gender, zipcode, dep, main_isp
                FROM contacts
                WHERE dwh_id GLOBAL IN (SELECT dwh_id FROM tmp_dwh_ids)
                SETTINGS max_memory_usage = 1000000000, max_threads = 2
            """)
        finally:
            self.conn.command("DROP TABLE IF EXISTS tmp_dwh_ids")

        if contacts.empty:
            contacts = pd.DataFrame(columns=empty_cols)

        return contacts

    # ------------------------------------------------------------------
    # Fenêtrage temporel
    # ------------------------------------------------------------------
    def iter_months(self):
        current = date(2025, 1, 1)
        end = date(2026, 1, 31)

        while current <= end:
            window_end = min(current + timedelta(days=2), end)
            yield (
                current.strftime("%Y-%m-%d"),
                window_end.strftime("%Y-%m-%d"),
            )
            current = window_end + timedelta(days=1)

    # ------------------------------------------------------------------
    # Préparation avant insert
    # ------------------------------------------------------------------
    def prepare_insert(self, df):
        columns = [
            "database_id", "ktk_id", "dwh_id", "country", "segmentId",
            "subject", "brand", "tag_id", "adv_id", "id_routers",
            "ListId", "ListName", "zipcode", "dep",
            "sends", "opens", "clicks", "unsubs", "complaints", "bounces",
            "age_range", "gender", "main_isp",
            "date_event", "updated_at",
        ]

        for col in columns:
            if col not in df.columns:
                if col in int_cols or col in uint_cols:
                    df[col] = 0
                else:
                    df[col] = ""

        df = df.loc[:, columns].copy()

        df["date_event"] = pd.to_datetime(df["date_event"]).dt.date
        df["updated_at"] = pd.to_datetime(df["updated_at"])

        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int32")
        for col in uint_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("uint8")
        for col in str_cols:
            df[col] = df[col].fillna("").astype(str)

        return df

    # ------------------------------------------------------------------
    # Point d'entrée
    # ------------------------------------------------------------------
    def run(self):
        for date_start, date_end in self.iter_months():
            print("=" * 50)
            print(f"Traitement période : {date_start} -> {date_end}")
            print("=" * 50)
            self.ajout(date_start, date_end)
        print("Chargement terminé (reporting)")


if __name__ == "__main__":
    AjoutReporting().run()
