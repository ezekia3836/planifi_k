import json

import pandas as pd
from sqlalchemy import text

from connexion import conn


class UpdateReportingFocus:
    """Alimente reporting_focus : une ligne par id_routers, agrégée
    directement en SQL côté PostgreSQL (pas de dict/boucle Python
    géant en mémoire)."""

    def __init__(self, pg_engine):
        self.pg = pg_engine
        self.conn = conn().getClient()
        self.table = "reporting_focus"

    # ------------------------------------------------------------------
    # Extraction agrégée depuis PostgreSQL
    # ------------------------------------------------------------------
    def recupere_pg_agrege(self, date_start: str, date_end: str) -> pd.DataFrame:
        query = text("""
            WITH base AS (
                SELECT
                    CASE
                        WHEN c.type = 'parent' THEN c.id
                        ELSE c.id_parent
                    END                     AS id_focus,
                    c.idsendout,
                    c.comment,
                    p.caeur                 AS ca,
                    p.clicksval              AS clicks_val,
                    p.leadsval               AS leads_val,
                    p.salesval               AS sales_val,
                    p.cpmval                 AS cpm_val,
                    p.model,
                    p.payvalue
                FROM campaigns c
                LEFT JOIN payout p
                    ON p.campaign_id = c.id
                WHERE c.date_schedule BETWEEN :date_start AND :date_end
                  AND c.idsendout IS NOT NULL
            )
            SELECT
                idsendout AS id_routers,
                array_agg(DISTINCT id_focus)            AS id_focus,
                SUM(COALESCE(ca, 0))                    AS ca,
                SUM(COALESCE(clicks_val, 0))            AS clicks_val,
                SUM(COALESCE(leads_val, 0))             AS leads_val,
                SUM(COALESCE(sales_val, 0))             AS sales_val,
                SUM(COALESCE(cpm_val, 0))                AS cpm_val,
                json_agg(DISTINCT jsonb_build_object(
                    'model', model,
                    'payvalue', payvalue,
                    'comment', comment
                ))                                       AS model
            FROM base
            WHERE idsendout IS NOT NULL
            GROUP BY idsendout
        """)

        with self.pg.connect() as connection:
            df = pd.read_sql(
                query, connection,
                params={"date_start": date_start, "date_end": date_end},
            )

        return df

    # ------------------------------------------------------------------
    # Préparation avant insert ClickHouse
    # ------------------------------------------------------------------
    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df["id_routers"] = pd.to_numeric(df["id_routers"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["id_routers"])
        df["id_routers"] = df["id_routers"].astype("int64")

        # id_focus : liste d'int, jamais de None dans la liste
        df["id_focus"] = df["id_focus"].apply(
            lambda x: [int(v) for v in x if v is not None] if isinstance(x, list) else []
        )

        # model : déjà une liste de dict côté PG -> sérialiser en JSON string
        df["model"] = df["model"].apply(
            lambda x: json.dumps(x, default=str, ensure_ascii=False) if not isinstance(x, str) else x
        )

        for col in ["ca", "clicks_val", "leads_val", "sales_val", "cpm_val"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")

        df["updated_at"] = pd.Timestamp.now(tz="UTC")

        columns = [
            "id_routers", "id_focus", "ca", "clicks_val",
            "leads_val", "sales_val", "cpm_val", "model", "updated_at",
        ]
        return df.loc[:, columns].copy()

    # ------------------------------------------------------------------
    # Point d'entrée
    # ------------------------------------------------------------------
    def run(self, date_start: str, date_end: str):
        print(f"Récupération PG agrégée : {date_start} -> {date_end}")
        df = self.recupere_pg_agrege(date_start, date_end)
        print(f"{len(df):,} id_routers récupérés")

        df = self._prepare(df)

        if not df.empty:
            self.conn.insert_df(self.table, df)
            print(f"Insertion terminée dans {self.table}")
        else:
            print("Aucune donnée à insérer")


if __name__ == "__main__":
    # Adapter la connexion PG existante du projet
    from db import pg_engine  # exemple : moteur SQLAlchemy déjà configuré

    UpdateReportingFocus(pg_engine).run("2025-01-01", "2026-01-31")
