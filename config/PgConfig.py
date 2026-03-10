
from sqlalchemy import create_engine
from sqlalchemy import text
from config.config import Config as config
import pandas as pd
class PgConfig:
    def __init__(self):
        self.host=config.FOCUS_CONFIG['HOST']
        self.dbname=config.FOCUS_CONFIG['NAME']
        self.user=config.FOCUS_CONFIG['USER']
        self.password=config.FOCUS_CONFIG['PASSWORD']
        self.port=config.FOCUS_CONFIG['PORT']
        self.connect_timeout=5
    def get_client(self):
        return create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}",
            pool_pre_ping=True
        )

"""engine = PgConfig().get_client()
query = text(
SELECT
    vd.id                        AS id,
    vd.namesendout               AS namesendout,
    vd.date_shedule::date        AS date_shedule,

    a.id                         AS advertiser_id,
    a.name                       AS name,

    COALESCE((
        SELECT json_agg(DISTINCT idsendout)
        FROM (
            SELECT vd.idsendout
            UNION
            SELECT vd2.idsendout
            FROM visu.v2_data vd2
            WHERE vd2.id = ANY (
                SELECT vdr2.id_reuse
                FROM visu.v2_data_reuse vdr2
                WHERE vdr2.id_v2 = vd.id
            )
            AND vd2.idsendout IS NOT NULL
        ) t
    ), '[]'::json)               AS id_routers,
    (
        COALESCE((
            SELECT SUM(vd2.sent)
            FROM visu.v2_data vd2
            WHERE vd2.id = ANY (
                SELECT vdr2.id_reuse
                FROM visu.v2_data_reuse vdr2
                WHERE vdr2.id_v2 = vd.id
            )
        ), 0) + COALESCE(vd.sent, 0)
    ) AS sent,

    (
        COALESCE((
            SELECT SUM(vd2.delivered)
            FROM visu.v2_data vd2
            WHERE vd2.id = ANY (
                SELECT vdr2.id_reuse
                FROM visu.v2_data_reuse vdr2
                WHERE vdr2.id_v2 = vd.id
            )
        ), 0) + COALESCE(vd.delivered, 0)
    ) AS delivered,

    (
        COALESCE((
            SELECT SUM(vd2.opens)
            FROM visu.v2_data vd2
            WHERE vd2.id = ANY (
                SELECT vdr2.id_reuse
                FROM visu.v2_data_reuse vdr2
                WHERE vdr2.id_v2 = vd.id
            )
        ), 0) + COALESCE(vd.opens, 0)
    ) AS opens,

    (
        COALESCE((
            SELECT SUM(vd2.clicks)
            FROM visu.v2_data vd2
            WHERE vd2.id = ANY (
                SELECT vdr2.id_reuse
                FROM visu.v2_data_reuse vdr2
                WHERE vdr2.id_v2 = vd.id
            )
        ), 0) + COALESCE(vd.clicks, 0)
    ) AS clicks,

    (
        COALESCE((
            SELECT SUM(vd2.unsubs)
            FROM visu.v2_data vd2
            WHERE vd2.id = ANY (
                SELECT vdr2.id_reuse
                FROM visu.v2_data_reuse vdr2
                WHERE vdr2.id_v2 = vd.id
            )
        ), 0) + COALESCE(vd.unsubs, 0)
    ) AS unsubs,

    (
        COALESCE((
            SELECT SUM(vd2.complains)
            FROM visu.v2_data vd2
            WHERE vd2.id = ANY (
                SELECT vdr2.id_reuse
                FROM visu.v2_data_reuse vdr2
                WHERE vdr2.id_v2 = vd.id
            )
        ), 0) + COALESCE(vd.complains, 0)
    ) AS complains

FROM visu.v2_data vd
JOIN visu.advertiser a ON a.id = vd.advertiser
JOIN visu.v2_status st ON st.id = vd.status

WHERE
    st.id = 5
    AND vd.advertiser = :advertiser_id
    AND vd.sent > 0
    AND vd.date_shedule >= NOW() - INTERVAL '2 days'

ORDER BY vd.date_shedule;

)

params = {
    "advertiser_id": 4892  
}

with engine.connect() as conn:
    df = pd.read_sql(query, conn, params=params)



    num_cols = ["sent", "delivered", "opens", "clicks", "unsubs", "complains","advertiser_id"]
    df[num_cols] = df[num_cols].fillna(0)
    df = (df.groupby(["id", "date_shedule","name"], as_index=False)[num_cols].sum())

    df["taux_delivery"] = (df["delivered"] / df["sent"]).where(df["sent"] > 0, 0) * 100
    df["taux_open"]     = (df["opens"]     / df["sent"]).where(df["sent"] > 0, 0) * 100
    df["taux_click"]    = (df["clicks"]    / df["sent"]).where(df["sent"] > 0, 0) * 100
    df["taux_unsubs"]   = (df["unsubs"]    / df["sent"]).where(df["sent"] > 0, 0) * 100
    df["taux_complain"] = (df["complains"] / df["sent"]).where(df["sent"] > 0, 0) * 100
    taux_cols = [
        "taux_delivery",
        "taux_open",
        "taux_click",
        "taux_unsubs",
        "taux_complain",
    ]

    df[taux_cols] = df[taux_cols].round(2)
    file_path='shoot.csv'
    df.to_csv(
        file_path,
        index=False,
        sep=";",
        encoding="utf-8-sig"
    )"""


