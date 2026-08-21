from datetime import date
from typing import Optional

import clickhouse_connect
from fastapi import FastAPI, Query
from fastapi.concurrency import run_in_threadpool

app = FastAPI(title="Reporting API")

# Un seul client ClickHouse réutilisé pour toute l'app (pas de reconnexion
# à chaque requête).
_client = None


def get_client():
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(
            host="localhost",   # adapter
            username="default", # adapter
            password="",        # adapter
            database="default", # adapter
        )
    return _client


@app.get("/reporting")
async def get_reporting(
    date_start: date = Query(..., description="Date de début (incluse)"),
    date_end: date = Query(..., description="Date de fin (incluse)"),
    brand: Optional[str] = Query(None),
    id_routers: Optional[int] = Query(None),
    limit: int = Query(1000, le=50000),
):
    """Retourne le detail événementiel enrichi (sends/opens/clicks + ca/model
    au niveau id_routers via dictGet, sans JOIN)."""

    filters = ["date_event BETWEEN {date_start:Date} AND {date_end:Date}"]
    params = {"date_start": date_start, "date_end": date_end}

    if brand:
        filters.append("brand = {brand:String}")
        params["brand"] = brand

    if id_routers:
        filters.append("id_routers = {id_routers:Int64}")
        params["id_routers"] = id_routers

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT *
        FROM reporting_enriched
        WHERE {where_clause}
        ORDER BY date_event DESC
        LIMIT {limit}
    """

    client = get_client()
    df = await run_in_threadpool(client.query_df, query, parameters=params)
    return df.to_dict(orient="records")


@app.get("/reporting/summary")
async def get_reporting_summary(
    date_start: date = Query(...),
    date_end: date = Query(...),
    group_by: str = Query("date_event", description="date_event, brand, age_range, main_isp"),
):
    """Agrégats corrects : sends/opens/clicks sommés au grain événement,
    ca/leads/sales sommés une seule fois par id_routers (pas de
    sur-comptage) via un sous-select any() groupé par id_routers."""

    allowed_group_by = {"date_event", "brand", "age_range", "main_isp"}
    if group_by not in allowed_group_by:
        group_by = "date_event"

    query = f"""
        SELECT
            {group_by},
            sum(sends)  AS total_sends,
            sum(opens)  AS total_opens,
            sum(clicks) AS total_clicks,
            sum(unsubs) AS total_unsubs,
            sum(ca_par_routeur)         AS total_ca,
            sum(clicks_val_par_routeur) AS total_clicks_val,
            sum(leads_val_par_routeur)  AS total_leads_val,
            sum(sales_val_par_routeur)  AS total_sales_val
        FROM (
            SELECT
                {group_by},
                id_routers,
                sum(sends)  AS sends,
                sum(opens)  AS opens,
                sum(clicks) AS clicks,
                sum(unsubs) AS unsubs,
                any(ca)         AS ca_par_routeur,
                any(clicks_val) AS clicks_val_par_routeur,
                any(leads_val)  AS leads_val_par_routeur,
                any(sales_val)  AS sales_val_par_routeur
            FROM reporting_enriched
            WHERE date_event BETWEEN {{date_start:Date}} AND {{date_end:Date}}
            GROUP BY {group_by}, id_routers
        )
        GROUP BY {group_by}
        ORDER BY {group_by}
    """

    client = get_client()
    df = await run_in_threadpool(
        client.query_df, query,
        parameters={"date_start": date_start, "date_end": date_end},
    )
    return df.to_dict(orient="records")


@app.get("/health")
async def health():
    return {"status": "ok"}
