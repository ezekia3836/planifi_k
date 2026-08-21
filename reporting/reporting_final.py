from threading import Lock
import json
from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from config.konticrea import connect_kit
from datetime import datetime, date, timedelta
from contextlib import contextmanager
from sqlalchemy import text
from dateutil.relativedelta import relativedelta
from queue import Queue
import logging
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("reporting")

CLK_POOL_SIZE = 8
JOURNAL_PATH  = "journal.txt"
INIT_START    = date(2026, 5, 1)   


class reporting:
    def __init__(self):
        self.clk       = ClickHouseConfig().getClient_prod()
        self.pg        = PgConfig().get_client()
        self.table     = "old_prod_reporting"
        self.konticrea = connect_kit()
        self.optimize_cache: dict = {}
        self._optimize_lock = Lock()
        self.init_clk_pool()

    # ── Pool ClickHouse ───────────────────────────────────────────────────────
    def init_clk_pool(self, size: int = CLK_POOL_SIZE):
        self.clk_pool = Queue(maxsize=size)
        for _ in range(size):
            self.clk_pool.put(ClickHouseConfig().getClient_prod())

    @contextmanager
    def get_clk(self):
        clk = self.clk_pool.get()
        try:
            yield clk
        finally:
            self.clk_pool.put(clk)

    # ── Notifications ─────────────────────────────────────────────────────────
    def notifier_info(self, msg):   logger.info(msg)
    def notifier_erreur(self, msg): logger.error(msg)

    def resilient_call(self, func, *args, max_retry=5, sleep_sec=5, backoff=True, **kwargs):
        attempt, wait = 1, sleep_sec
        while attempt <= max_retry:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Tentative {attempt}/{max_retry} : {e}")
                if attempt == max_retry:
                    raise
                time.sleep(wait)
                if backoff:
                    wait *= 2
                attempt += 1

    # ── Journal ───────────────────────────────────────────────────────────────
    def _read_journal(self) -> dict:
        """Lit journal.txt et retourne un dict de clés/valeurs."""
        journal = {}
        if not os.path.exists(JOURNAL_PATH):
            return journal
        with open(JOURNAL_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if "=" in line:
                    k, v = line.split("=", 1)
                    journal[k.strip()] = v.strip()
        return journal

    def _write_journal(self, data: dict):
        """Écrase journal.txt avec le dict fourni."""
        with open(JOURNAL_PATH, "w") as f:
            for k, v in data.items():
                f.write(f"{k}={v}\n")

    def _journal_set(self, key: str, value: str):
        """Met à jour une clé dans le journal sans écraser les autres."""
        j = self._read_journal()
        j[key] = value
        self._write_journal(j)

    def _is_initialized(self) -> bool:
        return self._read_journal().get("initialized") == "1"

    def _last_processed_month(self) -> date | None:
        """Retourne le dernier mois traité (1er du mois) ou None."""
        j = self._read_journal()
        v = j.get("last_processed_month")
        if v:
            try:
                return datetime.strptime(v, "%Y-%m").date().replace(day=1)
            except Exception:
                return None
        return None

    # ── Helpers dates ─────────────────────────────────────────────────────────
    @staticmethod
    def _month_bounds(d: date) -> tuple[str, str]:
        """Retourne (premier_jour, dernier_jour) du mois de d, en string."""
        first = d.replace(day=1)
        last  = (first + relativedelta(months=1)) - timedelta(days=1)
        return first.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d")

    @staticmethod
    def _current_month_start() -> date:
        return date.today().replace(day=1)

    # ── PG : focus_map ────────────────────────────────────────────────────────
    def recupere_pg(self, date_start: str, date_end: str, batch: int = 1000) -> dict:
        query = text("""
WITH base AS (
    SELECT
        CASE
            WHEN c.type = 'parent' THEN c.id
            ELSE c.id_parent
        END                         AS id_focus,

        c.comment                   AS comment,
        c.date_schedule             AS date_schedule,

        p.caeur                     AS ca,
        p.clicksval                 AS clicks_val,
        p.leadsval                  AS leads_val,
        p.salesval                  AS sales_val,
        p.cpmval                    AS cpm_val,
        p.model                     AS model,
        p.payvalue                  AS payvalue

    FROM campaigns c
    LEFT JOIN payout p
        ON p.campaign_id = c.id
    WHERE c.date_schedule BETWEEN :date_start AND :date_end
),

routers AS (
    SELECT
        CASE
            WHEN c.type = 'parent' THEN c.id
            ELSE c.id_parent
        END AS id_focus,

        c.idsendout,

        CASE
            WHEN c.type = 'parent' THEN FALSE
            ELSE TRUE
        END AS is_direct

    FROM campaigns c
    WHERE c.date_schedule BETWEEN :date_start AND :date_end
      AND c.idsendout IS NOT NULL
),

routers_agg AS (
    SELECT
        id_focus,
        json_agg(
            DISTINCT jsonb_build_object(
                'idsendout', idsendout,
                'is_direct', is_direct
            )
        ) AS id_routers
    FROM routers
    GROUP BY id_focus
),

focus_agg AS (
    SELECT
        b.id_focus,

        SUM(b.ca)         AS ca,
        SUM(b.clicks_val) AS clicks_val,
        SUM(b.leads_val)  AS leads_val,
        SUM(b.sales_val)  AS sales_val,
        SUM(b.cpm_val)    AS cpm_val,

        json_agg(
            DISTINCT jsonb_build_object(
                'model',    b.model,
                'payvalue', b.payvalue,
                'comment',  b.comment
            )
        ) AS model,

        json_agg(
            DISTINCT b.date_schedule
            ORDER BY b.date_schedule
        ) AS date_schedule

    FROM base b
    GROUP BY b.id_focus
)

SELECT
    fa.id_focus,
    fa.ca,
    fa.model,
    fa.clicks_val,
    fa.leads_val,
    fa.sales_val,
    fa.cpm_val,
    fa.date_schedule,
    ra.id_routers

FROM focus_agg fa
LEFT JOIN routers_agg ra
    ON ra.id_focus = fa.id_focus

ORDER BY fa.id_focus
""")

        result: dict = {}
        try:
            with self.pg.connect() as conn:
                rows_result = conn.execution_options(
                    stream_results=True, yield_per=batch
                ).execute(query, {"date_start": date_start, "date_end": date_end})

                while True:
                    rows = rows_result.fetchmany(batch)
                    if not rows:
                        break
                    for row in rows:
                        (id_focus, database_id, ca, model,clicks_val,
                         leads_val, cpm_val,date_schedule, id_routers_list) = row

                        if not id_routers_list:
                            continue

                        focus_data = {
                            "id_focus":      id_focus,
                            "database_id":   database_id,
                            "ca":            ca         or 0,
                            "model":         model or [],
                            "clicks_val":    clicks_val or 0,
                            "leads_val":     leads_val  or 0,
                            "cpm_val":       cpm_val    or 0,
                            "date_schedule": date_schedule if isinstance(date_schedule, list) else [],
                        }

                        for item in id_routers_list:
                            id_router = item.get("idsendout")
                            if (id_router is None or str(id_router).strip() in ("", "None", "NULL", "null")):
                                continue
                            try:
                                id_router = int(id_router)
                                if id_router < 0:
                                    continue
                            except Exception:
                                continue
                            is_direct = item.get("is_direct", False)
                            key = (int(id_router), int(database_id))
                            existing = result.get(key)
                            if existing is None or (is_direct and not existing["is_direct"]):
                                result[key] = {**focus_data, "is_direct": is_direct}

        except Exception as e:
            self.notifier_erreur(f"Erreur PG focus : {e}")
            return {}
        return result

    def _parse_date(self, v):
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            try:
                return datetime.strptime(v[:10], "%Y-%m-%d").date()
            except Exception:
                return None
        return None

    def _load_focus_to_clickhouse(self, date_start: str, date_end: str):
        logger.info("Chargement focus PG → ClickHouse...")

        focus_map = self.recupere_pg(date_start=date_start, date_end=date_end)
        logger.info(f"focus_map récupéré : {len(focus_map)} entrées")

        if not focus_map:
            logger.warning("focus_map vide")
            return

        column_names = [
            "id_router", "database_id", "id_focus", "ca", "model",
            "clicks_val", "leads_val", "cpm_val",
            "date_schedule", "date_schedule_max", "is_direct",
        ]

        rows = []

        for (id_router, database_id), focus in focus_map.items():

            raw_models = focus.get("model") or []
            clean_models: list[str] = []

            for i, m in enumerate(raw_models):
                if isinstance(m, str):
                    try:
                        json.loads(m)
                        clean_models.append(m)
                    except Exception:
                        logger.warning(f"STRING MODEL INVALIDE SKIPPED: {m}")
                    continue

                if isinstance(m, dict):
                    model_name = str(m.get("model") or "")
                    payvalue   = float(m.get("payvalue") or 0)
                    comment    = str(m.get("comment") or "")

                elif isinstance(m, (list, tuple)):
                    if len(m) < 3:
                        logger.warning(f"INCOMPLETE MODEL: {m}")
                        continue
                    model_name = str(m[0] or "")
                    payvalue   = float(m[1] or 0)
                    comment    = str(m[2] or "")

                else:
                    logger.warning(f"INVALID MODEL TYPE: {type(m)} — {m}")
                    continue

                clean_models.append(json.dumps(
                    {"model": model_name, "payvalue": payvalue, "comment": comment},
                    ensure_ascii=False
                ))

            # DATE SCHEDULE
            raw_schedule = focus.get("date_schedule") or []
            schedule = [
                d for d in (self._parse_date(x) for x in raw_schedule)
                if d is not None
            ]
            date_schedule_max = max(schedule) if schedule else None

            rows.append([
                int(id_router),
                int(database_id),
                int(focus["id_focus"]),
                float(focus.get("ca") or 0),
                clean_models,                          # ← List[str] JSON sérialisés
                int(focus.get("clicks_val") or 0),
                int(focus.get("leads_val") or 0),
                int(focus.get("cpm_val") or 0),
                schedule,
                date_schedule_max,
                bool(focus.get("is_direct", False)),
            ])

        logger.info(f"rows construits : {len(rows)}")

        # CLICKHOUSE TABLE
        self.clk.command("DROP TABLE IF EXISTS tmp_focus_map")
        self.clk.command("""
            CREATE TABLE tmp_focus_map (
                id_router         UInt64,
                database_id       Int64,
                id_focus          Int64,
                ca                Float64,
                model             Array(String),
                clicks_val        Int64,
                leads_val         Int64,
                cpm_val           Int64,
                date_schedule     Array(Date),
                date_schedule_max Nullable(Date),
                is_direct         Bool
            )
            ENGINE = Memory
        """)

        self.clk.insert("tmp_focus_map", rows, column_names=column_names)
        logger.info(f"tmp_focus_map chargé : {len(rows)} entrées")

    def _materialize_contacts(self, date_start: str, date_end: str):
        logger.info("Matérialisation contacts...")

        self.clk.command("DROP TABLE IF EXISTS tmp_contacts")

        self.clk.command("""
            CREATE TABLE tmp_contacts (
                dwh_id   String,
                age      Nullable(Int32),
                gender   Nullable(String),
                main_isp Nullable(String),
                zipcode  Nullable(String),
                dep      Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY dwh_id
        """)

        self.clk.command(f"""
            INSERT INTO tmp_contacts
            SELECT
                c.dwh_id,
                argMax(c.age, c.updated_at)      AS age,
                argMax(c.gender, c.updated_at)   AS gender,
                argMax(c.main_isp, c.updated_at) AS main_isp,
                argMax(c.zipcode, c.updated_at)  AS zipcode,
                argMax(c.dep, c.updated_at)      AS dep
            FROM prod_contacts c
            WHERE c.dwh_id IN (      
                SELECT dwh_id
                FROM prod_events_2
                WHERE Date BETWEEN '{date_start}' AND '{date_end}'
            )
            GROUP BY c.dwh_id
            SETTINGS
                join_algorithm = 'grace_hash',
                max_threads = 4,
                max_memory_usage = 8000000000,    
                max_bytes_before_external_group_by = 3000000000,
                max_bytes_before_external_sort     = 3000000000
                """)

        count = self.clk.query(
            "SELECT count() FROM tmp_contacts"
        ).result_rows[0][0]
        logger.info(f"tmp_contacts prêt : {count} contacts uniques")
    def _insert_reporting(self, date_start: str, date_end: str):
        logger.info("INSERT INTO reporting en cours...")
        query = f"""
            INSERT INTO {self.table}
            SELECT
                p.database_id                                            AS database_id,
                d.ktk_id                                                  AS ktk_id,
                p.dwh_id                                                 AS dwh_id,
                toInt32(d.country)                                       AS country,
                toInt32(p.SegmentId)                                     AS segmentId,
                p.MessageSubject                                         AS subject,
                p.brand                                                  AS brand,
                toInt32(p.tag)                                           AS tag_id,
                toInt32(p.adv_id)                                        AS adv_id,
                toString(p.MessageId)                                    AS id_routers,
                toInt32(f.id_focus)                                      AS id_focus,
                toInt32(p.affiliate_id)                                  AS affiliate_id,
                toInt32(p.client_id)                                     AS client_id,
                toInt32(p.ListId)                                        AS ListId,
                p.ListName                                               AS ListName,
                if(
                    lower(trim(BOTH ' ' FROM ifNull(toString(c.zipcode), '')))
                        IN ('', 'none', 'null', 'o', 'other'),
                    'zipcode_vide',
                    trim(BOTH ' ' FROM toString(c.zipcode))
                )                                                        AS zipcode,
                if(
                    lower(trim(BOTH ' ' FROM ifNull(toString(c.dep), '')))
                        IN ('', 'none', 'null', 'o', 'other'),
                    'dep_vide',
                    lower(trim(BOTH ' ' FROM toString(c.dep)))
                )                                                        AS dep,

                toUInt8(p.event_type = 'Sends')                          AS sends,
                toUInt8(p.event_type = 'Opens')                          AS opens,
                toUInt8(p.event_type = 'Clicks')                         AS clicks,
                toUInt8(
                    p.event_type = 'Removals'
                    AND p.removals_raison = 'Subscriber'
                )                                                        AS unsubs,
                toUInt8(p.event_type = 'Complaints')                     AS complaints,
                toUInt8(p.event_type = 'Bounces')                        AS bounces,

                multiIf(
                    c.age IS NULL, 'O_age',
                    c.age < 18,    '0-18',
                    c.age < 25,    '18-24',
                    c.age < 35,    '25-34',
                    c.age < 45,    '35-44',
                    c.age < 55,    '45-54',
                    c.age < 65,    '55-64',
                    c.age < 75,    '65-74',
                                   '75+'
                )                                                        AS age_range,

                if(
                    lower(trim(BOTH ' ' FROM ifNull(toString(c.gender), '')))
                        IN ('', 'none', 'null', 'o', 'other'),
                    'O_gender',
                    trim(BOTH ' ' FROM toString(c.gender))
                )                                                        AS gender,

                if(
                    lower(trim(BOTH ' ' FROM ifNull(toString(c.main_isp), '')))
                        IN ('', 'none', 'null', 'o', 'other'),
                    'O_isp',
                    trim(BOTH ' ' FROM toString(c.main_isp))
                )                                                        AS main_isp,

                concat(
                    multiIf(
                        c.age IS NULL, 'O_age',
                        c.age < 18, '0-18',
                        c.age < 25, '18-24',
                        c.age < 35, '25-34',
                        c.age < 45, '35-44',
                        c.age < 55, '45-54',
                        c.age < 65, '55-64',
                        c.age < 75, '65-74',
                                    '75+'
                    ),
                    '_',
                    if(
                        lower(trim(BOTH ' ' FROM ifNull(toString(c.gender), '')))
                            IN ('', 'none', 'null', 'o', 'other'),
                        'O_gender',
                        trim(BOTH ' ' FROM toString(c.gender))
                    ),
                    '_',
                    if(
                        lower(trim(BOTH ' ' FROM ifNull(toString(c.main_isp), '')))
                            IN ('', 'none', 'null', 'o', 'other'),
                        'O_isp',
                        trim(BOTH ' ' FROM toString(c.main_isp))
                    )
                )                                                        AS age_gender_isp,

                toFloat64(f.ca)                                          AS ca,
                f.model                                                  AS model,
                toInt32(f.clicks_val)                                    AS clicks_val,
                toInt32(f.leads_val)                                     AS leads_val,
                toInt32(f.cpm_val)                                       AS cpm_val,
                f.date_schedule                                          AS date_schedule,
                toDate(p.Date)                                           AS date_event,
                ''                                                       AS optimized,
                f.date_schedule_max                                      AS date_schedule_max,
                now()                                                    AS updated_at
            FROM prod_events_2 p
            INNER JOIN databases d
                ON d.id = p.database_id
            INNER JOIN tmp_focus_map f
                ON f.id_router = toUInt64(p.MessageId)
               AND toInt64(f.database_id) = toInt64(d.stats_id)
            LEFT JOIN tmp_contacts c
                ON c.dwh_id = p.dwh_id
            WHERE p.Date BETWEEN '{date_start}' AND '{date_end}' AND p.adv_id !=0

            SETTINGS
                max_memory_usage   = 10000000000,
                join_algorithm     = 'partial_merge',
                max_threads        = 8,
                max_insert_threads = 4
        """
        self.resilient_call(self.clk.command, query)
        logger.info("INSERT terminé")

    def _patch_optimized(self):
        logger.info("Patch optimized...")
        r = self.clk.query(f"""
            SELECT DISTINCT id_focus, ktk_id
            FROM {self.table}
            WHERE optimized = ''
        """)
        rows = [{"id_focus": str(row[0]), "ktk_id": str(row[1])} for row in r.result_rows]
        if not rows:
            logger.info("Rien à patcher")
            return

        optimize_map = self.recuper_optimize(rows)
        if not optimize_map:
            logger.info("optimize_map vide")
            return

        for (id_focus, ktk_id), value in optimize_map.items():
            safe_value = str(value).replace("'", "\\'")
            self.clk.command(f"""
                ALTER TABLE {self.table}
                UPDATE optimized = '{safe_value}'
                WHERE id_focus = {id_focus}
                  AND ktk_id = {ktk_id}
                  AND optimized = ''
            """)
        logger.info(f"Patch optimized terminé : {len(optimize_map)} valeurs")

    def recuper_optimize(self, rows_list: list, chunk: int = 500) -> dict:
        if not self.konticrea:
            return {}
        cursor = self.konticrea.cursor()
        keys   = list({
            (str(r.get("id_focus")), str(r.get("id_router")))
            for r in rows_list
            if r.get("id_focus") and r.get("ktk_id")
        })
        optimized_map = {}
        for i in range(0, len(keys), chunk):
            batch  = keys[i:i + chunk]
            values = ",".join(f"('{f}','{b}')" for f, b in batch)
            try:
                cursor.execute(f"""
                    SELECT focus_id, base_id, optimized
                    FROM creativities
                    WHERE (focus_id, base_id) IN ({values})
                """)
                for focus_id, base_id, optimized in cursor.fetchall():
                    optimized_map[(str(focus_id), str(base_id))] = optimized
            except Exception as e:
                self.notifier_erreur(f"Erreur optimize chunk {i//chunk+1} : {e}")
        cursor.close()
        return optimized_map

    def _process_month(self, month_start: date):
        date_start, date_end = self._month_bounds(month_start)
        label = month_start.strftime("%Y-%m")
        logger.info(f"═══ Traitement mois {label} ({date_start} → {date_end}) ═══")

        try:
            self._load_focus_to_clickhouse(date_start, date_end)
            self._materialize_contacts(date_start, date_end)
            self._insert_reporting(date_start, date_end)
            self._patch_optimized()
            logger.info(f"✓ Mois {label} traité avec succès")
        finally:
            self.clk.command("DROP TABLE IF EXISTS tmp_focus_map")
            self.clk.command("DROP TABLE IF EXISTS tmp_contacts")
            self.clk.command("DROP TABLE IF EXISTS tmp_contacts_dedup")
            logger.info("Tables temporaires supprimées")

    def report(self):
        today  = date.today()
        current_month = today.replace(day=1)

        if not self._is_initialized():
            logger.info("Initialisation : traitement historique depuis juin 2025...")

            last = self._last_processed_month()
            if last is None:
                cursor = INIT_START
            else:
                cursor = last + relativedelta(months=1)
                logger.info(f"Reprise depuis {cursor.strftime('%Y-%m')} (dernier traité : {last.strftime('%Y-%m')})")

            while cursor <= current_month:
                self._process_month(cursor)
                self._journal_set("last_processed_month", cursor.strftime("%Y-%m"))
                cursor += relativedelta(months=1)

            self._journal_set("initialized", "1")
            logger.info("Initialisation terminée → initialized=1 écrit dans le journal")

        else:
         
            month_m1 = (current_month - relativedelta(months=1))
            month_m0 = current_month

            logger.info(f"Mode normal : traitement de {month_m1.strftime('%Y-%m')} et {month_m0.strftime('%Y-%m')}")

            for month in (month_m1, month_m0):
                self._process_month(month)
                self._journal_set("last_processed_month", month.strftime("%Y-%m"))

            logger.info("Traitement des 2 mois récents terminé")
