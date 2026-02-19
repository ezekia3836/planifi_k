from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from datetime import datetime, timedelta
import time
import os
import math
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from winotify import Notification
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
import csv
import io
import gc
import pandas as pd
class reporting:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table = "reporting"
        today = datetime.today()
        self.date_end = today.date()
        self.date_start = datetime(year=today.year-1, month=7, day=1).date()
        self.batch_adv_size = 50
        self.adv_ids = [54]  # Events().get_adv_ids() si nécessaire

    # ------------------------- Helpers -------------------------
    def resilient_call(self, func, *args, max_retry=5, sleep_sec=5, backoff=True, **kwargs):
        attempt = 1
        wait = sleep_sec
        while attempt <= max_retry:
            try:
                return func(*args, **kwargs)
            except (requests.ConnectionError, requests.Timeout, Exception) as e:
                print(f"Tentative {attempt}/{max_retry} échouée : {e}")
                self.notifier_erreur(f"Tentative {attempt}/{max_retry} échouée : {e}")
                if attempt == max_retry:
                    raise
                time.sleep(wait)
                if backoff:
                    wait *= 2
                attempt += 1

    def notifier_info(self, message):
        toast = Notification(
            app_id="Reporting Planifik",
            title="✅ Succès",
            msg=message,
            duration="short"
        )
        toast.show()

    def notifier_erreur(self, message):
        toast = Notification(
            app_id="Reporting Planifik",
            title="❌ Erreur",
            msg=message,
            duration="long"
        )
        toast.show()

    def safe(self, value):
        try:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return 0
            f = float(value)
            if not math.isfinite(f):
                return 0
            return int(f)
        except (ValueError, TypeError):
            return 0

    def clean_adv_ids(self, adv_ids):
        return list(set([str(x).strip() for x in adv_ids if str(x).strip().isdigit()]))
    def safe_update(self, row, new_data):
        if isinstance(new_data, dict):
            row.update(new_data)

    # ------------------------- ClickHouse / PG -------------------------
    def recupere_events(self, adv_ids):
        print("events")
        adv_ids_clean = self.clean_adv_ids(adv_ids)
        if not adv_ids_clean:
            return []

        all_rows = []
        for i in range(0, len(adv_ids_clean), self.batch_adv_size):
            batch = adv_ids_clean[i:i+self.batch_adv_size]
            batch_str = ",".join(str(x) for x in batch)
            query = f"""
            SELECT
                e.database_id,
                e.MessageId AS id_routers,
                e.adv_id,
                e.dwh_id,
                e.SegmentId AS segmentId,
                e.MessageSubject AS subject,
                e.event_type,
                e.Date AS date_event,
                e.tag AS tag_id,
                e.brand,
                e.client_id,
                e.ListId
            FROM events_2 e
            INNER JOIN (
                SELECT MessageId, event_type, max(run_id) AS max_run_id
                FROM events_2
                WHERE adv_id IN ({batch_str})
                AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                GROUP BY MessageId, event_type
            ) m
            ON e.MessageId = m.MessageId AND e.event_type = m.event_type AND e.run_id = m.max_run_id
            WHERE e.adv_id IN ({batch_str})
            AND e.Date BETWEEN '{self.date_start}' AND '{self.date_end}'
            """
            try:
                r = self.resilient_call(self.clk.query, query)
                for row in r.result_rows:
                    record = dict(zip(r.column_names, row))
                    all_rows.append(record)
            except Exception as e:
                print(f"Erreur ClickHouse events batch {batch_str}: {e}")
                self.notifier_erreur(f"Erreur recup events: {e}")

        return all_rows

    def recupere_contacts(self, dwh_ids, batch_size=5000):
        print("contacts")
        if not dwh_ids:
            return {}

        all_rows = []
        for i in range(0, len(dwh_ids), batch_size):
            batch = dwh_ids[i:i+batch_size]
            batch_str = ",".join(f"'{x}'" for x in batch)
            query = f"""
                SELECT dwh_id, age, gender, main_isp, zipcode, dep
                FROM contacts_2
                PREWHERE dwh_id IN ({batch_str})
                ORDER BY updated_at DESC
                LIMIT 1 BY dwh_id
                SETTINGS optimize_read_in_order = 1
            """
            try:
                r = self.resilient_call(self.clk.query, query)
                for row in r.result_rows:
                    record = dict(zip(r.column_names, row))
                    all_rows.append(record)
            except Exception as e:
                print(f"Erreur contacts batch {batch_str}: {e}")
                self.notifier_erreur(f"Erreur recup contacts: {e}")

        return {str(r["dwh_id"]): r for r in all_rows}

    def recupere_pg(self, adv_ids):
        print("pg")
        adv_ids_clean = self.clean_adv_ids(adv_ids)
        if not adv_ids_clean:
            return {}

        adv_ids_str = ",".join(f"'{x}'" for x in adv_ids_clean)
        query = text(f"""
            SELECT
                vd.id AS id_focus,
                vd.campaingkind AS ca,
                COALESCE(json_agg(DISTINCT vd.date_shedule), '[]'::json) AS date_shedule,
                COALESCE(json_agg(DISTINCT idsendouts.idsendout), '[]'::json) AS id_routers
            FROM visu.v2_data vd
            JOIN visu.v2_status st ON st.id = vd.status
            LEFT JOIN LATERAL (
                SELECT vd1.idsendout
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
                ) AS vd1
            ) AS idsendouts ON TRUE
            WHERE st.id = 5
            AND vd.advertiser IN ({adv_ids_str})
            GROUP BY vd.campaingkind, vd.id
        """)

        try:
            with self.pg.connect() as conn:
                result = self.resilient_call(lambda: conn.execute(query).fetchall())

            pg_map = {}
            for row in result:
                id_focus = str(row[0])
                ca = row[1]
                date_shedule = row[2] or []
                id_routers_list = row[3] or []

                # On crée une entrée pour chaque id_routers
                for id_r in id_routers_list:
                    key = (id_focus, str(id_r))
                    pg_map[key] = {
                        "ca": ca,
                        "date_shedule": date_shedule
                    }

            return pg_map

        except Exception as e:
            print("Erreur Postgres:", e)
            self.notifier_erreur(f"Erreur Focus: {e}")
            return {}
    def recupere_ktk_id(self, database_ids):
        print("ktk_id")
        if not database_ids:
            return {}  # retourne dict vide

        try:
            ids = ",".join(str(x) for x in database_ids)
            query = f"""SELECT id AS database_id, ktk_id, basename FROM databases WHERE id IN ({ids})"""
            r = self.clk.query(query)

            if not r.result_rows:
                return {}

            db_map = {}
            for row in r.result_rows:
                # row est un tuple (database_id, ktk_id, basename)
                db_id = str(row[0])
                ktk_id = row[1] if row[1] is not None else "ktk_vide"
                basename = row[2] if row[2] is not None else "base_vide"
                db_map[db_id] = {"ktk_id": ktk_id, "basename": basename}

            return db_map

        except Exception as e:
            print("Erreur lors de la récupération ktk_id :", e)
            return {}

    def recuper_optimize(self, rows_list, max_workers=8):
        
        print("optimized")
        endpoint = "https://konticreav2.kontikimedia.fr:5009/api/creativities/filter-plannifik"

        def call_api(row):
            try:
                id_routers = self.safe(row.get("id_routers"))
                id_focus = self.safe(row.get("id_focus"))
                ktk_id = self.safe(row.get("ktk_id"))

                if not id_routers or not id_focus or not ktk_id:
                    return "url_vide"

                params = {"focus_id": id_focus, "base_id": ktk_id, "router_id": id_routers}
                resp = self.resilient_call(requests.post, endpoint, params=params, timeout=30)

                if resp.status_code == 200:
                    data = resp.json()
                    return next((item.get("optimized") for item in data.get("data", []) if item.get("optimized")), "url_vide")

            except Exception as e:
                self.notifier_erreur(f"Optimized erreur: {e}")
                print(f"Optimized erreur: {e}")
            return "url_vide"

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(call_api, rows_list))

        # Construire un dict pour accéder facilement par tuple
        optimized_map = {}
        for row, opt in zip(rows_list, results):
            key = (str(row.get("id_routers")), str(row.get("id_focus")), str(row.get("ktk_id")))
            optimized_map[key] = opt

        return optimized_map
    # ------------------------- Report principal -------------------------
    def report(self, journal="journal.txt", batch_optimize=5000):
        final_all = []

        # Lire le journal
        if os.path.exists(journal):
            with open(journal,'r') as f:
                process_adv = set(int(line.strip()) for line in f.readlines())
        else:
            process_adv = set()

        for adv_id in self.adv_ids:
            if adv_id in process_adv:
                continue

            print(f"Traitement advertiser {adv_id} ........")
            try:
                # Récupérer events
                events_rows = self.resilient_call(self.recupere_events, [adv_id])
                if not events_rows:
                    with open(journal, "a") as f:
                        f.write(f"{adv_id}\n")
                        process_adv.add(adv_id)
                    continue

                # Initialiser compteurs et flags
                for row in events_rows:
                    ev = row.get("event_type","")
                    row["sends"] = 1 if ev=="Sends" else 0
                    row["opens"] = 1 if ev=="Opens" else 0
                    row["clicks"] = 1 if ev=="Clicks" else 0
                    row["removals"] = 1 if ev=="Removals" else 0
                    row["complaints"] = 1 if ev=="Complaints" else 0
                    row["bounces"] = 1 if ev=="Bounces" else 0
                    row["opener"] = 1 if ev=="Opens" else 0
                    row["clicker"] = 1 if ev=="Clicks" else 0

                # Récupérer contacts
                dwh_ids = list({row.get("dwh_id") for row in events_rows if row.get("dwh_id")})
                contacts_map = self.resilient_call(self.recupere_contacts, dwh_ids)

                # Récupérer PG
                pg_map = self.resilient_call(self.recupere_pg, [adv_id])

                # Récupérer ktk_id
                database_ids = list({row.get("database_id") for row in events_rows})
                db_map = self.resilient_call(self.recupere_ktk_id, database_ids)

                # Merge contacts, pg, db et sécuriser les valeurs
                final_rows = []
                for row in events_rows:
                    # Contacts
                    dwh = row.get("dwh_id")
                    contact = contacts_map.get(dwh, {}) if contacts_map else {}
                    row.update(contact)

                    # PG safe
                    id_r = row.get("id_routers")
                    if isinstance(id_r, (list, tuple)):
                        id_r = str(id_r[0]) if id_r else "O_router"
                    else:
                        id_r = str(id_r or "O_router")

                    id_f = row.get("id_focus")
                    if isinstance(id_f, (list, tuple)):
                        id_f = str(id_f[0]) if id_f else "0"
                    else:
                        id_f = str(id_f or "0")

                    pg_data = pg_map.get((id_f, id_r), {}) if pg_map else {}
                    row.update(pg_data)

                    # DB safe
                    db_id = row.get("database_id")
                    if isinstance(db_id, (list, tuple)):
                        db_id = db_id[0] if db_id else 0
                    db_data = db_map.get(str(db_id), {"ktk_id":"ktk_vide","basename":"base_vide"})
                    row.update(db_data)

                    row["id_routers"] = id_r
                    row["id_focus"] = id_f
                    row["ktk_id"] = row.get("ktk_id","ktk_vide")
                    row["basename"] = row.get("basename","base_vide")

                    # Calcul age_range et age_civilite_isp
                    try:
                        age = float(row.get("age",0))
                        bins = [0,18,24,34,44,54,64,74,200]
                        labels = ['0-18','18-24','25-34','35-44','45-54','55-64','65-74','75+']
                        row["age_range"] = next((labels[i] for i in range(len(bins)-1) if bins[i]<=age<bins[i+1]), "O_age")
                    except:
                        row["age_range"] = "O_age"

                    row["gender"] = row.get("gender","O_gender")
                    row["main_isp"] = row.get("main_isp","O_isp")
                    row["age_civilite_isp"] = f"{row['age_range']}_{row['gender']}_{row['main_isp']}"

                    final_rows.append(row)

                # Optimized via endpoint
               # Préparer les tuples attendus par l'API
            rows_for_optimize = [
                (self.safe(r["id_routers"]), self.safe(r["id_focus"]), self.safe(r["ktk_id"]))
                for r in final_rows
            ]

            # Appeler la fonction d'optimisation
            optimized_results = self.resilient_call(self.recuper_optimize, rows_for_optimize)

            # Construire le mapping tuple -> optimized
            optimized_map = {}
            for item in optimized_results:
                # item est un tuple : (id_routers, id_focus, ktk_id, optimized_url)
                if len(item) == 4:
                    key = (self.safe(item[0]), self.safe(item[1]), self.safe(item[2]))
                    optimized_map[key] = item[3] or "url_vide"
                else:
                    # fallback si l'API renvoie seulement des tuples de 3 éléments
                    key = (self.safe(item[0]), self.safe(item[1]), self.safe(item[2]))
                    optimized_map[key] = "url_vide"

            # Mettre à jour final_rows
            for row in final_rows:
                key = (self.safe(row["id_routers"]), self.safe(row["id_focus"]), self.safe(row["ktk_id"]))
                row["optimized"] = optimized_map.get(key, "url_vide")
                row["updated_at"] = datetime.now()

                # Grouper et sommer
                grouped = {}
                for row in final_rows:
                    key = (
                        row.get("database_id"),row.get("dwh_id"), row.get("basename"), row.get("ktk_id"), row.get("segmentId"), row.get("adv_id"),
                        row.get("id_focus"), row.get("id_routers"), row.get("tag_id"), row.get("brand"), row.get("client_id"),
                        row.get("ListId"), row.get("zipcode"), row.get("dep"), row.get("age_range"), row.get("gender"),
                        row.get("main_isp"), row.get("age_civilite_isp")
                    )
                    if key not in grouped:
                        grouped[key] = {
                            "sends":0,"opens":0,"openers":0,"clicks":0,"clickers":0,
                            "removals":0,"complaints":0,"bounces":0,"ca":0,
                            "subject": row.get("subject"),
                            "optimized": row.get("optimized"),
                            "date_event": row.get("date_event"),
                            "date_shedule": row.get("date_shedule",[]),
                            "updated_at": row.get("updated_at")
                        }
                    g = grouped[key]
                    g["sends"] += row.get("sends",0)
                    g["opens"] += row.get("opens",0)
                    g["openers"] = max(g["openers"], row.get("opener",0))
                    g["clicks"] += row.get("clicks",0)
                    g["clickers"] = max(g["clickers"], row.get("clicker",0))
                    g["removals"] += row.get("removals",0)
                    g["complaints"] += row.get("complaints",0)
                    g["bounces"] += row.get("bounces",0)
                    g["ca"] = max(g["ca"], row.get("ca",0))
                    g["date_shedule"] = sorted(set(g["date_shedule"] + row.get("date_shedule",[])))

                # Préparer final pour ClickHouse
                final_list = []
                for k,v in grouped.items():
                    if v["sends"] == 0:
                        continue
                    (db_id,dwh_id,basename, ktk_id, segmentId, adv_id, id_focus, id_routers, tag_id, brand, client_id, ListId, zipcode, dep, age_range, gender, main_isp, age_civilite_isp) = k
                    final_list.append({
                        "database_id": self.safe(db_id),
                        "dwh_id":dwh_id,
                        "basename": basename,
                        "ktk_id": ktk_id,
                        "segmentId": self.safe(segmentId),
                        "adv_id": self.safe(adv_id),
                        "id_focus": self.safe(id_focus),
                        "id_routers": id_routers,
                        "tag_id": self.safe(tag_id),
                        "brand": brand,
                        "client_id": self.safe(client_id),
                        "ListId": ListId,
                        "zipcode": zipcode,
                        "dep": dep,
                        "age_range": age_range,
                        "gender": gender,
                        "main_isp": main_isp,
                        "age_civilite_isp": age_civilite_isp,
                        "sends": v["sends"],
                        "opens": v["opens"],
                        "openers": v["openers"],
                        "clicks": v["clicks"],
                        "clickers": v["clickers"],
                        "removals": v["removals"],
                        "complaints": v["complaints"],
                        "bounces": v["bounces"],
                        "ca": v["ca"],
                        "subject": v["subject"],
                        "optimized": v["optimized"],
                        "date_event": v["date_event"],
                        "date_shedule": v["date_shedule"],
                        "updated_at": v["updated_at"]
                    })

                # Inserer dans ClickHouse par chunk
                # Préparer le batch avant insertion
                for row in final_list:
                    # Colonnes string
                    row["dwh_id"] = str(row.get("dwh_id") or "")
                    row["basename"] = str(row.get("basename") or "")
                    row["id_routers"] = str(row.get("id_routers") or "")
                    row["brand"] = str(row.get("brand") or "")
                    row["dep"] = str(row.get("dep") or "")
                    row["age_range"] = str(row.get("age_range") or "")
                    row["gender"] = str(row.get("gender") or "")
                    row["main_isp"] = str(row.get("main_isp") or "")
                    row["age_civilite_isp"] = str(row.get("age_civilite_isp") or "")
                    row["subject"] = str(row.get("subject") or "")
                    row["zipcode"] = str(row.get("zipcode") or "")
                    row["optimized"] = str(row.get("optimized") or "")

                    # Colonnes int / UInt
                    row["database_id"] = int(row.get("database_id") or 0)
                    row["ktk_id"] = int(row.get("ktk_id") or 0)
                    row["segmentId"] = int(row.get("segmentId") or 0)
                    row["adv_id"] = int(row.get("adv_id") or 0)
                    row["id_focus"] = int(row.get("id_focus") or 0)
                    row["tag_id"] = int(row.get("tag_id") or 0)
                    row["client_id"] = int(row.get("client_id") or 0)
                    row["ListId"] = int(row.get("ListId") or 0)

                    # Colonnes float
                    row["ca"] = float(row.get("ca") or 0.0)

                    # Colonnes datetime
                    if not isinstance(row.get("date_event"), datetime):
                        row["date_event"] = datetime.now()
                    if not isinstance(row.get("updated_at"), datetime):
                        row["updated_at"] = datetime.now()

                    # Colonnes Array(String)
                    if not isinstance(row.get("date_shedule"), list):
                        row["date_shedule"] = []
                    row["date_shedule"] = [str(x) for x in row["date_shedule"]]

                print("insertion")
                
                batch_size = batch_optimize  # taille du batch
                for i in range(0, len(final_list), batch_size):
                    batch_dicts = final_list[i:i+batch_size]

                    # Convertir seulement le batch en DataFrame pour insertion
                    df_batch = pd.DataFrame(batch_dicts)

                    # Insérer dans ClickHouse
                    self.resilient_call(self.clk.insert_df, self.table, df_batch)

                    # Vider la mémoire
                    del df_batch
                    del batch_dicts
                    gc.collect()
                    # Ajouter au journal
                with open(journal, "a") as f:
                    f.write(f"{adv_id}\n")
                    process_adv.add(adv_id)

                print(f"Insertion advertiser {adv_id} terminée")
                self.notifier_info(f"Insertion advertiser {adv_id} terminée")

            except Exception as e:
                msg = f"Erreur globale sur advertiser {adv_id} : {e}"
                print(msg)
                self.notifier_erreur(msg)
                continue

        return final_all
    # ------------------------- Run boucle -------------------------
    def run(self, sleep_sec=60):
        while True:
            try:
                self.report()
                print(f"Nouvelle exécution dans {sleep_sec}s...")
                time.sleep(sleep_sec)
            except Exception as e:
                print(f"erreur : {e}")
                self.notifier_erreur(f"Erreur : {e}")
                print(f"Nouvelle tentative dans {sleep_sec}s...")
                time.sleep(sleep_sec)