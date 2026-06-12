from concurrent.futures import ThreadPoolExecutor, as_completed
from models.query2 import Query2
from service.cache import CacheManager
from config.ClickHouseConfig import ClickHouseConfig
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import time


class CacheBatchService:
    MAX_WORKERS = 2
    CHUNK_SIZE  = 10
    SQL_BATCH   = 500
    TABLE       = "clean_reporting"

    @staticmethod
    def _get_rolling_date_range():
        """Plage glissante de 4 mois, se terminant à demain (prêt pour le lendemain matin)."""
        date_end   = date.today() + timedelta(days=1)
        date_start = date_end - relativedelta(months=4)
        return str(date_start), str(date_end)

    @staticmethod
    def _get_all_countries(query: Query2) -> list[str]:
        rows = query._execute_query("SELECT country_code FROM country WHERE country_code IS NOT NULL")
        return [r["country_code"] for r in rows if r["country_code"]]

    def _get_advertiser_tag_ids(self, query: Query2, adv_id: int, date_start: str, date_end: str) -> list[int]:
        """Récupère tous les tag_id distincts pour un advertiser sur la plage glissante."""
        rows = query._execute_query(f"""
            SELECT DISTINCT tag_id
            FROM {self.TABLE}
            WHERE adv_id = {adv_id}
            AND tag_id IS NOT NULL
            AND tag_id != 0
            AND date_schedule_max BETWEEN '{date_start}' AND '{date_end}'
        """)
        return [int(r["tag_id"]) for r in rows if r["tag_id"]]

    @staticmethod
    def get_or_compute_advertiser(
        adv_id:        int,
        tag_id:        int = None,
        date_schedule: str = None,
        date_start:    str = None,
        date_end:      str = None,
    ):
        key  = CacheManager.key("advertiser", adv_id, tag_id, date_schedule, date_start, date_end)
        data = CacheManager.get(key)
        if data is not None:
            return data
        data = Query2().global_advertiser(adv_id, tag_id=tag_id, date_schedule=date_schedule, date_start=date_start, date_end=date_end)
        CacheManager.set(key, data)
        return data

    @staticmethod
    def get_or_compute_base(
        base_id:       int,
        date_schedule: str = None,
        date_start:    str = None,
        date_end:      str = None,
    ):
        key  = CacheManager.key("database", base_id, date_schedule, date_start, date_end)
        data = CacheManager.get(key)
        if data is not None:
            return data
        data = Query2().global_base(base_id, date_schedule=date_schedule, date_start=date_start, date_end=date_end)
        CacheManager.set(key, data)
        return data

    def run_full_batch(self):
        print("🚀 Starting lightweight cache warmup...")
        start = time.time()
        query = Query2()

        self._warm_global_endpoints()
        self._warm_extra_endpoints()

        top_adv  = self._top_ids(query, "adv_id",      top=20)
        top_base = self._top_ids(query, "database_id", top=20)
        print(f"🔥 Precaching top {len(top_adv)} adv / {len(top_base)} bases")

        self._warm_ids(top_adv,  mode="adv")
        self._warm_ids(top_base, mode="base")

        print(f"✅ Warmup finished in {time.time() - start:.2f}s")

    def _top_ids(self, query, column: str, top: int = 20) -> list[int]:
        rows = query._execute_query(f"""
            SELECT
                {column} AS id,
                count() AS weight
            FROM {self.TABLE}
            WHERE {column} IS NOT NULL
            AND {column} != 0
            AND adv_id != 0
            AND toYYYYMM(date_schedule_max) = (
                SELECT max(toYYYYMM(date_schedule_max))
                FROM {self.TABLE}
            )
            GROUP BY {column}
            ORDER BY weight DESC
            LIMIT {top}
        """)
        return [int(r["id"]) for r in rows]

    def _warm_ids(self, ids: list[int], mode: str):
        date_start, date_end = self._get_rolling_date_range()
        query = Query2()

        for _id in ids:
            try:
                if mode == "adv":
                    tag_ids = self._get_advertiser_tag_ids(query, _id, date_start, date_end)
                    print(f"  📌 advertiser {_id} → {len(tag_ids)} tags: {tag_ids}")
                    for tag_id in tag_ids:
                        key = CacheManager.key("advertiser", _id, tag_id, None, date_start, date_end)
                        if CacheManager.get(key) is None:
                            data = Query2().global_advertiser(_id, tag_id=tag_id, date_start=date_start, date_end=date_end)
                            CacheManager.set(key, data)
                            print(f"  ✅ advertiser {_id} tag={tag_id} cached")
                        time.sleep(0.3)

                else:
                    key = CacheManager.key("database", _id, None, date_start, date_end)
                    if CacheManager.get(key) is None:
                        data = Query2().global_base(_id, date_start=date_start, date_end=date_end)
                        CacheManager.set(key, data)
                        print(f"  ✅ database {_id} cached")

            except Exception as e:
                print(f"  ❌ {mode} {_id} error: {e}")

            time.sleep(0.3)

    def _warm_global_endpoints(self):
        print("Warming global endpoints...")
        query     = Query2()
        query.clk = ClickHouseConfig().getClient_prod()

        date_start, date_end = self._get_rolling_date_range()
        print(f"  📅 Rolling range: {date_start} → {date_end}")

        countries = self._get_all_countries(query)
        print(f"  🌍 Found {len(countries)} countries: {countries}")

        for name, func, key_args in [
            (
                "all_advertisers",
                lambda: query.all_advertisers(date_start=date_start, date_end=date_end),
                (None, date_start, date_end, None),
            ),
            (
                "all_bases",
                lambda: query.all_bases(date_start=date_start, date_end=date_end),
                (None, None, date_start, date_end, None),
            ),
        ]:
            try:
                CacheManager.set(CacheManager.key(name, *key_args), func())
                print(f"  ✅ {name} cached ({date_start} → {date_end})")
            except Exception as e:
                print(f"  ❌ {name} error:", e)
            time.sleep(0.5)

        # Par country + plage glissante
        for country in countries:
            for name, func, key_args in [
                (
                    "all_advertisers",
                    lambda c=country: query.all_advertisers(country=c, date_start=date_start, date_end=date_end),
                    (None, date_start, date_end, country),
                ),
                (
                    "all_bases",
                    lambda c=country: query.all_bases(country=c, date_start=date_start, date_end=date_end),
                    (None, None, date_start, date_end, country),
                ),
            ]:
                try:
                    CacheManager.set(CacheManager.key(name, *key_args), func())
                    print(f"  ✅ {name} [{country}] cached ({date_start} → {date_end})")
                except Exception as e:
                    print(f"  ❌ {name} [{country}] error:", e)
                time.sleep(0.5)

    def _warm_extra_endpoints(self):
        print("Warming extra endpoints...")
        query     = Query2()
        query.clk = ClickHouseConfig().getClient_prod()

        tasks = [
            ("segment",   lambda: query.get_segment(None, None), (None, None)),
            ("agences",   lambda: query.get_agences(None),       (None,)),
            ("tags",      lambda: query.get_tags(None),          (None,)),
            ("databases", lambda: query.get_databases(None),     (None,)),
        ]
        for name, func, args in tasks:
            try:
                CacheManager.set(CacheManager.key(name, *args), func())
                print(f"  ✅ {name} cached")
            except Exception as e:
                print(f"  ❌ {name} error:", e)
            time.sleep(0.2)

    def _chunk(self, lst, size=None):
        size = size or self.CHUNK_SIZE
        for i in range(0, len(lst), size):
            yield lst[i:i + size]