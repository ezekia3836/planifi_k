from concurrent.futures import ThreadPoolExecutor, as_completed
from models.query2 import Query2
from service.cache import CacheManager
from config.ClickHouseConfig import ClickHouseConfig
import time


class CacheBatchService:
    MAX_WORKERS = 2
    CHUNK_SIZE  = 10
    SQL_BATCH   = 500
    TABLE   = "clean_reporting"

    @staticmethod
    def get_or_compute_advertiser(
        adv_id:        int,
        date_schedule: str = None,
        date_start:    str = None,
        date_end:      str = None,
    ):
        key  = CacheManager.key("advertiser", adv_id, date_schedule, date_start, date_end)
        data = CacheManager.get(key)
        if data is not None:
            return data
        data = Query2().global_advertiser(adv_id, date_schedule, date_start, date_end)
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
        data = Query2().global_base(base_id, date_schedule, date_start, date_end)
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
        for _id in ids:
            try:
                if mode == "adv":
                    key = CacheManager.key("advertiser", _id, None, None, None)
                    if CacheManager.get(key) is not None:
                        continue
                    data = Query2().global_advertiser(_id)
                    CacheManager.set(key, data)
                else:
                    key = CacheManager.key("database", _id, None, None, None)
                    if CacheManager.get(key) is not None:
                        continue
                    data = Query2().global_base(_id)
                    CacheManager.set(key, data)

                print(f"  ✅ {mode} {_id} cached")
            except Exception as e:
                print(f"  ❌ {mode} {_id} error: {e}")

            time.sleep(0.3)

    def _warm_global_endpoints(self):
        print("Warming global endpoints...")
        query     = Query2()
        query.clk = ClickHouseConfig().getClient_prod()

        for name, func, key_args in [
            ("all_advertisers", query.all_advertisers, (None, None, None)),
            ("all_bases",       query.all_bases,       (None, None, None, None, None)),
        ]:
            try:
                CacheManager.set(CacheManager.key(name, *key_args), func())
                print(f"  ✅ {name} cached")
            except Exception as e:
                print(f"  ❌ {name} error:", e)
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