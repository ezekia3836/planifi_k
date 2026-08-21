from models.query2 import Query2
from service.cache import CacheManager
from config.ClickHouseConfig import ClickHouseConfig
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from itertools import product
import time

class CacheBatchService:
    MAX_WORKERS = 2
    CHUNK_SIZE  = 10
    SQL_BATCH   = 500
    TABLE = "clean_reporting"

    SORT_OPTIONS = ["ecpm", "ca", "clicks"]

    @staticmethod
    def _get_rolling_date_range():
        today      = date.today()
        date_end   = str(today.replace(day=1) + relativedelta(months=1) - timedelta(days=1))
        date_start = str(today.replace(day=1) - relativedelta(days=1))
        return str(date_start), str(date_end)

    @staticmethod
    def _get_available_years(query: Query2) -> list[int]:
        rows = query._execute_query(f"""
            SELECT DISTINCT toYear(date_schedule_max) AS year
            FROM clean_reporting
            WHERE date_schedule_max IS NOT NULL
            ORDER BY year
        """)
        return [int(r["year"]) for r in rows if r["year"]]

    @staticmethod
    def _year_range(year: int) -> tuple[str, str]:
        return f"{year}-01-01", f"{year}-12-31"

    @staticmethod
    def _get_all_countries(query: Query2) -> list[str]:
        rows = query._execute_query(
            "SELECT country_code FROM country WHERE country_code IS NOT NULL"
        )
        return [r["country_code"] for r in rows if r["country_code"]]

    def _get_advertiser_tag_ids(
        self, query: Query2, adv_id: int, date_start: str, date_end: str
    ) -> list[int]:
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
        adv_id, tag_id=None, date_schedule=None,
        date_start=None, date_end=None,
        include_o_age=True, include_o_gender=True, include_o_isp=True
    ):
        key  = CacheManager.key("advertiser", adv_id, tag_id, date_schedule, date_start, date_end, include_o_age, include_o_gender, include_o_isp)
        data = CacheManager.get(key)
        if data is not None:
            return data
        data = Query2().global_advertiser(adv_id, tag_id=tag_id, date_schedule=date_schedule, date_start=date_start, date_end=date_end, include_o_age=include_o_age, include_o_gender=include_o_gender, include_o_isp=include_o_isp)
        CacheManager.set(key, data)
        return data

    @staticmethod
    def get_or_compute_base(
        base_id, date_schedule=None, date_start=None, date_end=None,
        include_o_age=True, include_o_gender=True, include_o_isp=True
    ):
        key  = CacheManager.key("database", base_id, date_schedule, date_start, date_end, include_o_age, include_o_gender, include_o_isp)
        data = CacheManager.get(key)
        if data is not None:
            return data
        data = Query2().global_base(base_id, date_schedule=date_schedule, date_start=date_start, date_end=date_end, include_o_age=include_o_age, include_o_gender=include_o_gender, include_o_isp=include_o_isp)
        CacheManager.set(key, data)
        return data

    # ------------------------------------------------------------------ #
    #  Main entry point                                                    #
    # ------------------------------------------------------------------ #

    def run_full_batch(self):
        print("🚀 Starting lightweight cache warmup...")
        start = time.time()
        query = Query2()

        self._warm_global_endpoints()
        self._warm_extra_endpoints()
        self._warm_top_advertisers_and_bases()

        top_adv  = self._top_ids(query, "adv_id",      top=20)
        top_base = self._top_ids(query, "database_id", top=20)
        print(f"🔥 Precaching top {len(top_adv)} adv / {len(top_base)} bases")

        self._warm_ids(top_adv,  mode="adv")
        self._warm_ids(top_base, mode="base")

        print(f"✅ Warmup finished in {time.time() - start:.2f}s")

    # ------------------------------------------------------------------ #
    #  Warm top_advertisers_by_tag + top_10_bases                         #
    # ------------------------------------------------------------------ #

    def _warm_top_advertisers_and_bases(self):
        print("Warming top_advertisers + top_bases endpoints...")
        query     = Query2()
        years     = self._get_available_years(query)
        countries = self._get_all_countries(query)
        today     = date.today()

        if today.year not in years:
            years.append(today.year)

        print(f"  📅 Years to cache: {years}")
        print(f"  🌍 Countries to cache: {countries}")
        print(f"  🔀 Sort options: {self.SORT_OPTIONS}")

        total = len(years) * len(self.SORT_OPTIONS) * len(countries)
        done  = 0

        for year in years:
            date_start, date_end = self._year_range(year)

            for country in countries:

                for sort_by in self.SORT_OPTIONS:

                    # --- top_advertisers_by_tag ---
                    key_adv = CacheManager.key(
                        "top_advertisers_by_tag", None, date_start, date_end, sort_by, country
                    )
                    if CacheManager.get(key_adv) is None:
                        try:
                            data = query.top_advertisers_by_tag(
                                tag_id=None,
                                date_start=date_start,
                                date_end=date_end,
                                sort_by=sort_by,
                                country=country,
                            )
                            CacheManager.set(key_adv, data)
                            print(f"  ✅ top_advertisers sort={sort_by} year={year} country={country}")
                        except Exception as e:
                            print(f"  ❌ top_advertisers sort={sort_by} year={year} country={country}: {e}")
                    else:
                        print(f"  ⏭️  top_advertisers sort={sort_by} year={year} country={country} already cached")

                    time.sleep(0.1)

                    # --- top_10_bases (pas de sort_by, une fois par year+country) ---
                    if sort_by == self.SORT_OPTIONS[0]:
                        key_base = CacheManager.key(
                            "top_base", None, date_start, date_end, country
                        )
                        if CacheManager.get(key_base) is None:
                            try:
                                data = query.top_10_bases(
                                    tag_id=None,
                                    date_start=date_start,
                                    date_end=date_end,
                                    country=country,
                                )
                                CacheManager.set(key_base, data)
                                print(f"  ✅ top_base year={year} country={country}")
                            except Exception as e:
                                print(f"  ❌ top_base year={year} country={country}: {e}")
                        else:
                            print(f"  ⏭️  top_base year={year} country={country} already cached")

                        time.sleep(0.1)

                    done += 1
                    print(f"  📊 Progress: {done}/{total}")

        print(f"  🏁 top_advertisers + top_bases warmup done ({len(years)} years × {len(self.SORT_OPTIONS)} sorts × {len(countries)} countries)")

    # ------------------------------------------------------------------ #
    #  Existing warmup methods                                             #
    # ------------------------------------------------------------------ #

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

        include_options = list(product([False, True], repeat=3))

        for _id in ids:
            try:
                if mode == "adv":
                    tag_ids = self._get_advertiser_tag_ids(query, _id, date_start, date_end)
                    print(f"  📌 advertiser {_id} → {len(tag_ids)} tags: {tag_ids}")

                    for tag_id in tag_ids:
                        for include_o_age, include_o_gender, include_o_isp in include_options:
                            key = CacheManager.key(
                                "advertiser", _id, tag_id, None,
                                date_start, date_end,
                                include_o_age, include_o_gender, include_o_isp
                            )
                            if CacheManager.get(key) is None:
                                data = Query2().global_advertiser(
                                    _id, tag_id=tag_id,
                                    date_start=date_start, date_end=date_end,
                                    include_o_age=include_o_age,
                                    include_o_gender=include_o_gender,
                                    include_o_isp=include_o_isp
                                )
                                CacheManager.set(key, data)
                                print(
                                    f"  ✅ advertiser {_id} tag={tag_id} "
                                    f"(age={include_o_age}, gender={include_o_gender}, isp={include_o_isp}) cached"
                                )
                            time.sleep(0.05)
                else:
                    for include_o_age, include_o_gender, include_o_isp in include_options:
                        key = CacheManager.key(
                            "database", _id, None,
                            date_start, date_end,
                            include_o_age, include_o_gender, include_o_isp
                        )
                        if CacheManager.get(key) is None:
                            data = Query2().global_base(
                                _id,
                                date_start=date_start, date_end=date_end,
                                include_o_age=include_o_age,
                                include_o_gender=include_o_gender,
                                include_o_isp=include_o_isp
                            )
                            CacheManager.set(key, data)
                            print(
                                f"  ✅ database {_id} "
                                f"(age={include_o_age}, gender={include_o_gender}, isp={include_o_isp}) cached"
                            )
                        time.sleep(0.05)

            except Exception as e:
                print(f"  ❌ {mode} {_id} error: {e}")

            time.sleep(0.2)

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
        from models.recommended import Recommended
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

        # Recommend — 3 sorts × tous les pays
        recommended = Recommended()
        query2      = Query2()
        countries   = self._get_all_countries(query2)

        for sort_by in ["ecpm", "clickers", "ca"]:
            for country in countries:
                key = CacheManager.key("recommend", sort_by, country)
                if CacheManager.get(key) is None:
                    try:
                        CacheManager.set(key, recommended.recommend(sort_by=sort_by, country=country))
                        print(f"  ✅ recommend sort={sort_by} country={country} cached")
                    except Exception as e:
                        print(f"  ❌ recommend sort={sort_by} country={country} error:", e)
                    time.sleep(0.2)
                else:
                    print(f"  ⏭️  recommend sort={sort_by} country={country} already cached")

    def _chunk(self, lst, size=None):
        size = size or self.CHUNK_SIZE
        for i in range(0, len(lst), size):
            yield lst[i:i + size]