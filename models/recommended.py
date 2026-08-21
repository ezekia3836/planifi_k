from models.query2 import Query2
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

SORT_OPTIONS = {
    "ecpm":     "ecpm DESC, click_rate DESC",
    "clickers": "click_rate DESC, ecpm DESC",
    "ca":       "total_ca DESC, ecpm DESC",
}
DEFAULT_SORT = "ecpm"


class Recommended(Query2):
    def __init__(self):
        super().__init__()
    @staticmethod
    def _resolve_order(sort_by: str) -> str:
        sort_by = sort_by.lower()
        if sort_by not in SORT_OPTIONS:
            raise ValueError(
              f"sort_by invalide : '{sort_by}'. "
                f"Valeurs acceptées : {list(SORT_OPTIONS)}"
            )
        return SORT_OPTIONS[sort_by]

    def _build_top_tags_query(self, where_clause, order_by, min_sends=1000, limit=10):
        return f"""
        WITH focus_agg AS (
            SELECT
                t.tag_id,
                t.id_focus,
                sum(t.sends)            AS total_sends,
                countIf(t.opens > 0)    AS total_openers,
                countIf(t.clickers > 0) AS total_clickers,
                countIf(t.unsubs > 0)   AS total_unsubs,
                max(t.ca)               AS ca_focus
            FROM {self.table} t
            LEFT JOIN country c ON c.id = t.country
            WHERE {where_clause}
                AND t.tag_id      != 0
                AND t.adv_id      != 0
                AND t.database_id != 0
            GROUP BY t.tag_id, t.id_focus
        ),
        stats AS (
            SELECT
                tag_id,
                sum(total_sends)    AS sends,
                sum(total_openers)  AS openers,
                sum(total_clickers) AS clickers,
                sum(total_unsubs)   AS unsubs,
                sum(ca_focus)       AS total_ca,
                round(sum(total_openers)  / nullIf(sum(total_sends), 0) * 100,  2) AS open_rate,
                round(sum(total_clickers) / nullIf(sum(total_sends), 0) * 100,  2) AS click_rate,
                round(sum(total_clickers) / nullIf(sum(total_openers), 0) * 100, 2) AS taux_cto,
                round(sum(total_unsubs)   / nullIf(sum(total_sends), 0) * 100,  2) AS taux_unsubs,
                round(sum(ca_focus) / nullIf(sum(total_sends), 0) * 1000, 2)        AS ecpm,
                round(
                    (sum(ca_focus) / nullIf(sum(total_sends), 0) * 1000)
                    * log10(sum(total_clickers) + 1), 2
                ) AS weighted_score
            FROM focus_agg
            GROUP BY tag_id
        )
        SELECT
            s.*,
            tg.tag AS tag_name,
            row_number() OVER (ORDER BY {order_by}) AS rank
        FROM stats s
        LEFT JOIN tags tg ON tg.id = s.tag_id
          
        ORDER BY rank
        LIMIT {limit}
        """

    def _build_top_advertisers_for_tags_query(
        self, where_clause, tag_ids, order_by, min_sends=500, limit=10
    ):
        tag_ids_sql = ", ".join(str(t) for t in tag_ids)
        return f"""
        WITH focus_agg AS (
            SELECT
                t.tag_id,
                t.adv_id,
                t.id_focus,
                sum(t.sends)            AS total_sends,
                countIf(t.opens > 0)    AS total_openers,
                countIf(t.clickers > 0) AS total_clickers,
                countIf(t.unsubs > 0)   AS total_unsubs,
                max(t.ca)               AS ca_focus
            FROM {self.table} t
            LEFT JOIN country c ON c.id = t.country
            WHERE {where_clause}
                AND t.tag_id IN ({tag_ids_sql})
                AND t.adv_id      != 0
                AND t.database_id != 0
            GROUP BY t.tag_id, t.adv_id, t.id_focus
        ),
        stats AS (
            SELECT
                tag_id,
                adv_id,
                sum(total_sends)    AS sends,
                sum(total_openers)  AS openers,
                sum(total_clickers) AS clickers,
                sum(total_unsubs)   AS unsubs,
                sum(ca_focus)       AS total_ca,
                round(sum(total_openers)  / nullIf(sum(total_sends), 0) * 100,  2) AS open_rate,
                round(sum(total_clickers) / nullIf(sum(total_sends), 0) * 100,  2) AS click_rate,
                round(sum(total_clickers) / nullIf(sum(total_openers), 0) * 100, 2) AS taux_cto,
                round(sum(total_unsubs)   / nullIf(sum(total_sends), 0) * 100,  2) AS taux_unsubs,
                round(sum(ca_focus) / nullIf(sum(total_sends), 0) * 1000, 2)        AS ecpm,
                round(
                    (sum(ca_focus) / nullIf(sum(total_sends), 0) * 1000)
                    * log10(sum(total_clickers) + 1), 2
                ) AS weighted_score
            FROM focus_agg
            GROUP BY tag_id, adv_id
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY tag_id
                    ORDER BY {order_by}
                ) AS rank
            FROM stats WHERE sends >= {min_sends}
              
        )
        SELECT
            r.*,
            a.name AS adv_name
        FROM ranked r
        LEFT JOIN advertiser a ON a.id = r.adv_id
        WHERE rank <= {limit}
        ORDER BY tag_id, rank
        """

    def _build_all_bases_query(
        self, where_clause, tag_adv_pairs, order_by, limit=10, min_sends=500
    ):
        pairs_sql = ", ".join(
            f"({tag_id}, {adv_id})" for tag_id, adv_id in tag_adv_pairs
        )
        return f"""
        WITH focus_agg AS (
            SELECT
                t.tag_id,
                t.adv_id,
                t.database_id,
                t.id_focus,
                sum(t.sends)            AS total_sends,
                countIf(t.opens > 0)    AS total_openers,
                countIf(t.clickers > 0) AS total_clickers,
                countIf(t.unsubs > 0)   AS total_unsubs,
                max(t.ca)               AS ca_focus
            FROM {self.table} t
            LEFT JOIN country c ON c.id = t.country
            WHERE {where_clause}
                AND (t.tag_id, t.adv_id) IN ({pairs_sql})
                AND t.database_id != 0
            GROUP BY t.tag_id, t.adv_id, t.database_id, t.id_focus
        ),
        stats AS (
            SELECT
                tag_id,
                adv_id,
                database_id,
                sum(total_sends)    AS sends,
                sum(total_openers)  AS openers,
                sum(total_clickers) AS clickers,
                sum(total_unsubs)   AS unsubs,
                sum(ca_focus)       AS total_ca,
                round(sum(total_openers)  / nullIf(sum(total_sends), 0) * 100,  2) AS open_rate,
                round(sum(total_clickers) / nullIf(sum(total_sends), 0) * 100,  2) AS click_rate,
                round(sum(total_clickers) / nullIf(sum(total_openers), 0) * 100, 2) AS taux_cto,
                round(sum(total_unsubs)   / nullIf(sum(total_sends), 0) * 100,  2) AS taux_unsubs,
                round(sum(ca_focus) / nullIf(sum(total_sends), 0) * 1000, 2)        AS ecpm,
                round(
                    (sum(ca_focus) / nullIf(sum(total_sends), 0) * 1000)
                    * log10(sum(total_clickers) + 1), 2
                ) AS weighted_score
            FROM focus_agg
            GROUP BY tag_id, adv_id, database_id
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY tag_id, adv_id
                    ORDER BY {order_by}
                ) AS rank
            FROM stats WHERE sends >={min_sends}
            
        )
        SELECT
            r.*,
            d.basename AS database_name
        FROM ranked r
        LEFT JOIN databases d ON d.id = r.database_id
        WHERE rank <= {limit}
        ORDER BY tag_id, adv_id, rank
        """


    def _get_where_clauses(self, country: str = 'FR'):
        today = date.today()
        current_month = today.month
        next_month = (current_month % 12) + 1
        current_year = today.year
        date_start_current = f"{current_year}-{current_month:02d}-01"
        date_end_current = today.strftime("%Y-%m-%d")

        return (
            f"date_schedule_max BETWEEN '{date_start_current}' AND '{date_end_current}' AND c.country_code = '{country}'",
            f"toMonth(date_schedule_max) = {next_month} AND c.country_code = '{country}'",
        )

    # ------------------------------------------------------------------ #
    #  Row formatter                                                       #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _fmt_base_stats(r):
        if r["sends"] < r["clickers"] or r["sends"] < r["openers"] or r["sends"] < r["unsubs"] or r["sends"]==0:
            return None
        return {
            "rank":           r["rank"],
            "sends":          r["sends"],
            "openers":        r["openers"] or 0,
            "clickers":       r["clickers"] or 0,
            "unsubs":         r["unsubs"] or 0,
            "total_ca":       r["total_ca"] or 0,
            "open_rate":      r["open_rate"] or 0,
            "click_rate":     r["click_rate"] or 0,
            "taux_cto":       r["taux_cto"] or 0,
            "taux_unsubs":    r["taux_unsubs"] or 0,
            "ecpm":           r["ecpm"] or 0,
            "weighted_score": r["weighted_score"],
        }


    def _build_hierarchy(self, where_clause, order_by):
        top_tags = self._execute_query(
            self._build_top_tags_query(where_clause, order_by)
        )
        if not top_tags:
            return []

        tag_ids = [r["tag_id"] for r in top_tags]

        all_advs = self._execute_query(
            self._build_top_advertisers_for_tags_query(where_clause, tag_ids, order_by)
        )

        advs_by_tag = defaultdict(list)
        for row in all_advs:
            advs_by_tag[row["tag_id"]].append(row)

        tag_adv_pairs = [(row["tag_id"], row["adv_id"]) for row in all_advs]

        bases_by_tag_adv = defaultdict(list)
        if tag_adv_pairs:
            all_bases = self._execute_query(
                self._build_all_bases_query(where_clause, tag_adv_pairs, order_by)
            )
            for b in all_bases:
                bases_by_tag_adv[(b["tag_id"], b["adv_id"])].append(b)

        result = []
        for tag_row in top_tags:
            tag_stats = self._fmt_base_stats(tag_row)
            if tag_stats is None:
                continue  # on ignore les tags qui échouent au test de cohérence

            tag_id = tag_row["tag_id"]
            tag_entry = {
                **tag_stats,
                "tag_id":      tag_id,
                "tag_name":    tag_row["tag_name"],
                "advertisers": [],
            }
            for adv_row in advs_by_tag.get(tag_id, []):
                adv_stats = self._fmt_base_stats(adv_row)
                if adv_stats is None:
                    continue  # on ignore les advertisers qui échouent au test

                adv_id = adv_row["adv_id"]
                bases = []
                for b in bases_by_tag_adv.get((tag_id, adv_id), []):
                    base_stats = self._fmt_base_stats(b)
                    if base_stats is None:
                        continue  # on ignore les bases qui échouent au test
                    bases.append({
                        **base_stats,
                        "database_id":   b["database_id"],
                        "database_name": b["database_name"],
                    })

                adv_entry = {
                    **adv_stats,
                    "adv_id":   adv_id,
                    "adv_name": adv_row["adv_name"],
                    "bases": bases,
                }
                tag_entry["advertisers"].append(adv_entry)
            result.append(tag_entry)

        return result
    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def recommend(self, sort_by: str = DEFAULT_SORT, country: str = 'FR'):
        order_by = self._resolve_order(sort_by)
        where_current, where_next = self._get_where_clauses(country)
        today = date.today()
        current_month = today.month
        next_month = (current_month % 12) + 1

        with ThreadPoolExecutor(max_workers=2) as executor:
            f_current = executor.submit(self._build_hierarchy, where_current, order_by)
            f_next    = executor.submit(self._build_hierarchy, where_next,    order_by)
            data_current = f_current.result()
            data_next    = f_next.result()

        return {
            "sort_by": sort_by,
            "country": country,
            "current_month": {
                "month": current_month,
                "year":  today.year,
                "data":  data_current,
            },
            "next_month": {
                "month": next_month,
                "data":  data_next,
            },
        }