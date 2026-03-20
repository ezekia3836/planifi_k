from .data_agent import dataAgent
from .candidate_agent import candidatAgent
from datetime import datetime, timedelta
from collections import defaultdict
from functools import lru_cache


class strategyAgent:
    HEURE_MIN = 8
    HEURE_MAX = 22
    HEURE_DEFAUT = 9
    def __init__(self):
        self.jours_actifs = frozenset(["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi"])
        self.candidat = candidatAgent()
        self.data = dataAgent()
        self._bst_cache: dict[tuple, list[str]] = {}

    def _preload_bst(self, schedule: list):
        pairs: set[tuple] = set()
        for day in schedule:
            for adv in day.get("advertisers", []):
                adv_id = adv.get("advertiser")
                tag    = adv.get("tags")
                if adv_id and tag:
                    pairs.add((adv_id, tag))

        for adv_id, tag in pairs:
            if (adv_id, tag) in self._bst_cache:
                continue 
            try:
                bests = self.data.best_send_time(adv_id, tag)
                self._bst_cache[(adv_id, tag)] = self._best_heures_from_data(bests)
            except Exception as e:
                print(f"[strategyAgent] BST erreur ({adv_id},{tag}) : {e}")
                self._bst_cache[(adv_id, tag)] = []

    def _best_heures_from_data(self, bests: list):
        scored_hours = []
        for b in bests:
            metric = self.candidat._calcul_metrics(b)
            if not metric:
                continue
            taux_clickers, taux_openers, taux_unsubs = metric
            score = self.candidat._calcul_score(
                taux_clickers, taux_openers, taux_unsubs, b.get("sends", 0)
            )
            heure = b.get("heure", "")
            try:
                h = int(str(heure).split(":")[0])
                if self.HEURE_MIN <= h <= self.HEURE_MAX:
                    scored_hours.append((score, f"{h:02d}:00"))
            except (ValueError, TypeError):
                continue
        scored_hours.sort(reverse=True)
        return [h for _, h in scored_hours[:3]]

    def _is_base_sature(self, base):
        return base

    def _is_heure_valid(self, heure):
        try:
            h = int(str(heure).split(":")[0]) if heure else self.HEURE_DEFAUT
        except (ValueError, TypeError):
            return f"{self.HEURE_DEFAUT:02d}:00"
        if h < self.HEURE_MIN:
            h = self.HEURE_DEFAUT
        elif h > self.HEURE_MAX:
            h = self.HEURE_MAX
        return f"{h:02d}:00"

    def resolve_heure(self, reco: dict, adv_id, tag):
        heure_init = reco.get("heure")
        try:
            h = int(str(heure_init).split(":")[0]) if heure_init else None
        except (ValueError, TypeError):
            h = None
        if h is not None and self.HEURE_MIN <= h <= self.HEURE_MAX:
            return f"{h:02d}:00"
        bst_list = self._bst_cache.get((adv_id, tag), [])
        for bst in bst_list:
            if self.HEURE_MIN <= int(bst.split(":")[0]) <= self.HEURE_MAX:
                return bst

        return self._is_heure_valid(heure_init)

    @lru_cache(maxsize=128)
    def _predict_date(self, weekday_name, mois):
        weekday_map = {
            "Lundi": 0, "Mardi": 1, "Mercredi": 2,
            "Jeudi": 3, "Vendredi": 4, "Samedi": 5, "Dimanche": 6
        }
        target_weekday = weekday_map.get(weekday_name)
        if target_weekday is None:
            return None
        try:
            mois = int(mois)
            assert 1 <= mois <= 12
        except (TypeError, ValueError, AssertionError):
            return None

        today = datetime.now().date()
        now = datetime.now()
        year_cible = now.year if mois >= now.month else now.year + 1

        candidate_dates = []
        dt = datetime(year_cible, mois, 1).date()
        while dt.month == mois:
            if dt.weekday() == target_weekday and dt >= today:
                candidate_dates.append(dt)
            dt += timedelta(days=1)

        if candidate_dates:
            return min(candidate_dates).strftime("%d/%m/%Y")

        days_ahead = (target_weekday - today.weekday() + 7) % 7 or 7
        return (today + timedelta(days=days_ahead)).strftime("%d/%m/%Y")

    def optimize(self, schedule: list, n= 3):
        now            = datetime.now()
        mois_courant   = now.month
        annee_courante = now.year
        self._preload_bst(schedule)
        slots_occupes = set()
        by_date= defaultdict(dict)
        for day in schedule:
            jour_name = day.get("jour")
            if jour_name not in self.jours_actifs:
                continue

            for adv in day.get("advertisers", []):
                advertiser = adv.get("advertiser")
                tag = adv.get("tags")

                recommandations = sorted(
                    adv.get("recommandation", []),
                    key=lambda x: float(x.get("score", 0)),
                    reverse=True,
                )
                selection = []
                for reco in recommandations:
                    base  = reco.get("base")
                    age   = reco.get("age")
                    gender  = reco.get("gender")
                    isp  = reco.get("isp")
                    heure_candidates = [reco.get("heure")] + self._bst_cache.get((advertiser, tag), [])
                    heure_finale = None
                    for h in heure_candidates:
                        h_norm = self._is_heure_valid(h)
                        slot = (advertiser, base, age, gender, isp, jour_name, h_norm)
                        if slot not in slots_occupes:
                            slots_occupes.add(slot)
                            heure_finale = h_norm
                            break
                    if not heure_finale:
                        continue  

                    mois = reco.get("mois")
                    predicted_date = self._predict_date(jour_name, mois)
                    try:
                        dt = datetime.strptime(predicted_date, "%d/%m/%Y")
                        if dt.month != mois_courant or dt.year != annee_courante:
                            continue  
                    except ValueError:
                        continue
                    if not predicted_date:
                        continue
                    selection.append({
                        "base":     self._is_base_sature(base),
                        "age":      age,
                        "gender":   gender,
                        "isp":      isp,
                        "country":  reco.get("country"),
                        "currency": reco.get("currency"),
                        "heure":    heure_finale,
                        "score":    reco.get("score")
                    })

                    if len(selection) >= n:
                        break

                if not selection:
                    continue
                if advertiser in by_date[predicted_date]:
                    by_date[predicted_date][advertiser]["recommandation"].extend(selection)
                else:
                    by_date[predicted_date][advertiser] = {
                        "advertiser":     advertiser,
                        "tags":           tag,
                        "recommandation": selection,
                    }

        def parse_date(d: str):
            try:
                return datetime.strptime(d, "%d/%m/%Y")
            except ValueError:
                return datetime.max

        return [
            {
                "jour":date,
                "advertisers": list(advs.values()),
            }
            for date, advs in sorted(
                by_date.items(),
                key=lambda x: parse_date(x[0])
            )
        ]