from .data_agent import dataAgent
from .candidate_agent import candidatAgent

class strategyAgent:
    def __init__(self):
        self.jours_actifs = ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi"]
        self.candidat = candidatAgent()
        self.data = dataAgent()  

    def _is_base_sature(self, base):
        return base

    def _is_heure_valid(self, heure):
        h = int(heure.split(":")[0]) if isinstance(heure, str) else int(heure)
        if 0 <= h <= 6:
            h = 9
        elif 6 < h <= 8:
            h = 10
        elif h > 22:
            h = 22
        return f"{h:02d}:00"

    def _is_best_send_time(self, adv_id, tag, top_n=3):
        bests = self.data.best_send_time(adv_id, tag)[:top_n]
        result = []
        for best in bests:
            metric = self.candidat._calcul_metrics(best)
            if not metric:
                continue
            taux_clickers, taux_openers, taux_unsubs = metric
            score = self.candidat._calcul_score(taux_clickers, taux_openers, taux_unsubs, best.get("sends",0))
            candidat = self.candidat._build_candidat(best, score)
            result.append(candidat)
        return result

    def resolve_heure(self, reco):
        heure_init = reco.get("heure")
        h = int(heure_init.split(":")[0]) if isinstance(heure_init, str) else int(heure_init)
        if 8 <= h <= 22:
            return f"{h:02d}:00"
        advertiser = reco.get("advertiser")
        tags = reco.get("tags")
        best_times = self._is_best_send_time(advertiser, tags)
        for bt in best_times:
            h_best = int(bt.get("heure", "08:00").split(":")[0])
            if 8 <= h_best <= 22:
                return f"{h_best:02d}:00"
        return self._is_heure_valid(heure_init)

    def optimize(self, row, n=3):
        result = []
        for day in row:
            slot_occupe = set()
            jour = day.get("jour")
            if jour not in self.jours_actifs:
                continue
            for adv in day.get("advertisers", []):
                recommandations = sorted(adv.get("recommandation", []), key=lambda x: x["score"], reverse=True)
                selection = []
                for reco in recommandations:
                    base = reco.get("base")
                    segment = reco.get("segment")
                    slot = (adv.get("advertiser"), base, segment)
                    reco["base"] = self._is_base_sature(base)
                    reco["heure"] = self.resolve_heure(reco)
                    if slot in slot_occupe:
                        continue
                    slot_occupe.add(slot)
                    reco["rang"] = len(selection) + 1
                    selection.append(reco)
                    if len(selection) >= n:
                        break
                adv["recommandation"] = selection
            result.append(day)
        return result