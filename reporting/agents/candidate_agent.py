from numpy import log1p
class candidatAgent:
    def __init__(self):
        self.days_maps={
            1:"Lundi",
            2:"Mardi",
            3:"Mercredi",
            4:"Jeudi",
            5:"Vendredi",
            6:"Samedi",
            7:"Dimanche"
        }

    def _calcul_metrics(self,row):
        sends = row["sends"]
        if sends==0:
            return None
        taux_clickers = row["clickers"]/sends
        taux_openers = row["openers"]/sends
        taux_unsubs = row["unsubs"]/sends
        return taux_clickers,taux_openers,taux_unsubs
    
    def _calcul_score(self,taux_clickers,taux_openers,taux_unsubs,sends):
        score = (taux_clickers*0.5+taux_openers*0.3-taux_unsubs*0.3)*log1p(sends)
        return round(score,4)
    
    def _build_candidat(self,row,score):
        return{
            "advertiser":row["adv_id"],
            "base":row["database_id"],
            "segment":row["segment"],
            "tag":row["tag"],
            "jour":self.days_maps[row["day"]],
            "heure":f"{int(row['hour']):02d}:00",
            "score":score
        }
    
    def _select_best(self,candidats):
        best={}
        for c in candidats:
            key=(c["advertiser"],c["segment"],c["jour"])
            if key not in best or c["score"]>best[key]["score"]:
                best[key]=c
        return list(best.values())
    
