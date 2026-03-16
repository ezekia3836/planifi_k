class strategyAgent:
    def optimize(self, row, n=3):
        for day in row:
            slot_occupe=set()
            for adv in day.get("advertisers", []):
                recommandations = adv.get("recommandation", [])
                advertiser = adv.get("advertiser")
                recommandations = sorted(recommandations, key=lambda x: x["score"], reverse=True)
                selection=[]
                rang=0
                for reco in recommandations:
                    rang +=1
                    base = reco.get("base")
                    segment = reco.get("segment")
                    slot = (advertiser,base,segment)
                    if slot in slot_occupe:
                        continue
                    slot_occupe.add(slot)
                    reco["rang"]=len(selection)+1
                    selection.append(reco)
                    if len(selection)>=n:
                        break
                adv["recommandation"] = selection
        return row