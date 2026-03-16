class agentValidation:
    def validation(self, row):
        seen_validate = set()
        for day in row:
            jour = day["jour"]
            for adv in day.get("advertisers", []):
                for reco in adv.get("recommandation", []):
                    key = (adv["advertiser"], reco["base"], reco["segment"], jour) 
                    if key in seen_validate:
                        return False, f"Doublon détecté pour advertiser {adv['advertiser']}, base {reco['base']}, segment {reco['segment']}, jour {jour}"
                    if 'score' in reco:
                        del reco["score"]
                    seen_validate.add(key)
                    heure_str = str(reco["heure"])
                    if ":" in heure_str:
                        hour = int(heure_str.split(":")[0])
                    else:
                        hour = int(heure_str)
                    if hour < 0 or hour > 23:
                        return False, f"Heure invalide pour advertiser {adv['advertiser']}, base {reco['base']}, jour {jour}"
        return True, None