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
                    seen_validate.add(key)
        return True, None