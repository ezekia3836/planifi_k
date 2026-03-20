class agentValidation:
    VALID_GENDERS = {"M", "F", "O_gender"}
    VALID_AGE_CLASSES = {"18","0-18","18-24","25-34","35-44","45-54","55-64","65-74","75+","O_age"}
    def validation(self, row):
        seen_validate = set()
        errors = []
        for day in row:
            jour = day.get("jour")
            if not jour:
                errors.append("Champ 'jour' manquant")
            for adv in day.get("advertisers", []):
                if "tags" not in adv or not adv["tags"]:
                    errors.append(f"Advertiser {adv.get('advertiser')} : champ 'tags' manquant")
                if "stat_campaign_id" not in adv and "advertiser" not in adv:
                    errors.append(f"Advertiser {adv.get('advertiser')} : 'stat_campaign_id' manquant")
                for reco in adv.get("recommandation", []):
                    key = (adv["advertiser"], reco["base"], reco.get("age"), reco.get("gender"), reco.get("isp"), jour)
                    if key in seen_validate:
                        errors.append(f"Doublon détecté pour advertiser {adv['advertiser']}, base {reco['base']}, jour {jour}")
                    seen_validate.add(key)
                    for field in ["base","age","gender","isp","country","currency","heure"]:
                        if field not in reco:
                            errors.append(f"Advertiser {adv['advertiser']}, base {reco.get('base')}: champ '{field}' manquant")
                    if reco.get("gender") and reco["gender"] not in self.VALID_GENDERS:
                        errors.append(f"Advertiser {adv['advertiser']}, base {reco.get('base')}: gender invalide {reco['gender']}")
                    if reco.get("age") and reco["age"] not in self.VALID_AGE_CLASSES:
                        errors.append(f"Advertiser {adv['advertiser']}, base {reco.get('base')}: age invalide {reco['age']}")
        if errors:
            return False, errors
        return True, None