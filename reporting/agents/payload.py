from datetime import datetime
from locale import currency
class Payload:
    def build_payload(self,row):
        payloads=[]
        for r in row:
            date_str = r.get("jour")
            
            for adv in r.get("advertisers",[]):
                tag = adv.get("tags")
                advertiser = adv.get("advertiser")
                for reco in adv.get("recommandation",[]):
                    heure = reco.get("heure")
                    try:
                        date_heure = datetime.strptime(f"{date_str} {heure}","%d/%m/%Y %H:%M")
                        shoot_date = date_heure.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        print(f"[Payload] Erreur date : date_str='{date_str}' heure='{heure}' → {e}")
                        continue
                    gender = reco.get("gender")
                    isp = reco.get("isp")
                    age = reco.get("age")
                    gender_list = [gender] if gender else None
                    age_list = [age] if age else None
                    payload={
                        "tag":tag,
                        "stat_base_id":str(reco.get("base")),
                        "stat_campaign_id":str(advertiser),
                        "shoot_date":shoot_date,
                        "revenue_type":"",
                        "revenue_valeu":"",
                        "country":reco.get("country"),
                        "currency":reco.get("currency"),
                        "fed_router_immediately":"",
                        "gender":gender_list,
                        "age_class":age_list,
                        "isp_domain_family":isp
                        
                    }
                    payloads.append(payload)
        return payloads