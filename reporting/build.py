import pandas as pd
from reporting.analyze import analyse

class build:
    def __init__(self):
        self.analyze=analyse()

    def safe_str(self, value):
        return str(value).encode("utf-8", errors="replace").decode("utf-8")
    

    def make_data(self,df):
        data={}
        sends = int(df['Sends'].sum())
        opens = int(df['Opens'].sum())
        clicks = int(df['Clicks'].sum())
        unsubs = int(df['Removals'].sum())

        opens = min(opens, sends)
        clicks = min(clicks, sends)

        if sends > 0:
            taux_openers = round((opens / sends) * 100, 3) if sends else 0.0
            taux_clickers = round((clicks / sends) * 100, 3) if sends else 0.0
            taux_unsubs = round((unsubs / sends) * 100, 3) if sends else 0.0
        else:
            taux_openers = taux_clickers = taux_unsubs = 0.0

        if opens > 0:
            cto_rate = round((clicks / opens) * 100, 3)
        else:
            cto_rate = 0.0
        data['sends']=sends
        data['taux_clickers']=taux_clickers
        data['taux_desabo']=taux_unsubs
        data['taux_cto']=cto_rate
        
        analyse={
            "taux_click":self.analyze.analyze_click_rate(taux_clickers),
            "taux_cto":self.analyze.analyze_cto_rate(cto_rate,opens),
            "taux_desabo":self.analyze.analyze_unsub_rate(taux_unsubs)
        }
        data['analyse']=analyse
        return data
    

    def build_dim(self,group, dim, make_data, safe_key, max_items=None):
        result = {}
        for k, sub in group.groupby(dim):
            data = make_data(sub)
            if not data or data.get('sends', 0) <= 0:
                continue
            result[safe_key(k)] = data
            if max_items and len(result) >= max_items:
                break
        return result
    


    def build_comb(self,group, make_data, safe_key):
        result = {}
        for (a, g, i), sub in group.groupby(['age_range', 'gender', 'main_isp']):
            data = make_data(sub)
            if not data or data.get('sends', 0) <= 0:
                continue
            key = f"{safe_key(a)}_{safe_key(g)}_{safe_key(i)}"
            result[key] = data
        return result
    

    def build_global_avertiser(self, group):
        data = {}
        dbs={}
        for db_id, db_group in group.groupby('database_id'):
            total_sent = float(db_group['Sends'].sum())
            total_ca = float(db_group['ca'].sum())

            ecpm = round((total_ca / total_sent) * 1000, 3) if total_sent else 0.0

            dbs[str(db_id)] = {
                "ecpm": ecpm
            }

        for dim, name in [('gender', 'civilite'),('age_range', 'age'),('main_isp', 'isp')]:
            dim_data = self.build_dim( group,dim=dim,make_data=self.make_data,safe_key=self.safe_str)
            if dim_data:
                data[name] = dim_data
            comb = self.build_comb(group,make_data=self.make_data,safe_key=self.safe_str)
            if comb:
                data['age_civilite_isp'] = comb
        total_sent = float(group['Sends'].sum())
        total_clicks = float(group['Clicks'].sum())
        total_opens = float(group['Opens'].sum())
        total_unsubs = float(group['Removals'].sum())
        total_ca = float(group['ca'].sum())

        clk = round(total_clicks / total_sent * 100, 3) if total_sent else 0.0
        taux_opens = round(total_opens / total_sent * 100, 3) if total_sent else 0.0
        taux_desabo = round(total_unsubs / total_sent * 100, 3) if total_sent else 0.0
        cto_rate = round(total_clicks / total_opens * 100, 3) if total_opens else 0.0
        ecpm = round((total_ca / total_sent) * 1000, 3) if total_sent else 0.0

        data['data_global'] = {
            "volume": total_sent,
            "ecpm": ecpm,
            "taux_clicks": clk,
            "taux_opens": taux_opens,
            "taux_cto": cto_rate,
            "taux_desabo": taux_desabo,
            "total_ca": total_ca,
            "analyse": {
                "taux_clicks": self.analyze.analyze_click_rate(clk),
                "taux_desabo": self.analyze.analyze_unsub_rate(taux_desabo),
                "taux_cto": self.analyze.analyze_cto_rate(cto_rate, total_opens),
                "classe":self.analyze.classify_advertiser(ecpm,clk)
            },
            "databases":dbs
        }
        return data
    
    def build_global_db(self, group):
        data = {}
        for dim, name in [('gender', 'civilite'),('age_range', 'age'),('main_isp', 'isp')]:
            dim_data = self.build_dim( group,dim=dim,make_data=self.make_data,safe_key=self.safe_str)
            if dim_data:
                data[name] = dim_data
        comb = self.build_comb(group,make_data=self.make_data,safe_key=self.safe_str)
        if comb:
            data['age_civilite_isp'] = comb

        advs=[]
        for (adv_id,advertiser_name), db_group in group.groupby(['adv_id','advertiser_name']):
            total_sent = float(db_group['Sends'].sum())
            total_clicks = float(db_group['Clicks'].sum())
            total_ca = float(db_group['ca'].sum())

            clicks = round(total_clicks / total_sent * 100, 3) if total_sent else 0.0
            ecpm = round((total_ca / total_sent) * 1000, 3) if total_sent else 0.0

            advs.append({
                "name":advertiser_name,
                "classe":self.analyze.classify_advertiser(ecpm,clicks)
            })

        total_sent = float(group['Sends'].sum())
        total_clicks = float(group['Clicks'].sum())
        total_opens = float(group['Opens'].sum())
        total_unsubs = float(group['Removals'].sum())
        total_ca = float(group['ca'].sum())

        clicks = round(total_clicks / total_sent * 100, 3) if total_sent else 0.0
        taux_opens = round(total_opens / total_sent * 100, 3) if total_sent else 0.0
        taux_desabo = round(total_unsubs / total_sent * 100, 3) if total_sent else 0.0
        cto_rate = round(total_clicks / total_opens * 100, 3) if total_opens else 0.0
        ecpm = round((total_ca / total_sent) * 1000, 3) if total_sent else 0.0

        data['data_global'] = {
            "volume": total_sent,
            "ecpm": ecpm,
            "taux_clicks": clicks,
            "taux_opens": taux_opens,
            "taux_cto": cto_rate,
            "taux_desabo": taux_desabo,
            "total_ca": total_ca,
            "analyse": {
                "taux_clicks": self.analyze.analyze_click_rate(clicks),
                "taux_desabo": self.analyze.analyze_unsub_rate(taux_desabo),
                "taux_cto": self.analyze.analyze_cto_rate(cto_rate, total_opens),
                "advertisers":set(advs)
            }
        }
        return data