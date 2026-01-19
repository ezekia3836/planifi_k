def report(self):
        rows = []

        df_clk = self.recupere_clk(advertiser=self.adv_ids)
        df_pg = self.recupere_pst(advertiser=self.adv_ids)

        df_pg = df_pg.explode('id_routers')
        df_clk['id_routers'] = df_clk['id_routers'].astype(str)
        df_pg['id_routers'] = df_pg['id_routers'].astype(str)
        df_pg = df_pg.rename(columns={'advertiser': 'adv_id'})

        df = df_clk.merge(df_pg, on='id_routers', how='inner')

        # Age
        bins = [0,18,24,34,44,54,64,74,200]
        labels = ['0-18','18-24','25-34','35-44','45-54','55-64','65-74','75+']
        df['age_range'] = pd.cut(df['age'], bins=bins, labels=labels)
        df['age_range'] = df['age_range'].cat.add_categories('O_age').fillna('O_age').astype(str)

        df['gender'] = df['gender'].fillna('O_gender').replace({'O':'O_gender'})
        df['main_isp'] = df['main_isp'].fillna('O_isp').replace({'Other':'O_isp'})

        # Events
        for ev in ['Sends','Opens','Clicks','Removals']:
            df[ev] = (df['event_type'] == ev).astype(int)

        
        for keys, group in df.groupby(['database_id','id_routers','tag_id','tag_name','adv_id','advertiser_name']):
            db_id, router, tag_id, tag_name, adv_id, advertiser_name = keys

            for dim, dim_name in [('gender','civilite'),('age_range','age'),('main_isp','isp')]:
                for value, sub in group.groupby(dim):
                    clicks = float(sub['Clicks'].sum())
                    ca = float(sub['ca'].sum())
                    sends = float(sub['Sends'].sum())
                    ecpm = round((ca/100) / sends*1000,3)
                    desabo = float(sub['Removals'].sum())
                    taux_clicks=round(clicks/sends *100,2)
                    taux_desabo = round(desabo / sends *100,2)
                    rows.append({
                        'database_id': int(db_id),
                        'id_routers': int(router),
                        'tag_id': int(tag_id),
                        'tag_name': tag_name,
                        'adv_id': int(adv_id),
                        'advertiser_name': advertiser_name,
                        'dimension': dim_name,
                        'dim_content': str(value),
                        'sends': int(sub['Sends'].sum()),
                        'opens': int(sub['Opens'].sum()),
                        'clicks': int(sub['Clicks'].sum()),
                        'removals': int(sub['Removals'].sum()),
                        'ca': ca,
                        'ecpm':ecpm,
                        'taux_clickers':taux_clicks,
                        'taux_desabo':taux_desabo,
                        'created_at': self.created_at
                    })

            # age_civilite_isp
            for (a,g,i), sub in group.groupby(['age_range','gender','main_isp']):
                 clicks = float(sub['Clicks'].sum())
                 ca = float(sub['ca'].sum())
                 sends = float(sub['Sends'].sum())
                 ecpm = round((ca/100) / sends*1000,3)
                 desabo = float(sub['Removals'].sum())
                 taux_clicks=round(clicks/sends *100,2)
                 taux_desabo = round(desabo / sends *100,2)
                 rows.append({
                    'database_id': int(db_id),
                    'id_routers': int(router),
                    'tag_id': int(tag_id),
                    'tag_name': tag_name,
                    'adv_id': int(adv_id),
                    'advertiser_name': advertiser_name,
                    'dimension': 'age_civilite_isp',
                    'dim_content': f"{a}_{g}_{i}",
                    'sends': int(sub['Sends'].sum()),
                    'opens': int(sub['Opens'].sum()),
                    'clicks': int(sub['Clicks'].sum()),
                    'removals': int(sub['Removals'].sum()),
                    'ca': ca,
                    'ecpm':ecpm,
                    'taux_clickers':taux_clicks,
                    'taux_desabo':taux_desabo,
                    'created_at': self.created_at
                })
        
        for adv_id, group in df.groupby('adv_id'):
            for db_id, sub in group.groupby('database_id'):
                clicks = float(sub['Clicks'].sum())
                ca = float(sub['ca'].sum())
                sends = float(sub['Sends'].sum())
                ecpm = round((ca/100) / sends*1000,3)
                desabo = float(sub['Removals'].sum())
                taux_clicks=round(clicks/sends *100,2)
                taux_desabo = round(desabo / sends *100,2)
                rows.append({
                    'database_id': 0,
                    'id_routers': 0,
                    'tag_id': 0,
                    'tag_name': '',
                    'adv_id': int(adv_id),
                    'advertiser_name': group['advertiser_name'].iloc[0],
                    'dimension': 'global_advertiser',
                    'dim_content': '',
                    'sends': int(sub['Sends'].sum()),
                    'opens': int(sub['Opens'].sum()),
                    'clicks': int(sub['Clicks'].sum()),
                    'removals': int(sub['Removals'].sum()),
                    'ca': ca,
                    'ecpm':ecpm,
                    'taux_clickers':taux_clicks,
                    'taux_desabo':taux_desabo,
                    'created_at': self.created_at
                })

        
        for db_id, group in df.groupby('database_id'):
             clicks = float(group['Clicks'].sum())
             ca = float(group['ca'].sum())
             sends = float(group['Sends'].sum())
             ecpm = round((ca/100) / sends*1000,3)
             desabo = float(group['Removals'].sum())
             taux_clicks=round(clicks/sends *100,2)
             taux_desabo = round(desabo / sends *100,2)
             rows.append({
                'database_id': int(db_id),
                'id_routers': 0,
                'tag_id': 0,
                'tag_name': '',
                'adv_id': 0,
                'advertiser_name': '',
                'dimension': 'global_base',
                'dim_content': '',
                'sends': int(group['Sends'].sum()),
                'opens': int(group['Opens'].sum()),
                'clicks': int(group['Clicks'].sum()),
                'removals': int(group['Removals'].sum()),
                'ca': ca,
                'ecpm':ecpm,
                'taux_clickers':taux_clicks,
                'taux_desabo':taux_desabo,
                'created_at': self.created_at
            })
        df_pg.to_csv('aff.csv',index=False,sep=';')
        df_final = pd.DataFrame(rows)
        for col in ['database_id','tag_id','adv_id','sends','opens','clicks','removals']:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype('Int64')
        """for col in ['id_routers','tag_name','advertiser_name']:
            df_final[col] = df_final[col].astype(str).fillna('')
        #df_final.to_csv('all.csv',index=False,sep=';')
        self.clk.insert_df('reporting1', df_final)
        print("Reporting inséré")"""
