import re
import hashlib
import pandas as pd
from hashlib import sha256,md5
from datetime import datetime
import numpy as np

ISP_FILTER  = {
    "sfr" : ["9business.fr","9online.fr","akeonet.com","cario.fr","cegetel.net","club-internet.fr","club.fr","clubinternet.fr","estvideo.fr","fnac.net","mageos.com","modulonet.fr","neuf.fr","noos.fr","numericable.com","numericable.fr","sfr.fr","waika9.com"],
    "apple" : ["icloud.com","me.com","mac.com"],
    "bouygues" : ["bbox.fr"],
    "british internet" : ["btinternet.com"],
    "free" : ["free.fr","libertysurf.fr","infonie.fr","chez.com","aliceadsl.fr","freesbee.fr","worldonline.fr","online.fr","alicepro.fr","nomade.fr"],
    "gmail" : ["gmail.com"],
    "italiaonline" : ["libero.it","virgilio.it"],
    "laposte" : ["laposte.net"],
    "orange": ["orange.fr","wanadoo.fr","martinetelec.com","elsil.fr","cabinetcourty.com","stebdx.fr"],
    "outlook" : ["50pas972.com","aces-sobesky.fr","acfci.cci.fr","achard-sa.com","actemium.com","actes-sud.fr","adecco.fr","adisseo.com","advanta.fr","aforp.fr","agss.fr","akanea.com","altareacogedim.com","alvs.fr","ampvisualtv.tv","arcadie-so.com","arminaeurope.com","aviapartner.aero","belambra.fr","betchoulet.fr","bms.com","bouyguestelecom.fr","cabinet-taboni.fr","cegelec.com","cegid.fr","cern.ch","cerp-rouen.fr","cfa-afmae.fr","cfecgc.fr","ch-wasquehal.fr","chateauform.com","cheval-sa.com","chinasealeh.com","chpolansky.fr","cneap.fr","cokecce.com","conair.com","cr-champagne-ardenne.fr","creativenetwork.fr","croissy.com","daher.com","darty.fr","dartybox.com","davines.it","dbmail.com","delachaux.fr","desenfans.com","drancy.fr","ece-france.com","ecole-eme.fr","edhec.com","edhec.edu","edu.esce.fr","effem.com","egidys.com","ehtp.fr","eiffage.com","esatea.net","esc-larochelle.fr","esc-pau.net","esitc-caen.net","et.esiea.fr","ets-bernard.com","ets-verhaeghe.fr","etu.u-pec.fr","eurocast.fr","evun360.onmicrosoft.com","expanscience.com","expertiscfe.fr","fft.fr","fidel-fillaud.com","fr.tmp.com","france-boissons.fr","gacd.fr","gfi.fr","gl-events.com","gondrand.fr","goodyear.com","grandhainaut.cci.fr","grandlebrun.com","grassavoye.com","groupe-crit.com","groupeflo.fr","groupeisf.com","haut-rhin.fr","hoparb.com","hotmail.be","hotmail.ca","hotmail.ch","hotmail.co.kr","hotmail.co.uk","hotmail.com","hotmail.com.tr","hotmail.de","hotmail.es","hotmail.fr","hotmail.gr","hotmail.it","icfenvironnement.com","immodefrance.com","inseec-france.com","inseec.com","ipag.fr","ipsen.com","ipsos.com","isipharm.fr","jcdava.com","joffeassocies.com","kedgebs.com","lamutuellegenerale.fr","lesbougiesdefrance.fr","lespinet.com","lgh.fr","live.be","live.ca","live.co.uk","live.com","live.com.pt","live.fr","live.ie","live.it","live.ru","loca-rhone.com","lorraine.eu","lyonnaise-des-eaux.fr","mairie-villeparisis.fr","majencia.com","marie-laure-plv.fr","marquant.fr","marseille.archi.fr","materne.fr","mdlz.com","medecinsdumonde.net","meylan.fr","micheldechabannes.fr","milan.fr","mondadori.fr","monitorgd.com","monitoroffice365.com","monoprix.fr","montpellier-bs.com","msn.com","opcapl.com","oppbtp.fr","outlook.com","outlook.fr","pagesjaunes.fr","pathe.com","planet-fitness.fr","pommier.fr","ponticelli.com","prepacom.net","previfrance.fr","purina.nestle.com","quimper.cci.fr","redoute.fr","rexel.fr","rumeurpublique.fr","sadoul.biz","scam.fr","scgpm.fr","shqiptar.eu","sita.fr","skema.edu","socotec.com","sofinther.fr","spiebatignolles.fr","staci.com","starlight.fr","steria.com","supdepub.com","supinfo.com","suzuki.fr","synergie.fr","synhorcat.com","taz-media.com","tessi.fr","tf1.fr","tgh82.org","tnb.com","toutelectric.fr","tso-catenaires.fr","ucem.fr","umusic.com","unifrance.org","vertbaudet.com","vet-alfort.fr","viacesi.fr","ville-bastia.fr","ville-lebourget.fr","vinci-energies.com","wartsila.com","windowslive.com","ynov.com"],
    "skynet"   : ["skynet.be"],
    "telecom italia" : ["alice.it","aliceposta.it","tim.it","tin.it"],
    "telstra" : ["bigpond.com","bigpond.com.au","telstra.com"],
    "tiscali" : ["tiscali.it","inwind.it"],
    "web.de" : ["cityweb.de","cool.ms","email.de","kuss.ms","taetgren.de","web.de"],
    "yahoo" : ["aol.fr","aol.com","yahoo.fr","yahoo.com","ymail.com","yahoo.co.in","rocketmail.com","aim.com","yahoo.co.uk","yahoo.it","yahoo.com.ar","yahoo.ca","yahoo.com.br","yahoo.es","yahoo.com.sg","yahoo.de","netscape.net","yahoo.com.mx","aol.co.uk","yahoo.ro","yahoo.be","yahoo.co.id","yahoo.cl","yahoo.com.ph","yahoo.co.nz","yahoo.com.tw","yahoo.com.vn","yahoo.in","yahoo.pl","rogers.com","aol.de","verizon.net"],
    "telefonica": [""]
}
    

def generate_champ_id(row):
    md5 = hashlib.md5((row['email']).encode('utf-8')).hexdigest()
    s = f"{row['database_id']}_{md5}"
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def verif_space(data):
    if re.search(' +', data):
        data = data.replace(' ', '')
    return data

def makeMobile(df, name, prefix, name_id='id'):
    df[name] = df[name].astype(str).str.replace(r'\D', '', regex=True)
    lengths = df[name].str.len()
    phone_series = df[name].copy()
    mask_9  = (lengths == 9)
    mask_10 = (lengths == 10)
    mask_11 = (lengths == 11)
    mask_12 = (lengths == 12)
    mask_13 = (lengths == 13)
    phone_series.loc[mask_9]  = prefix + phone_series.loc[mask_9]
    phone_series.loc[mask_10] = prefix + phone_series.loc[mask_10].str[1:]
    phone_series.loc[mask_11] = prefix + phone_series.loc[mask_11].str[2:]
    phone_series.loc[mask_12] = prefix + phone_series.loc[mask_12].str[3:]
    phone_series.loc[mask_13] = prefix + phone_series.loc[mask_13].str[4:]
    valid_mask = mask_9 | mask_10 | mask_11 | mask_12 | mask_13
    df.loc[valid_mask, name] = phone_series.loc[valid_mask]
    # df = df[valid_mask].copy()
    df.loc[~valid_mask, name] = ''
    if 'firstname' in df.columns:
        df['firstname'] = df['firstname'].str.encode('ascii', 'ignore').str.decode("utf-8")
    if 'lastname' in df.columns:
        df['lastname'] = df['lastname'].str.encode('ascii', 'ignore').str.decode("utf-8")
    return df

def strip_email_column(df, column_name='email'):
    df = df.copy()
    df[column_name] = df[column_name].astype(str).str.strip()
    return df

def cleanZipcode(zipcode):
    if pd.isna(zipcode) or zipcode == '':
        return ''
    try:
        zipcode = str(zipcode).strip().replace('.0', '')
        if len(zipcode) == 4:
            return '0' + zipcode
        elif len(zipcode) >= 6:
            return ''
        return zipcode
    except Exception:
        return ''
    
def cleanText(text):
    if not isinstance(text, str):
        if pd.isna(text):
            return ''
        try:
            text = str(text)
        except Exception:
            return ''
    return re.sub(r'[^a-zA-Z\s]', '', text)

def clean_column(df, column_name):
    df[column_name] = df[column_name].fillna("").str.strip()
    return df

def categorize_users(df):
    # Convertir les colonnes en datetime une seule fois
    df['date_last_click'] = pd.to_datetime(df['date_last_click'], format='%m/%d/%Y %H:%M:%S %p', errors='coerce')
    df['date_last_open'] = pd.to_datetime(df['date_last_open'], format='%m/%d/%Y %H:%M:%S %p', errors='coerce')
    df['date_last_sent'] = pd.to_datetime(df['date_last_sent'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
    df['subscription_date'] = pd.to_datetime(df['subscription_date'], format='%m/%d/%Y %H:%M:%S %p', errors='coerce')
    # Calculer les valeurs une seule fois pour éviter de recalculer dans apply()
    now = pd.Timestamp.now()
    df['last_activity'] = df[['date_last_click', 'date_last_open', 'date_last_sent']].max(axis=1)
    df['inactivity_duration'] = (now - df['last_activity']).dt.days
    df['time_since_signup'] = (now - df['subscription_date']).dt.days
    # Définir les catégories en utilisant numpy pour une exécution plus rapide
    conditions = [
        df['date_last_sent'].isna(),  # F1 : Jamais reçu d'email
        df['subscription_date'].astype(str) == "bot",  # F2 : Hyper User (bots)
        df['date_last_click'].notna() & (df['inactivity_duration'] < 90),  # F3 : Clicker < 3 mois
        df['inactivity_duration'] < 30,  # F4 : Activité < 1 mois
        (df['inactivity_duration'] >= 30) & (df['inactivity_duration'] < 90),  # F5 : Activité entre 1 et 3 mois
        df['inactivity_duration'].isna() & (df['time_since_signup'] < 90),  # F6 : Inactifs et timestamp < 3 mois
        df['inactivity_duration'].isna() & (df['time_since_signup'].between(90, 180)),  # F7 : Inactifs entre 3 et 6 mois
        df['inactivity_duration'].between(90, 180),  # F8 : Activité entre 3 et 6 mois
        df['inactivity_duration'].between(180, 365),  # F9 : Activité entre 6 et 12 mois
        df['inactivity_duration'].between(365, 1095),  # F10 : Activité entre 12 et 36 mois
        df['inactivity_duration'].isna() & df['time_since_signup'].between(180, 1095),  # F11 : Inactifs entre 6 et 36 mois
        df['inactivity_duration'] > 1095,  # F12 : Activité > 36 mois
        df['inactivity_duration'].isna() & (df['time_since_signup'] > 1095),  # F13 : Inactifs et timestamp > 36 mois
        df['time_since_signup'].isna()  # F14 : Timestamp vide
    ]
    categories = ["F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10", "F11", "F12", "F13", "F14"]
    df['categorie'] = np.select(conditions, categories, default=None)
    df['recency'] = df[['date_last_click', 'date_last_open', 'date_last_sent','subscription_date']].max(axis=1)
    # df = df[df['recency'] >= (now - pd.DateOffset(months=36))]
    return df

def generate_id(prt_database: str, email: str, key_salt: str) -> str:
    hash_md5 = md5(f"{email}{key_salt}".encode()).hexdigest()
    id_value = f"{prt_database}_{hash_md5.upper()}"
    return id_value

def add_age_column(df, birthdate_col='birthdate'):
    try:
        df[birthdate_col] = pd.to_datetime(df[birthdate_col], errors='coerce', format='%m/%d/%Y')
        today = datetime.today().date()
        df['age'] = today.year - df[birthdate_col].dt.year
        df['age'] -= ((df[birthdate_col].dt.month > today.month) | 
                      ((df[birthdate_col].dt.month == today.month) & (df[birthdate_col].dt.day > today.day))).astype(int)
        df['age'] = df['age'].fillna(0).astype(int)
        return df
    except Exception as e:
        print('Error adding age column:', e)
        return df
    
def add_main_isp_column(df, email_col='email'):
    try:
        if email_col not in df.columns:
            raise ValueError(f"Column '{email_col}' not found in DataFrame")
        df['isp'] = df[email_col].astype(str).str.extract(r'@([\w.-]+)').fillna('Unknown')
        isp_dict = {domain: isp for isp, domains in ISP_FILTER.items() for domain in domains}
        df['main_isp'] = df['isp'].map(isp_dict).fillna('Other')
        return df
    except Exception as e:
        print('Error adding main ISP column:', e)
        return df
    
def assign_scores(df):
    try:
        score_columns = ['density_per_km2', 'score_of_landlords', 'score_of_individual_houses', 
                         'score_median_income', 'score_of_tax_house_holds', 'score_poverty']
        dtypes = {col: int for col in score_columns}
        dtypes['CP'] = str
        df_scores = pd.read_excel("utils/doc/open_insee.xlsx", dtype=dtypes)
        df_scores['CP'] = df_scores['CP'].apply(cleanZipcode)
        df = pd.merge(df, df_scores[['CP'] + score_columns], left_on='zipcode', right_on='CP', how='left')
        for col in score_columns:
            df[col] = df[col].fillna("0").astype(int)
        df['csp_score'] = 0
        return df
    except Exception as e:
        print(f"error assign score ", e)