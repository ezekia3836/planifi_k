from cron.p_contact import p_contact
from cron.p_activity import p_activity
from cron.p_advertiser import p_advertiser
from cron.p_segment import p_segment
from cron.p_agence import p_agence
from cron.p_tags import p_tags
from reporting.reporting_final import reporting as final
from service.cache_batch import CacheBatchService
import requests
import pandas as pd

class Cron():

    def __init__(self):
        pass

    def start_cont(self):
        try:
            cron = p_contact()
            cron.start_contact()
        except Exception as e:
            print('error at cron contacts ', e)
            pass
    def start_tags(self):
        try:
            cron = p_tags()
            cron.startGetTags()
        except Exception as e:
            print('error at cron tags ', e)
            pass
    def start_act(self):
        try:
            cron = p_activity()
            cron.start_activities()
        except Exception as e:
            print('error at cron activities', e)
            pass
    def start_advertiser(self):
        try:
            cron = p_advertiser()
            cron.start_advertiser()
        except Exception as e:
            print("erreur",e)
    def start_segment(self):
        try:
            cron = p_segment()
            cron.run()
        except Exception as e:
            print("[Erreur] cron segment:",e)
    def start_report_final(self):
        try:
            cron=final()
            cron.report()
        except Exception as e:
            print(e)
    def start_agence(self):
        try:
            cron=p_agence()
            cron.run()
        except Exception as e:
            print(f"[Erreur] cron agence: {e}")
    def start_cache_batch(self):
        try:
            cache_batch_service = CacheBatchService()
            cache_batch_service.run_full_batch()
        except Exception as e:
            print(f"[Erreur] cron cache batch: {e}")