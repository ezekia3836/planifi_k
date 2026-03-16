from cron.p_contact import p_contact
from cron.p_activity import p_activity
from cron.p_advertiser import p_advertiser
from reporting import auto_plann
from reporting.reporting_final import reporting as final
from reporting.auto_plann import AutoPlann
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
    def start_auto_plan(self):
        try:
            cron = AutoPlann()
            cron.run()
        except Exception as e:
            print('error at cron auto_plan', e)
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

    def start_report_final(self):
        try:
            cron=final()
            cron.report()
        except Exception as e:
            print(e)