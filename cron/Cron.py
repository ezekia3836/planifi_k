from cron.p_contact import p_contact
from cron.p_activity import p_activity
from cron.p_tags import p_tags
from cron.p_advertiser import p_advertiser
from reporting.report import reporting

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

    def start_act(self):
        try:
            cron = p_activity()
            cron.start_activities()
        except Exception as e:
            print('error at cron activities', e)
            pass

    def start_tags(self):
        try:
            cron= p_tags()
            cron.startGetTags()
        except Exception as e:
            print("erreur Tags",e)
            pass

    def start_advertiser(self):
        try:
            cron = p_advertiser()
            cron.start_advertiser()
        except Exception as e:
            print("erreur",e)
    def start_reporting(self):
        try:
            cron=reporting()
            cron.report()
        except Exception as e:
            pass