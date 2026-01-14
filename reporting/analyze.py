from config.config import Config as config
import unicodedata
class analyse:
     def __init__(self):
          pass
     def safe_unicode(self,s):
        if not isinstance(s, str):
            s = str(s)
        return ''.join(
            c for c in unicodedata.normalize('NFKD', s)
            if not unicodedata.combining(c)
        )
     def analyze_click_rate(self, click_rate):
            if click_rate >= config.REPORTING_CR_EXCELLENT:
                return "🟢 Taux de clic satisfaisant"
            elif config.REPORTING_CR_GOOD[0] <= click_rate < config.REPORTING_CR_GOOD[1]:
                return "🟠 Taux de clic bon"
            elif config.REPORTING_CR_MEDIUM[0] <= click_rate < config.REPORTING_CR_MEDIUM[1]:
                return "🟡 Taux de clic moyen"
            else:
                return "🔴 Taux de clic faible"
    
     def analyze_cto_rate(self, cto_rate, opened):
            if cto_rate >= config.REPORTING_CTO_EXCELLENT:
                return "🔥 Engagement très fort (CTO)"
            elif config.REPORTING_CTO_GOOD[0] <= cto_rate < config.REPORTING_CTO_GOOD[1]:
                return "👍 Engagement correct"
            elif config.REPORTING_CTO_MEDIUM[0] <= cto_rate < config.REPORTING_CTO_MEDIUM[1]:
                if opened > 50:
                    return "😐 Faible engagement malgré les ouvertures"
                else:
                    return "🙂 Engagement moyen"
            else:
                return "⚠️ Faible engagement (CTO)"
     def analyze_unsub_rate(self, unsub_rate):
            if unsub_rate > config.REPORTING_UNSUB_CRITICAL:
                return "🚨 Taux de désabonnement très élevé"
            elif config.REPORTING_UNSUB_HIGH[0] < unsub_rate <= config.REPORTING_UNSUB_HIGH[1]:
                return "⚠️ Taux de désabonnement élevé"
            elif config.REPORTING_UNSUB_MEDIUM[0] < unsub_rate <= config.REPORTING_UNSUB_MEDIUM[1]:
                return "🔶 Taux de désabonnement moyen"
            elif unsub_rate <= config.REPORTING_UNSUB_LOW:
                return "✅ Taux de désabonnement faible"
            else:
                return "ℹ️ Taux de désabonnement normal"
            