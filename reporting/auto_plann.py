
from .agents import (candidate_agent, data_agent,
                     planning_agent,strategy_agent,
                     validation_agent,correction_agent)
class AutoPlann:
    def __init__(self):
        self.data = data_agent.dataAgent()
        self.candidat = candidate_agent.candidatAgent()
        self.planning = planning_agent.PlanningAgent()
        self.strategy = strategy_agent.strategyAgent()
        self.validate = validation_agent.agentValidation()
        self.correct = correction_agent.CorrectionAgent()
    def run(self):
        adv_ids = self.data.get_advertisers()
        print(f"[AutoPlann] Advertisers actifs : {adv_ids}")
        report_data = self.data.get_reporting_data(adv_ids)
        candidats = []
        for row in report_data:
            if row["sends"] < 50: 
                continue
            metrics = self.candidat._calcul_metrics(row)
            if not metrics:
                continue
            taux_clickers, taux_openers, taux_unsubs = metrics
            score = self.candidat._calcul_score(taux_clickers, taux_openers, taux_unsubs, row["sends"])
            candidat = self.candidat._build_candidat(row, score)
            candidats.append(candidat)
        best_candidats = self.candidat._select_best(candidats)
        planning= self.planning.generate_schedule(best_candidats)
        strategy = self.strategy.optimize(planning)
        validate,error = self.validate.validation(strategy)
        if validate:
            return strategy
        correct = self.correct.correct(strategy,error)
        print("correction")
        return correct
    


        
