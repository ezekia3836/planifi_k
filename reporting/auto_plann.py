from .agents import (
    candidate_agent, data_agent,
    planning_agent, strategy_agent,
    validation_agent, correction_agent,
    payload
)

class AutoPlann:
    def __init__(self):
        self.data = data_agent.dataAgent()
        self.candidat = candidate_agent.candidatAgent()
        self.planning = planning_agent.PlanningAgent()
        self.strategy = strategy_agent.strategyAgent()
        self.validate = validation_agent.agentValidation()
        self.correct = correction_agent.CorrectionAgent()
        self.payload = payload.Payload()

    def run(self):
        adv_ids= self.data.get_advertisers()
        if not adv_ids:
            return []

        report_data = self.data.get_reporting_data(adv_ids)
        if not report_data:
            return []

        candidats = []
        for row in report_data:
            metrics = self.candidat._calcul_metrics(row)
            if not metrics:
                continue
            taux_clickers, taux_openers, taux_unsubs = metrics
            score = self.candidat._calcul_score(taux_clickers, taux_openers, taux_unsubs, row.get("sends",0))
            candidats.append(self.candidat._build_candidat(row, score))

        if not candidats:
            return []
        best_candidats = self.candidat._select_best(candidats)
        planning = self.planning.generate_schedule(best_candidats)
        strategy = self.strategy.optimize(planning)
        validate, error = self.validate.validation(strategy)
        if validate:
            payload=self.payload.build_payload(strategy)
            print("Payload",payload)
            return strategy
        return self.correct.correct(strategy, error)