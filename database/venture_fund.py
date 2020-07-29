from network_analysis import *
from utils import *

class VentureFund:
    """ A representation of a venture fund.
    :param investfirm_record the record of the venture firm in Mongo.db.investfirm_detail.
    May be missing for some firms.
    """

    def __init__(self, investfirm_record):
        self.name : str = None
        self.founded_year : str = None
        self.investment_count : str = None
        self.investments_dist : dict = None
        self.total_invested_heuristic = None
        self.investments : list = None
        # self.ipo_valuations = None
        self.ipos = []
        self.coinvest_lead : list = []
        self.coinvest_follow : list = []
        self.invest_rounds: dict = {}
        self.founders_school = None
        self.founders_employer = None
        self.id : str = None
        self.stage : str = None

        for round in investfirm_record["basic_info"]["investment_round"]:
            rd_name = round["name"].split("(")[0]
            self.invest_rounds[rd_name+"_num"] = \
                round["zs"]
            self.invest_rounds[rd_name + "_tot"] = \
                round["money"]
            self.invest_rounds[rd_name + "_mean"] = \
                safe_division(round["money"],round["zs"])

        self.id = investfirm_record["_id"]
        self.name = investfirm_record["basic_info"]["name"]

        try:
            self.founded_year = investfirm_record["basic_info"]["year"]
        except KeyError as e:
            print(e.args)
        try:
            self.investment_count = investfirm_record["basic_info"]["num"]
        except KeyError as e:
            print(e.args)
        try:
            self.investments_dist = dict(investfirm_record["basic_info"]["single_investment_scale"])
        except KeyError as e:
            print(e.args)
        try:
            self.ipos = VentureFund.__getListIPOs(list(investfirm_record["coinvest_data"]["quit_case"]))
        except KeyError as e:
            print(e.args)
        try:
            self.investments = list(investfirm_record["all_investments"])
        except KeyError as e:
            print(e.args)
        try:
            rounds = list(investfirm_record["basic_info"]["investment_round"])
            count = [round_["zs"] for round_ in rounds]
            if count[0] + count[1] + count[2] > sum(count) / 3:
                if count[3] + count[4] + count[5] > sum(count) / 3:
                    self.stage = "mixed"
                else:
                    self.stage = "early"
            else:
                self.stage = "late"
        except KeyError as e:
            print(e.args)
        except IndexError as e:
            print(e.args)

        if self.name in VentureFund.schools_dict.keys():
            self.founders_school = VentureFund.schools_dict[self.name]
        if self.name in VentureFund.employer_dict.keys():
            self.founders_employer = VentureFund.employer_dict[self.name]

        try:
            self.total_invested_heuristic = self.investments_dist['数十万人民币'] * 100 + \
                                            self.investments_dist['数百万人民币'] * 1000 + \
                                            self.investments_dist['数千万人民币'] * 10000 + \
                                            self.investments_dist['亿元以上']    * 100000
        except Exception as e:
            print(e.args)


    schools_dict, employer_dict = getPartners_data()

    def __getListIPOs(list_exits: list) -> list:
        list_ipo = []
        for exit_case in list_exits:
            if exit_case["round_name"] == "IPO上市":
                list_ipo += [(exit_case["name"], exit_case["invst_round_name"])]
        return list_ipo


    def report(self):
        report = {
            "_id": self.id,
            "name": self.name,
            "founded_year": self.founded_year,
            "investment_count": self.investment_count,
            "total_invested_heuristic": self.total_invested_heuristic,
            "investment_rounds": self.invest_rounds,
            "founders_school": self.founders_school,
            "founders_employer": self.founders_employer,
            "ipos": self.ipos
        }
        return report

    def __str__(self):
        return(str(self.report())+"\n"+
               str([k for k in self.investments]) + "\n" +
               str([dict for dict in self.coinvest_lead ]) + "\n" +
               str([dict for dict in self.coinvest_follow ])
        )