from pymongo import MongoClient
from database.venture_fund import VentureFund

class DataFormat:
    """ Class for mongo database.
    :param master_dict dictionary of form (leadinvestor, follow, datestring): investment_info

    """

    client = MongoClient('localhost', 27017)
    db = client.ITjuzi
    invest_event = db.itjuzi_investevent
    company_specs = db.company_specs

    def __init__(self, master_dict=None, cutoff = 10):
        if master_dict is None:
            master_dict = {}
        self.master_dict = master_dict
        # self.ipo_venture_funds: dict[str, VentureFund] = {}
        self.all_venture_funds: dict[str, VentureFund] = {}
        self.syndicated_companies = {}
        self.companies = {}
        self.all_investments = []

        self.lead_count = {}
        self.follow_count = {}
        self.overall_count = {}
        self.year_count = {}
        self.networked_investors = set([])
        self.total_investors = set([])
        self.total_startups = set([])
        self.networked_startups = set([])
        self.networked_investment_events = set([])
        self.total_investment_events = set([])

        #Fill self.all_venture_funds
        docs = list(DataFormat.db.investfirm_detail.find())
        for doc in docs:
            try:
                vc: VentureFund = VentureFund(doc)
                if len(list(doc["all_investments"])) >= cutoff:
                    self.all_venture_funds[vc.name] = vc
                # if len(vc.ipos) > 0 and len(list(doc["all_investments"])) >= cutoff:
                #     mongo.ipo_venture_funds[vc.name] = vc
            except KeyError as e:
                print(e.args)