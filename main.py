from generate_graph import *
from network_analysis import *
from pymongo import MongoClient
import collections, logging, os
from network_analysis import getPartners_data
from utils import *
import pickle
import powerlaw
import numpy as np
from compute_return_rate.all_categories_subcategories import categories_subcategories, translations

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
class Mongo:
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
        docs = list(Mongo.db.investfirm_detail.find())
        for doc in docs:
            try:
                vc: VentureFund = VentureFund(doc)
                if len(list(doc["all_investments"])) >= cutoff:
                    self.all_venture_funds[vc.name] = vc
                # if len(vc.ipos) > 0 and len(list(doc["all_investments"])) >= cutoff:
                #     mongo.ipo_venture_funds[vc.name] = vc
            except KeyError as e:
                print(e.args)

def print_degree_dist(G:nx.Graph, title):
    degree_sequence = sorted([d for n, d in G.degree()], reverse=True)  # degree sequence
    # print "Degree sequence", degree_sequence
    degree_count = collections.Counter(degree_sequence)
    deg, cnt = zip(*degree_count.items())
    x,y = deg, cnt
    data = [[x[i]] * cnt[i] for i in range(len(x))]
    data = [a for b in data for a in b if a <= 50 ]
    power_law = powerlaw.Fit(data)
    print("China "+ title + " Degree Histogram", power_law.power_law.alpha)
    fig, ax = plt.subplots()
    ax.bar(x, y, log=True, ec="k", align="edge")
    ax.set_xscale("log")
    plt.title("China " + title + " Degree Histogram")
    plt.ylabel("Count")
    plt.xlabel("Degree")
    plt.savefig("graphs/all_categories/*plots/" + title)
    plt.show()


def print_excel_report(mongo:Mongo):
    reports = [vc.report() for vc in mongo.all_venture_funds.values()]
    df = pd.DataFrame(reports)
    df.to_excel("vc_firm_reports.xlsx")

    cols = ["A轮","B轮","C轮及以后","天使轮","战略投资","种子轮"]
    cols = [[a+"_num", a+"_tot", a+"_mean"] for a in cols]
    cols = [d for a in cols for d in a]

    a = [rep["investment_rounds"] for rep in reports]
    df2 = pd.DataFrame(a)
    for col in cols:
        col_zscore = col + '_zscore'
        df2[col_zscore] = (df2[col] - df2[col].mean()) / df2[col].std(ddof=0)
    df2.to_excel("dist_vc_invest.xlsx")

def compute_syndication_percent_mongo(mongo):
    totals = [0, 0, 0]
    totals_syndic = [0, 0, 0]
    money_totals = [0, 0, 0]
    money_totals_syndic = [0, 0, 0]
    m = {
        "seed": 0,
        "A": 1,
        "B": 2
    }
    rounds = {"seed":["天使轮", "种子轮"], "A":["Pre-A轮", "A轮", "A+轮"], "B":["Pre-B轮", "B轮", "B+轮"]}
    all_invest = [a for a in mongo.all_investments if a["currency"] == "rmb"]
    dfss = [[a for a in all_invest if a["round"] in rounds["seed"]],
           [a for a in all_invest if a["round"] in rounds["A"]],
            [a for a in all_invest if a["round"] in rounds["B"]]]
    for i in [0,1,2]:
        dfs = dfss[i]
        for df in dfs:
            totals[i] += 1
            totals_syndic[i] += int(df["syndicated"])
            if not df["money"] > 100:
                continue
            money_totals[i] += int(df["money"])
            money_totals_syndic[i] += int(df["money"]) * int(df["syndicated"])

    money_totals_syndic = [a/7 for a in money_totals_syndic]
    money_totals = [a/7 for a in money_totals]
    percents = [totals_syndic[i] / totals[i] for i in range(3)]
    percents_m = [money_totals_syndic[i] / money_totals[i] for i in range(3)]
    print(totals,
          "\n",
          totals_syndic,
          "\n",
          money_totals,
          "\n",
          money_totals_syndic,
          "\n",
          percents,
          "\n",
          percents_m)

def interactive(mongo:Mongo):
    while True:
        name = input("VC NAME: ")
        if name in mongo.all_venture_funds.keys():
            cur_vc = mongo.all_venture_funds[name]
            print(cur_vc.__str__(), "\n")
            print(cur_vc.coinvest_lead, "\n")
            print(cur_vc.coinvest_follow, "\n")
        elif name == "quit":
            break
        else:
            print("sorry try again.")

#Assumes mongo.masterdict populated in generate_and_save_graph
def get_all_companies(mongo, cutoff=10):
    companies = {}
    docs = list(mongo.db.company_specs.find())
    syndicated_company_list = list(set([a["name"] for a in mongo.master_dict.values()]))
    syndicated_companies = {}
    for doc in docs:
        try:
            basic_info = doc["basic"]["basic"]
            name = basic_info["com_name"]
            company = {
                "name": name,
                "valuation": int(basic_info["total_money"]) * 10000,
                "round": basic_info["com_round_name"],
                "scope": basic_info["com_scope"]["cat_name"],
                "sub_scopes": [a["name"] for a in basic_info["com_sub_scope"]],
                # "tags": [a["name"] for a in basic_info["tag_info"]["normal_tag"]],
                "type": "final",
                "date": "2019-04-01"
            }
            companies[name] = company
            if name in syndicated_company_list:
                syndicated_companies[name] = company
        except Exception as e:
            print(e.args)
    mongo.syndicated_companies = syndicated_companies
    mongo.companies = companies
    with open("files/syndicated_companies.bin", "wb+") as f:
        pickle.dump(syndicated_companies, f)
    with open("files/all_companies.bin", "wb+") as f:
        pickle.dump(companies, f)


def main_old():
    # os.remove("files/debug.log")
    # logging.basicConfig(filename='files/debug.log', level=logging.DEBUG)
    # mongo = Mongo()
    # get_all_companies(mongo)
    # with open("files/all_investments.bin", "wb+") as f:
    #     pickle.dump(mongo.all_investments, f)
    # with open("files/mongo.bin", "wb+") as f:
    #     pickle.dump(mongo, f)
    #
    #
    with open("files/mongo.bin", "rb+") as f:
        mongo:Mongo = pickle.load(f)
    #
    #
    #
    ###
    # G, Gs, Gall = parsed_network_analysis_G_Gs_Gall(mongo)
    ###
    # G_seed = parsed_network_analysis_by_rounds(mongo, ["天使轮", "种子轮"], 0)
    # G_A = parsed_network_analysis_by_rounds(mongo, ["Pre-A轮", "A轮", "A+轮"], 0)
    # G_B = parsed_network_analysis_by_rounds(mongo, ["Pre-B轮", "B轮", "B+轮"], 0)

    categories = categories_subcategories().keys()
    for category in categories:
        G_seed = parsed_network_analysis_firms(mongo, ["天使轮", "种子轮"], 0, sector= category)
        G_A = parsed_network_analysis_firms(mongo, ["Pre-A轮", "A轮", "A+轮"], 0, sector= category)
        G_B = parsed_network_analysis_firms(mongo, ["Pre-B轮", "B轮", "B+轮"], 0, sector= category)
        G_all = parsed_network_analysis_firms(mongo, ["天使轮", "种子轮", "Pre-A轮", "A轮", "A+轮", "Pre-B轮", "B轮", "B+轮"], 0, sector= category)


        nx.write_gexf(G_seed, "graphs/all_categories_firms/"+category+"_G_seed.gexf")
        nx.write_gexf(G_A, "graphs/all_categories_firms/"+category+"_G_A.gexf")
        nx.write_gexf(G_B, "graphs/all_categories_firms/"+category+"_G_B.gexf")
        nx.write_gexf(G_all, "graphs/all_categories_firms/"+category+"_G_all.gexf")


        with open("graphs/all_categories/"+category+"_G_seed_G_A_G_B_G_all.bin", "wb+") as f:
            pickle.dump((G_seed, G_A, G_B, G_all), f)

        # i = 0
        # for g in [G_seed, G_A, G_B, G_all]:
        #     # clustering = nx.algorithms.cluster.clustering(g)
        #     # clustering = sorted(clustering.items(), key=lambda kv: kv[1], reverse=True)
        #     # print("clustering", clustering)
        #     names = {0: " Seed and Angel Round",
        #              1: " A Round",
        #              2: " B Round",
        #              3: " Seed-B Round"}
        #     print_degree_dist(g, translations()[category] + names[i])
        #     i+=1

def main_fully_connected():
    # mongo = Mongo()
    # get_all_companies(mongo)
    # populate_fully_connected_masterdict(mongo, companies=True)
    # df = pd.DataFrame(mongo.all_investments)
    # df.to_excel("final_push/all_investments.xlsx")
    # with open("files/mongo.bin", "wb+") as f:
    #     pickle.dump(mongo, f)
    with open("files/mongo.bin", "rb+") as f:
        mongo: Mongo = pickle.load(f)

    G_seed = parsed_network_analysis(mongo, ["天使轮", "种子轮"], 0)
    G_A = parsed_network_analysis(mongo, ["Pre-A轮", "A轮", "A+轮"], 0)
    G_B = parsed_network_analysis(mongo, ["Pre-B轮", "B轮", "B+轮"], 0)
    G_all = parsed_network_analysis(mongo, ["种子轮","天使轮","Pre-A轮","A轮","A+轮","Pre-B轮","B轮","B+轮","C轮","C+轮","D轮","D+轮","E轮"], 0)

    nx.write_gexf(G_seed, "graphs/fully_connected_G_seed_.gexf")
    nx.write_gexf(G_A, "graphs/fully_connected_G_A.gexf")
    nx.write_gexf(G_B, "graphs/fully_connected_G_B.gexf")
    nx.write_gexf(G_all, "graphs/fully_connected_G_all.gexf")


    with open("files/G_seed_G_A_G_B_fully_connected.bin", "wb+") as f:
        pickle.dump((G_seed, G_A, G_B), f)

    i = 0
    for g in [G_seed, G_A, G_B]:
        # clustering = nx.algorithms.cluster.clustering(g)
        # clustering = sorted(clustering.items(), key=lambda kv: kv[1], reverse=True)
        # print("clustering", clustering)
        names = {0: "Seed and Angel Round",
                 1: "A Round",
                 2: "B Round"}
        print_degree_dist(g, names[i])
        i+=1


if __name__ == '__main__':
    main_fully_connected()