import collections, powerlaw
from database.data_format import DataFormat
from china_data.network_analysis import *
from analysis.vc_investment_return_computation.all_categories_subcategories import categories_subcategories

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


def print_excel_report(mongo:DataFormat):
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

def compute_syndication_percent(mongo):
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

def interactive(mongo:DataFormat):
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


def main2():
    with open("files/mongo.bin", "rb+") as f:
        mongo:DataFormat = pickle.load(f)

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

def main_fully_connected():
    with open("files/mongo.bin", "rb+") as f:
        mongo: DataFormat = pickle.load(f)

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
        names = {0: "Seed and Angel Round",
                 1: "A Round",
                 2: "B Round"}
        print_degree_dist(g, names[i])
        i+=1


if __name__ == '__main__':
    main_fully_connected()