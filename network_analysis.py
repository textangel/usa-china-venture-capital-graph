import generate_graph
import networkx as nx
import seaborn as sns
import matplotlib.pyplot as plt
import math, pickle
import pandas as pd
from datetime import datetime, timedelta
# from compute_return_rate.analyze_return_rate import generate_summary_irr


def network_analysis(mongo):
    filtered = {k: v for (k, v) in mongo.master_dict.items()
            if (v["currency"] == "rmb" and
                (v["round"] in ["种子轮", '天使轮', 'Pre-A轮',
                                "A轮", "A+轮"]) and
                v["valuation"] >= 1000)
            }
    vals = [math.log10(v['valuation']) for (k,v) in filtered.items()]
    sns.distplot(vals)
    plt.show(block=True)
    print(len(mongo.master_dict), len(filtered))

    unique_nodes = list(set([node for k in filtered.keys() for node in [k[0], k[1]]]))
    G = nx.Graph()
    G.add_nodes_from(unique_nodes)
    for k,v in filtered.items():
        v["time"] = k[2]
        G.add_edge(k[0],k[1],data=v)

    return G

def string_tuple_to_list(s):
    try:
        if s is float('nan') or s is None or s in ["NaN","nan"]:
            return []
        if len(s)>2:
            temp = s[1:-1].split(",")
            temp = [w.replace("'","").strip() for w in temp]
            temp = [a for a in temp if a != ""]
            return temp
    except:
        print("error", s)
    return []

def getPartners_data():
    partners_data = pd.read_excel("./selenium_scraper/all_partners_data_3.xlsx", header=0, index_col=0)
    partners_data = partners_data[partners_data.founder_role.str.contains("合伙人")]
    all_firms = set(list(partners_data["name"]))
    schools_dict = {}
    employer_dict = {}
    for firm in all_firms:
        temp = list(partners_data[partners_data.name == firm]["founder_schools"])
        temp = [string_tuple_to_list(s) for s in temp]
        temp = [a for s in temp for a in s]
        schools_dict[firm] = temp

        temp = list(partners_data[partners_data.name == firm]["founder_employers"])
        temp = [string_tuple_to_list(s) for s in temp]
        temp = [a for s in temp for a in s]
        employer_dict[firm] = temp

    return schools_dict, employer_dict

def common(l1:list,l2:list):
    return set(l1).intersection(set(l2))



#The one we use is filtered_all.
#Firms with >= 10 coinvestments
def parsed_network_analysis_G_Gs_Gall(mongo):
    print(mongo.overall_count)
    filtered = {k: v for (k, v) in mongo.master_dict.items()
                if (v["currency"] == "rmb" and
                    # (v["round"] in ["种子轮", '天使轮', 'Pre-A轮',
                    #                 "A轮", "A+轮"]) and
                    # v["valuation"] >= 1000 and
                    k[0] in mongo.overall_count.keys() and
                    k[1] in mongo.overall_count.keys() and
                    mongo.overall_count[k[0]] >= 2 and
                    mongo.overall_count[k[1]] >= 2)
                }

    unique_nodes = list(set([node for k in filtered.keys() for node in [k[0], k[1]]]))
    # G = nx.DiGraph()
    # G.add_nodes_from(unique_nodes)
    # for k, v in filtered.items():
    #     if G.has_edge(k[1], k[0]):
    #         cnt = G.get_edge_data(k[1], k[0])["Weight"]
    #         G[k[1]][k[0]].update({"Weight": cnt + 1})
    #     else:
    #         G.add_edge(k[1], k[0], Weight=1)
    # G.remove_nodes_from(list(nx.isolates(G)))

    # Gs = G.copy()
    # for u,v,count in list(Gs.edges.data('Weight', default=2)):
    #     if count < 2:
    #         Gs.remove_edge(u, v)
    # Gs.remove_nodes_from(list(nx.isolates(Gs)))

    filtered_all = {k: v for (k, v) in mongo.master_dict.items()
                    if (k[0] in mongo.overall_count.keys() and
                        k[1] in mongo.overall_count.keys() and
                        mongo.overall_count[k[0]] >= 10 and
                        mongo.overall_count[k[1]] >= 10
                        # v["currency"] == "rmb"
                        )
                    }

    Gall = nx.DiGraph()
    all_vcs = [vc_name for vc_name in mongo.all_venture_funds.keys()]
    all_ids = {vc.name:vc.id for vc in mongo.all_venture_funds.values()}
    Gall.add_nodes_from(all_vcs)

    for k, v in filtered_all.items():
        if k[0] in all_vcs and k[1] in all_vcs:
            if Gall.has_edge(k[1], k[0]):
                cnt = Gall.get_edge_data(k[1], k[0])["Weight"]
                Gall[k[1]][k[0]].update({"Weight": cnt + 1})
            else:
                Gall.add_edge(k[1], k[0], Weight=1, StartDate=k[2], EndDate="2019-03-21")

    Gall.remove_nodes_from(list(nx.isolates(Gall)))
    used_ids = {all_ids[name]:name for name in list(Gall.nodes)}
    with open("selenium_scraper/used_ids.txt", "w+") as f:
        f.write(str(used_ids))

    to_remove = []
    Gs = Gall.copy()
    for (u, v, c) in Gs.edges.data('Weight', default=1):
        if c < 2:
            to_remove.append((u,v))
    Gs.remove_edges_from(to_remove)
    Gs.remove_nodes_from(list(nx.isolates(Gs)))



    schools_dict, employers_dict = getPartners_data()
    for vc in all_vcs:
        if vc not in schools_dict.keys():
            schools_dict[vc] = []
        if vc not in employers_dict.keys():
            employers_dict[vc] = []

    G = nx.MultiDiGraph()
    all_vcs = [vc_name for vc_name in mongo.all_venture_funds.keys()]
    G.add_nodes_from(all_vcs)
    for k, v in filtered_all.items():
        if k[0] in all_vcs and k[1] in all_vcs:
            edge_no = G.number_of_edges(k[1], k[0])
            G.add_edge(k[1], k[0], StartDate=k[2], EndDate="2019-03-21",
                           StartDateYearAfter='{:%Y-%m-%d}'.format(datetime.strptime(k[2], "%Y-%m-%d") + timedelta(days=365)),
                           round=v["round"], valuation=v["valuation"],
                           money=v["money"], currency=v["currency"],
                           com_scope=v["com_scope"], com_sub_scope=v["com_sub_scope"],
                           edge_no = edge_no + 1, name = v["name"],
                       common_schools= str(common(schools_dict[k[1]], schools_dict[k[0]])),
                       common_employers = str(common(employers_dict[k[1]], employers_dict[k[0]])),
                       final_val=v["final_val"], final_round=v["round"])
    G.remove_nodes_from(list(nx.isolates(G)))
    df = nx.to_pandas_edgelist(G)
    df.to_csv("files/master_graph_edgelist.csv")

    return G, Gs, Gall

def search_for_irr(dict_comp, name, date, round):
    try:
        df = dict_comp[name]["df"]
        a = df[(df["date"] == date) & (df["round"] == round)]["irr"]
        irr = float(a)
        return irr
    except:
        return -1

def search_for_time_btwn_A_B(dict_comp, name):
    try:
        df = dict_comp[name]["df"]
        a = df[(df["round"] == "A轮")]["date"].values[0]
        date_a = datetime.strptime(a, "%Y-%m-%d")
        b = df[(df["round"] == "B轮")]["date"].values[0]
        date_b = datetime.strptime(b, "%Y-%m-%d")
        return (date_b-date_a).days
    except:
        return -1

# search_for_irr(dict_comp, v["name"], k[2],v["round"]),


def parsed_network_analysis(mongo, rounds, min_overall_count = 10, sector = None, sub_sector = None):
    if sector == None and sub_sector == None:
        filepath = "./files/irr/%s" + str(rounds)[1:-1].replace(",' ", "") + ".bin"
    elif sub_sector is not None:
        filepath = "./files/irr/sub_scope/" + sub_sector + "%s" + str(rounds)[1:-1].replace(",' ", "") + ".bin"
    elif sector is not None:
        filepath = "./files/irr/scope/" + sector + "%s" + str(rounds)[1:-1].replace(",' ", "") + ".bin"

    #Call generate_summary_irr if the filepath does not exist.

    with open(filepath % ('overall_investors_irr'), "rb+") as f:
        overall_investors_irr = pickle.load(f)
        overall_investors_irr = {k: float(v) for k,v in overall_investors_irr.items() if v > -100000}
    with open(filepath % ('investors_irr'), "rb+") as f:
        investors_irr = pickle.load(f)
        investors_irr = {k: float(v)  for k, v in investors_irr.items() if v > -100000}
    with open(filepath % ('money_irr'), "rb+") as f:
        money_irr =  pickle.load(f)
        money_irr = {k: float(v)  for k, v in money_irr.items() if v > -100000}



    with open("./files/valuations_dfs.bin", "rb+") as f:
        dict_comp = pickle.load(f, encoding="gb2312")

    print(mongo.overall_count)

    filtered_all = {k: v for (k, v) in mongo.master_dict.items()
                    if (k[0] in mongo.overall_count.keys() and
                        k[1] in mongo.overall_count.keys() and
                        mongo.overall_count[k[0]] >= min_overall_count and
                        mongo.overall_count[k[1]] >= min_overall_count
                        # v["currency"] == "rmb"
                        )
                    }

    schools_dict, employers_dict = getPartners_data()
    all_vcs = [vc_name for vc_name in mongo.all_venture_funds.keys()]
    for vc in all_vcs:
        if vc not in schools_dict.keys():
            schools_dict[vc] = []
        if vc not in employers_dict.keys():
            employers_dict[vc] = []

    G = nx.MultiDiGraph()
    all_vcs = [vc_name for vc_name in mongo.all_venture_funds.keys()]
    G.add_nodes_from(all_vcs)
    nx.set_node_attributes(G, overall_investors_irr, name="overall_investors_irr")
    nx.set_node_attributes(G, investors_irr, name="investors_irr")
    nx.set_node_attributes(G, money_irr, name="money_irr")

    max_edge_weights = {}
    for k, v in filtered_all.items():
        if v["round"] not in rounds: continue
        if sector is not None and v["com_scope"] != sector: continue
        if sub_sector is not None and v["com_sub_scope"] != sub_sector: continue
        if k[0] in all_vcs and k[1] in all_vcs:
            if (k[1], k[0]) not in max_edge_weights.keys(): max_edge_weights[(k[1], k[0])] = 0
            if (k[0], k[1]) not in max_edge_weights.keys(): max_edge_weights[(k[0], k[1])] = 0
            max_edge_weights[(k[1], k[0])] += 1
            max_edge_weights[(k[0], k[1])] += 1

    for k, v in filtered_all.items():
        if v["round"] not in rounds: continue
        time_a_b = -1
        if v["round"] == "A轮":
            time_a_b = search_for_time_btwn_A_B(dict_comp, v["name"])
        if sector is not None and v["com_scope"] != sector: continue
        if sub_sector is not None and v["com_sub_scope"] != sub_sector: continue
        if k[0] in all_vcs and k[1] in all_vcs:
            edge_no = G.number_of_edges(k[1], k[0])
            G.add_edge(k[1], k[0], Lead=k[0] , StartDate=k[2], EndDate="2019-03-21",
                       StartDateYearAfter='{:%Y-%m-%d}'.format(
                           datetime.strptime(k[2], "%Y-%m-%d") + timedelta(days=365)),
                       irr = search_for_irr(dict_comp, v["name"], k[2],v["round"]),
                       round=v["round"], valuation=v["valuation"],
                       money=v["money"], currency=v["currency"],
                       com_scope=v["com_scope"], com_sub_scope=v["com_sub_scope"],
                       max_edge_weights = max_edge_weights[(k[1], k[0])],
                       name = v["name"], time_a_b=time_a_b,
                       final_val=v["final_val"], final_round=v["final_round"])
            G.add_edge(k[0], k[1], Lead=k[0], StartDate=k[2], EndDate="2019-03-21",
                       StartDateYearAfter='{:%Y-%m-%d}'.format(
                           datetime.strptime(k[2], "%Y-%m-%d") + timedelta(days=365)),
                       irr=search_for_irr(dict_comp, v["name"], k[2], v["round"]),
                       round=v["round"], valuation=v["valuation"],
                       money=v["money"], currency=v["currency"],
                       com_scope=v["com_scope"], com_sub_scope=v["com_sub_scope"],
                       max_edge_weights= max_edge_weights[(k[0], k[1])],
                       name = v["name"], time_a_b=time_a_b,
                       final_val=v["final_val"], final_round=v["final_round"])
    G.remove_nodes_from(list(nx.isolates(G)))
    df = nx.to_pandas_edgelist(G)
    # df.to_csv("files/master_graph_edgelist.csv")

    return G



with open("./files/valuations_dfs.bin", "rb+") as f:
    dict_comp = pickle.load(f, encoding="gb2312")

def parsed_network_analysis_firms(mongo, rounds, min_overall_count = 0, sector = None, sub_sector = None):
    if sector == None and sub_sector == None:
        filepath = "./files/irr/%s" + str(rounds)[1:-1].replace(",' ", "") + ".bin"
    elif sub_sector is not None:
        filepath = "./files/irr/sub_scope/" + sub_sector + "%s" + str(rounds)[1:-1].replace(",' ", "") + ".bin"
    elif sector is not None:
        filepath = "./files/irr/scope/" + sector + "%s" + str(rounds)[1:-1].replace(",' ", "") + ".bin"

    #Call generate_summary_irr if the filepath does not exist.

    with open(filepath % ('overall_investors_irr'), "rb+") as f:
        overall_investors_irr = pickle.load(f)
        overall_investors_irr = {k: float(v) for k,v in overall_investors_irr.items() if v > -100000}
    with open(filepath % ('investors_irr'), "rb+") as f:
        investors_irr = pickle.load(f)
        investors_irr = {k: float(v)  for k, v in investors_irr.items() if v > -100000}
    with open(filepath % ('money_irr'), "rb+") as f:
        money_irr =  pickle.load(f)
        money_irr = {k: float(v)  for k, v in money_irr.items() if v > -100000}

    print(mongo.overall_count)

    filtered_all = {k: v for (k, v) in mongo.master_dict.items()
                    if (k[0] in mongo.overall_count.keys() and
                        k[1] in mongo.overall_count.keys() and
                        mongo.overall_count[k[0]] >= min_overall_count and
                        mongo.overall_count[k[1]] >= min_overall_count
                        # v["currency"] == "rmb"
                        )
                    }

    schools_dict, employers_dict = getPartners_data()
    all_vcs = [vc_name for vc_name in mongo.all_venture_funds.keys()]
    for vc in all_vcs:
        if vc not in schools_dict.keys():
            schools_dict[vc] = []
        if vc not in employers_dict.keys():
            employers_dict[vc] = []

    G = nx.MultiDiGraph()
    all_vcs = [vc_name for vc_name in mongo.all_venture_funds.keys()]
    G.add_nodes_from(all_vcs)
    nx.set_node_attributes(G, overall_investors_irr, name="overall_investors_irr")
    nx.set_node_attributes(G, investors_irr, name="investors_irr")
    nx.set_node_attributes(G, money_irr, name="money_irr")

    firm_names = [v["name"] for v in filtered_all.values()]
    firm_irr = {name: v["df"].iloc[0,"irr"] for name, v in dict_comp.items() if v["df"] is not None and len(v["df"]) > 0}
    firm_scopes = {v["name"]:v["com_scope"] for k,v in filtered_all.items()}
    firm_subscopes = {v["name"]:v["com_sub_scope"]    for k, v in filtered_all.items()}
    firm_valuation = {v["name"]: v["valuation"] for k, v in filtered_all.items()}

    G.add_nodes_from(firm_names)
    nx.set_node_attributes(G, firm_scopes, name="scope")
    nx.set_node_attributes(G, firm_subscopes, name="subscope")
    nx.set_node_attributes(G, firm_valuation, name="firm_valuation")


    for k, v in filtered_all.items():
        if v["round"] not in rounds: continue
        if sector is not None and v["com_scope"] != sector: continue
        if sub_sector is not None and v["com_sub_scope"] != sub_sector: continue
        if k[0] in all_vcs and k[1] in all_vcs:

            G.add_edge(k[0], v['name'], StartDate=k[2], EndDate="2019-03-21",
                       StartDateYearAfter='{:%Y-%m-%d}'.format(
                           datetime.strptime(k[2], "%Y-%m-%d") + timedelta(days=365)),
                       irr = search_for_irr(dict_comp, v["name"], k[2],v["round"]),
                       round=v["round"], valuation=v["valuation"],
                       money=v["money"], currency=v["currency"],
                       com_scope=v["com_scope"], com_sub_scope=v["com_sub_scope"], name = v["name"],
                       final_val = v["final_val"], final_round= v["final_round"])

            G.add_edge(k[1], v['name'], StartDate=k[2], EndDate="2019-03-21",
                       StartDateYearAfter='{:%Y-%m-%d}'.format(
                           datetime.strptime(k[2], "%Y-%m-%d") + timedelta(days=365)),
                       irr = search_for_irr(dict_comp, v["name"], k[2],v["round"]),
                       round=v["round"], valuation=v["valuation"],
                       money=v["money"], currency=v["currency"],
                       com_scope=v["com_scope"], com_sub_scope=v["com_sub_scope"], name = v["name"],
                       final_val=v["final_val"], final_round= v["final_round"])

    G.remove_nodes_from(list(nx.isolates(G)))
    df = nx.to_pandas_edgelist(G)
    # df.to_csv("files/master_graph_edgelist.csv")

    return G
