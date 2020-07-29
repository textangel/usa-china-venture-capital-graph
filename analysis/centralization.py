#-*- coding: utf-8 -*-

import pandas as pd
import networkx as nx
countries = "china", "america"


convert = {
"china":{"8":"Round", "4":"Date"},
"america":{"0":"Date", "2": "Round"}
}

round_mapper = {"天使轮" : "seed"
,"种子轮" : "seed"
,"Pre-A轮" :"A"
,"A轮":"A"
,"A+轮" :"A"
,"Pre-B轮":"B"
,"B轮":"B"
,"B+轮" :"B"}

rounds = ["all", "seed","A","B"]
top = [200,80,40,25]

writer = pd.ExcelWriter('files/centralization/degree_cents2.xls', engine='xlsxwriter')
writer2 = pd.ExcelWriter('files/centralization/centrlaizations.xls', engine='xlsxwriter')

def compute_centralization(closeness, ref_closeness):
    base = closeness[0][1]
    values_c = [base-v for k,v in closeness]
    base_r = ref_closeness[0][1]
    values_r = [base_r - v for k, v in ref_closeness]
    print(sum(values_c), sum(values_r), sum(values_c)/sum(values_r))
    return sum(values_c)/sum(values_r)


if __name__ == "__main__":
    a = pd.read_excel("./final_push/all_investments_america.xlsx")

    results = {}
    for c in countries:
        data = pd.read_csv("all_{}_syndications.tsv".format(c), header = 0).rename(convert[c], axis=1)
        data = data[["Source", "Target", "Round", "Date"]]
        if c == "china":
            data["Round"] = data["Round"].map(round_mapper)
        data = data[(data.Round == "seed") | (data.Round == "A") | (data.Round == "B")]
        data = data[data["Date"].str.contains("2015")]

        all_vcs = set(data.Source).union(set(data.Target))

        for round_ in rounds:
            for max_ in top:
                print(c,round_,max_)
                df = data
                if round_ != "all":
                    df = data[data["Round"] == round_]
                G = nx.MultiGraph()
                G.add_nodes_from(all_vcs)
                for ix, row in df.iterrows():
                    G.add_edge(row["Source"], row["Target"])
                G.remove_nodes_from(list(nx.isolates(G)))

                deg_cent = dict(sorted(nx.closeness_centrality(G).items(), key = lambda kv:kv[1], reverse=True)[:max_])
                print(len(deg_cent), deg_cent)
                nodes_to_remove = [a for a in G.nodes if a not in deg_cent.keys()]
                for node in nodes_to_remove:
                    G.remove_node(node)
                closeness  = sorted(nx.closeness_centrality(G).items(), key = lambda kv:kv[1], reverse=True)

                G_ref = nx.MultiGraph()
                G_ref.add_nodes_from(range(max_))
                for i in range(1, max_):
                    # print(i)
                    G_ref.add_edge(0,i)
                ref_closeness = sorted(nx.closeness_centrality(G_ref).items(), key = lambda kv:kv[1], reverse=True)
                print("close: ", closeness)
                print("ref: ", ref_closeness)
                centralization = compute_centralization(closeness, ref_closeness)
                closeness = [("ref",centralization)] + closeness
                sheetname = str(c + "_" + round_ + "_" + str(max_))
                results[sheetname] = centralization
                closenessdf = pd.DataFrame(dict(closeness), index=[0])
                closenessdf.to_excel(writer, sheet_name=sheetname, encoding="utf8")

    final = pd.DataFrame(results, index=[0]).to_excel(writer2, sheet_name="Sheet 1", encoding="utf8")
    writer.save()
