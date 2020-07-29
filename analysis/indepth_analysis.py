#encoding=utf8
import pandas as pd
import ast, numpy as np
from collections import Counter
dir = "indepth_analysis/strong_ties_ipo/"


def contains_top_80(path, country="china"):
    if country == "china":
        top80=["红杉资本中国","IDG资本","创新工场","真格基金","KPCB凯鹏华盈中国","北极光创投","经纬中国","蓝驰创投","君联资本","启明创投","金沙江创投","晨兴资本","DCM中国","今日资本","宽带资本CBC","PreAngel","高通Qualcomm Ventures","九合创投","GGV纪源资本","清科创投","险峰长青","光速中国","挚信资本","深创投","鼎晖投资","Ventech China银泰资本","戈壁创投","成为资本","祥峰投资","策源创投","腾讯","德同资本","阿里巴巴","松禾资本","顺为资本","华创资本","赛富基金","达晨创投","SIG海纳亚洲","信中利资本","平安创新投资基金","天图投资","云锋基金","东方富海","联想之星","软银中国","DST","华平投资","Temasek淡马锡","英诺天使基金","复星锐正资本","景林投资","明势资本","中信产业基金","京东","清流资本","高榕资本","钟鼎创投","高瓴资本","新天域资本","梅花创投","普华资本","华人文化产业投资基金","H Capital","和玉资本","线性资本","源码资本","云启资本","蓝湖资本","洪泰基金","元璟资本","光信资本","微光创投","愉悦资本","峰瑞资本","华兴新经济基金","赫斯特Hearst Ventures","元生资本","昆仲资本","火山石资本"]
        top40 =["红杉资本中国","IDG资本","创新工场","真格基金","KPCB凯鹏华盈中国","北极光创投","经纬中国","蓝驰创投","君联资本","启明创投","金沙江创投","晨兴资本","DCM中国","今日资本","宽带资本CBC","PreAngel","高通Qualcomm Ventures","九合创投","GGV纪源资本","清科创投","险峰长青","光速中国","挚信资本","深创投","鼎晖投资","Ventech China银泰资本","戈壁创投","成为资本","祥峰投资","策源创投","腾讯","德同资本","阿里巴巴","松禾资本","顺为资本","华创资本","赛富基金","达晨创投","SIG海纳亚洲","信中利资本",]
    elif country == "america":
        top80=["New Enterprise Associates","SV Angel","Kleiner Perkins Caufield & Byers","First Round","Intel Capital","Sequoia Capital","Accel","500 Startups","Google Ventures","Andreessen Horowitz","Greylock Partners","Draper Fisher Jurvetson (DFJ)","Lerer Hippeau Ventures","Bessemer Venture Partners","General Catalyst Partners","Venrock","RRE Ventures","Redpoint Ventures","Menlo Ventures","Y Combinator","Khosla Ventures","Benchmark","Lightspeed Venture Partners","Felicis Ventures","Polaris Partners","Canaan Partners","InterWest Partners","Battery Ventures","Atlas Venture","Great Oaks Venture Capital","Norwest Venture Partners - NVP","U.S. Venture Partners (USVP)","Founder Collective","CRV","CrunchFund","BoxGroup","True Ventures","Foundation Capital","Crosslink Capital","Highland Capital Partners","DAG Ventures","Mayfield Fund","Matrix Partners","Slow Ventures","SoftTech VC","Shasta Ventures","Data Collective","Morgenthaler Ventures","Trinity Ventures","FLOODGATE","Goldman Sachs","North Bridge Venture Partners & Growth Equity","Spark Capital","Sigma Partners","Ignition Partners","DCM","QueensBridge Venture Partners","Icon Ventures","Sutter Hill Ventures","IVP","SoftBank Capital","FundersClub","AME Cloud Ventures","Qualcomm Ventures","Comcast Ventures","August Capital","Union Square Ventures","Silicon Valley Bank","Sherpa Capital","Baseline Ventures","Correlation Ventures","Betaworks","GGV Capital","Thrive Capital","Tenaya Capital","Lowercase Capital","Social Capital","Harrison Metal"]
        top40=["New Enterprise Associates","SV Angel","Kleiner Perkins Caufield & Byers","First Round","Intel Capital","Sequoia Capital","Accel","500 Startups","Google Ventures","Andreessen Horowitz","Greylock Partners","Draper Fisher Jurvetson (DFJ)","Lerer Hippeau Ventures","Bessemer Venture Partners","General Catalyst Partners","Venrock","RRE Ventures","Redpoint Ventures","Menlo Ventures","Y Combinator","Khosla Ventures","Benchmark","Lightspeed Venture Partners","Felicis Ventures","Polaris Partners","Canaan Partners","InterWest Partners","Battery Ventures","Atlas Venture","Great Oaks Venture Capital","Norwest Venture Partners - NVP","U.S. Venture Partners (USVP)","Founder Collective","CRV","CrunchFund","BoxGroup","True Ventures","Foundation Capital","Crosslink Capital"]
    sheets = pd.read_excel(path, sheetname = "agg", header=0, encoding="utf8")
    for a in ["Source", "Source_Deg", "Weight","s_num_acquired","s_num_invest","s_num_ipo"]:
        sheets[a] = sheets[a].apply(ast.literal_eval)
    sheets["ContainsTOP80"] = sheets["Source"].map(lambda x: any(e in top80 for e in x))
    sheets["AllTOP80"] = sheets["Source"].map(lambda x: all(e in top80 for e in x))
    sheets["ContainsTOP40"] = sheets["Source"].map(lambda x: any(e in top40 for e in x))
    sheets["AllTOP40"] = sheets["Source"].map(lambda x: all(e in top40 for e in x))
    sheets.to_excel(dir+"strong_ties_ipo_{}_top4080.xlsx".format(country))


##Americatemp is the edge file in america's graph
def america_temp_edit(path):
    america = pd.read_csv(path, header = 0)
    america = america.groupby([["Source"],["Target"]])

def get_num_invest(path, country = "china"):
    print(path)
    sheets = pd.read_excel(path, sheetname= ["nodes","edges"], header =0)
    sheet1, sheet2 = sheets["edges"], sheets["nodes"]
    names = sheet2["Id"]
    amounts = {}
    amounts["num_invest"], amounts["num_acquired"], amounts["num_ipo"] = {}, {}, {}
    for name in names:
        n = name
        s = sheet1[sheet1["Source"] == n]
        if country == "china":
            all_df = [s,s[s["Final_Outcome"]=="已被收购"],s[s["Final_Outcome"]=="新三板"],s[s["Final_Outcome"]=="已上市"]]
            res = tuple([len(set(list(a["Name_Date"]))) for a in all_df])
            amounts["num_invest"][n], amounts["num_acquired"][n], amounts["num_thirdboard"][n], amounts["num_ipo"][n] = res
        elif country == "america":
            all_df = [s, s[s["Final_Outcome"] == "acquired"], s[s["Final_Outcome"] == "ipo"]]
            res = tuple([len(set(list(a["Name_Date"]))) for a in all_df])
            amounts["num_invest"][n], amounts["num_acquired"][n], amounts["num_ipo"][n] = res
        else:
            print("Error: Country!")

    df = pd.DataFrame(amounts)
    df.to_excel(dir + "strong_ties_ipo_{}_count.xlsx".format(country))



def list_unique(x):
    return str(list(set(list(x))))

def aggregate(path, country = "china"):
    aggregation_china = {
        "Source": lambda x: str(list(set(list(x)))),
        "Weight": {
            "Weight": lambda x: str(list(set(list(x)))),
        "max_Weight": lambda x: np.max(list(set(list(x))))
        },
        "Source_Deg": {
            "Source_Deg": lambda x: str(list(set(list(x)))),
            "max_Source_Deg": lambda x: max(list(set(list(x))))
        },
        # "Source_IRR": lambda x: str(list(set(list(x)))),
        "s_num_acquired": {
            "s_num_acquired": lambda x: str(list(set(list(x)))),
            "max_num_acquired": lambda x: max(list(set(list(x))))
        },
        "s_num_invest": {
            "s_num_invest":lambda x: str(list(set(list(x)))),
            "max_num_invest": lambda x: max(list(set(list(x))))
        },
        "s_num_ipo":{
            "s_num_ipo":lambda x: str(list(set(list(x)))),
            "max_num_ipo": lambda x: max(list(set(list(x))))
        },
        "s_num_thirdboard":lambda x: str(list(set(list(x)))),
        "Name":"max",
        "Year":"max",
        "Date": "max",
        "IRR": "max",
        "Round": "max",
        "Val": "max",
        "Money": "max",
        "Cur": "max",
        "Scope": "max",
        "Sub_Scope": "max",
        "Time_To_B": "max",
        "Final_Val": "max",
        "Final_Outcome": "max"
    }
    aggregation_america = {
        "Source": lambda x: str(list(set(list(x)))),
        "Weight": {
            "Weight": lambda x: str(list(set(list(x)))),
            "max_Weight": lambda x: np.max(list(set(list(x))))
        },
        "Source_Deg": {
            "Source_Deg": lambda x: str(list(set(list(x)))),
            "max_Source_Deg": lambda x: max(list(set(list(x))))
        },
        "s_num_acquired": {
            "s_num_acquired": lambda x: str(list(set(list(x)))),
            "max_num_acquired": lambda x: max(list(set(list(x))))
        },
        "s_num_invest": {
            "s_num_invest": lambda x: str(list(set(list(x)))),
            "max_num_invest": lambda x: max(list(set(list(x))))
        },
        "s_num_ipo": {
            "s_num_ipo": lambda x: str(list(set(list(x)))),
            "max_num_ipo": lambda x: max(list(set(list(x))))
        },
        "Name": "max",
        "Year": "max",
        "Date": "max",
        "Round": "max",
        "Money": "max",
        "Categories": "max",
        "Final_Outcome": "max"
    }
    sheets = pd.read_excel(path, ["edges"], header =0)
    sheet1 = sheets["edges"]
    if country == "china":
        sheet1 = sheet1.groupby("Name_Date").agg(aggregation_china)
    elif country == "america":
        sheet1 = sheet1.groupby("Name_Date").agg(aggregation_america)

    sheet1.to_excel(dir + "strong_ties_ipo_{}_agg.xlsx".format(country), sheet_name="agg")


def top2(list):
    if list is None:
        return None
    return tuple(sorted(list, reverse=True)[0:2])

def getrow(x,div1, div2):
    if x<  div1: return 0
    elif x <div2: return 1
    else: return 2

def convert_counted(s, div1, div2):
    if s is None or len(s) < 2: return None
    x,y = s[0],s[1]
    return getrow(x,div1, div2), getrow(y, div1, div2)

def sum_counted(list):
    cnt = np.zeros((3, 3))
    for a in list:
        if a is None: continue
        cnt[a[0],a[1]] += 1
    return cnt

def get_count_matrix(results, r, k,df, div1, div2):
    m = sum_counted([convert_counted(s, div1, div2) for s in df["s_num_invest"].apply(top2)])
    n = m.copy()
    sum = np.sum(m)
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            n[i][j] = n[i][j] / sum
    print(str(r) + " Round | " + k, m)
    print(str(r) + " Round | " + k + " | Ratio", n)


def syndicate_len(results, r, k,df, country):
    syndicate_len = dict(Counter(list(df["Source"].apply(len))))
    results[str(r) + " Round | " + k] = syndicate_len
    pd.DataFrame(results).transpose().to_excel(dir+"{}_count_results.xlsx".format(country))

def strong_ties_analyze(agg_path, country="china"):
    sheets = pd.read_excel(agg_path, header=0)
    for a in ["Source", "Source_Deg", "Weight","s_num_acquired","s_num_invest","s_num_ipo"]:
        sheets[a] = sheets[a].apply(ast.literal_eval)
    # print(sheets[0])
    if country == "china":
        rounds = {"All":"All", "A":["A轮","A+轮","Pre-A轮"],"B":["B轮","B+轮","Pre-B轮"]}
        acq_tag, ipo_tag = "已被收购","已上市"
        div1, div2 = 21,90
    elif country == "america":
        rounds = {"All": "All", "A": ["A"], "B": ["B"]}
        acq_tag, ipo_tag = "acquired", "ipo"
        div1, div2 = 30,191
    else: return
    results = {}
    for r,v in rounds.items():
        if r != "All": s = sheets[sheets["Round"].isin(v)]
        else: s = sheets
        dfs = {
            "all":s,
            "acquired": s[s["Final_Outcome"]==acq_tag],
            "ipo": s[s["Final_Outcome"]==ipo_tag]
            }
        for k,df in dfs.items():
            get_count_matrix(results, r, k, df, div1, div2)
            syndicate_len(results,r,k,df, country)

def strong_weak_tie_anaysis(agg_path, country = "china"):
    import matplotlib.pyplot as plt
    s = pd.read_excel(agg_path, header=0)
    for a in ["Source", "Source_Deg", "Weight","s_num_acquired","s_num_invest","s_num_ipo"]:
        s[a] = s[a].apply(ast.literal_eval)
    ss = {
        "Neighborhood of Top 80" : s[s["ContainsTOP80"] == True],
        "Neighborhood of Top 40": s[s["ContainsTOP40"] == True],
        "All Investors in Top 80": s[s["AllTOP80"] == True],
        "All Investors in Top 40": s[s["AllTOP40"] == True]
    }
    for title, s_ in ss.items():
        if country == "china":
            acq_tag, ipo_tag, thirdboard_tag = "已被收购", "已上市", "新三板"
        elif country == "america":
            acq_tag, ipo_tag, thirdboard_tag = "acquired", "ipo", "_"
        else: return
        all_list = list(s_["max_Weight"])
        acqu_ipo_list = list(s_[(s_["Final_Outcome"] == acq_tag) |
                           (s_["Final_Outcome"] == ipo_tag) |
                           (s_["Final_Outcome"] == thirdboard_tag)]["max_Weight"])
        ipo_list = list(s_[s_["Final_Outcome"]==ipo_tag]["max_Weight"])
        aqui_list = list(s_[s_["Final_Outcome"]==acq_tag]["max_Weight"])
        graphs = {"Aquisitions and IPOs": acqu_ipo_list,
                  "IPO Exits": ipo_list,
                  "Acquisition Exits": aqui_list}
        if country == "china":
            thirdboard_china = list(s_[s_["Final_Outcome"] == thirdboard_tag]["max_Weight"])
            graphs["Third Board Exits"] = thirdboard_china

        for k, g in graphs.items():
            plt.close()
            plt.hist([g, all_list], density=True, color=["blue", "red"], label=[k, "All"])
            plt.title("{} Percent Histogram of Strongest Tie in Syndicate:\n {} ({})".format(country.capitalize(), k, title))
            plt.xlabel('Tie Strength (Number of Other Syndications)')
            plt.ylabel('Proportion')
            plt.legend()
            plt.savefig("plots_{}/".format(country) + title + "_"+ k + ".png")

def get_second_wt(x):
    if len(x) <= 1:
        return None
    return sorted(x, reverse=True)[1]

def get_sheet_cluster(sheet_cluster, tag, x:list):
    if len(x) == 0:
        return []
    array = []
    for y in x:
        if y in sheet_cluster.index:
            array += [sheet_cluster[tag][y]]
    return array

def polish(agg_path, country="china"):
    sheets = pd.read_excel(agg_path, ["agg", "cluster", "bet"], header=0)
    sheets, sc, sb = sheets["agg"], sheets["cluster"], sheets["bet"]
    for a in ["Source", "Source_Deg", "Weight", "s_num_acquired", "s_num_invest", "s_num_ipo"]:
        sheets[a] = sheets[a].apply(ast.literal_eval)
    sc = sc.set_index("Name")
    sb = sb.set_index("Name")
    sheets["Max_Bet0"] = sheets["Source"].map(lambda x: max(get_sheet_cluster(sb, "Bet", x), default=None))
    sheets["Min_Bet0"] = sheets["Source"].map(lambda x: min(get_sheet_cluster(sb, "Bet", x), default=None))
    sheets["Max_Cluster0"] = sheets["Source"].map(lambda x: max(get_sheet_cluster(sc, "Clust", x), default=None))
    sheets["Min_Cluster0"] = sheets["Source"].map(lambda x: min(get_sheet_cluster(sc, "Clust", x), default=None))

    sheets["Max_Bet"] = sheets["Source"].map(lambda x: max(get_sheet_cluster(sb,"norm_bet", x), default=None))
    sheets["Min_Bet"] = sheets["Source"].map(lambda x: min(get_sheet_cluster(sb,"norm_bet", x), default=None))
    sheets["Max_Cluster"] = sheets["Source"].map(lambda x: max(get_sheet_cluster(sc,"Triangles/Deg", x), default=None))
    sheets["Min_Cluster"] = sheets["Source"].map(lambda x: min(get_sheet_cluster(sc,"Triangles/Deg", x), default=None))
    # sheets["Med_Cluster"] = sheets["Source"].map(lambda x: median(get_sheet_cluster(sc, x)))
    sheets["Syndiate_Len"] = sheets["Source"].map(lambda x:len(x))
    sheets["Second_Wt"] = sheets["Weight"].apply(lambda x: get_second_wt(x))
    sheets["2nd_Deg"] = sheets["Source_Deg"].apply(lambda x: get_second_wt(x))
    sheets["2nd_num_acquired"] = sheets["s_num_acquired"].apply(lambda x: get_second_wt(x))
    sheets["2nd_num_invest"] = sheets["s_num_invest"].apply(lambda x: get_second_wt(x))
    sheets["2nd_num_ipo"] = sheets["s_num_ipo"].apply(lambda x: get_second_wt(x))
    sheets["Wt_diff"] = sheets["max_Weight"].subtract(sheets["Second_Wt"])
    sheets["Deg_diff"] = sheets["max_Source_Deg"].subtract(sheets["2nd_Deg"])
    sheets["num_acquired_diff"] = sheets["max_num_acquired"].subtract(sheets["2nd_num_acquired"])
    sheets["num_invest_diff"] = sheets["max_num_invest"].subtract(sheets["2nd_num_invest"])
    sheets["num_ipo_diff"] = sheets["max_num_ipo"].subtract(sheets["2nd_num_ipo"])

    if country == "china":
        sheets["RoundTag"] = sheets["Round"].map({
            "天使轮" : 0, "种子轮" : 0, "Pre-A轮" : 1, "A轮" : 1, "A+轮" : 1, "Pre-B轮" : 2, "B轮" : 2, "B+轮" : 2, "Pre-C轮" : 3, "C轮" : 3, "C+轮" : 3, "Pre-D轮" : 4, "D轮" : 4, "D+轮" : 4, "Pre-E轮" : 4, "E轮" : 4, "E+轮" : 4
        })
        sheets["2nd_num_thirdboard"] = sheets["s_num_thirdboard"].apply(lambda x: get_second_wt(x))
    elif country == "america":
        sheets["RoundTag"] = sheets["Round"].map({
            "seed": 0, "A": 1, "B":2, "C":3, "D":4, "E":5
        })
    sheets.to_excel(dir + "——polish_{}_agg.xlsx".format(country), sheet_name="agg")

if __name__ == "__main__":
    country="china"
    agg_path = dir+"strong_ties_ipo_{}_agg.xlsx".format(country)
    polish(agg_path, country)
    country="america"
    agg_path = dir+"strong_ties_ipo_{}_agg.xlsx".format(country)
    polish(agg_path, country)
