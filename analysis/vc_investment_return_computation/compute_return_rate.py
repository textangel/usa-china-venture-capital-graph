# from main import Mongo
import pickle
from datetime import datetime, timedelta
import re
from collections import OrderedDict
from functools import cmp_to_key
import pandas as pd

with open("../files/all_companies.bin", "rb+") as f:
    companies = pickle.load(f, encoding="gb2312")
with open("../files/syndicated_companies.bin", "rb+") as f:
    syndicated_companies = pickle.load(f, encoding="gb2312")
with open("../files/all_investments.bin", "rb+") as f:
    all_investments = pickle.load(f, encoding="gb2312")

def convert_money(money_string):
    currency = None
    shi = "十" in money_string
    bai = "百" in money_string
    qian = "千" in money_string
    wan = "万" in money_string
    yi = "亿" in money_string

    shu = "数" in money_string

    num_part = re.findall(r"[-+]?\d*\.\d+|\d+", money_string)
    if len(num_part) == 0:
        num_part = 1
    else:
        num_part = float(num_part[0])

    amt = num_part
    if shu: amt *= 3
    if shi: amt *= 10
    if bai: amt *= 100
    if qian: amt *= 1000
    if wan: amt *= 10000
    if yi: amt *= 100000000

    if money_string[-1] == "币":
        currency = "rmb"
    elif money_string[-1] == "元":
        currency = "usd"
    else:
        currency = "other"

    if currency == "usd":
        amt *= 7.0

    if amt < 20:
        amt = -1

    return int(amt)

def sort_date(d1:dict, d2:dict):
    if "date" not in d1.keys() and "date" not in d2.keys():
        return 0
    if "date" not in d1.keys():
        return -1000000
    if "date" not in d2.keys():
        return 1000000
    return (datetime.strptime(d1["date"],'%Y-%m-%d') - datetime.strptime(d2["date"],'%Y-%m-%d')).days


# rounds = ['种子轮', 'E轮', 'D轮', 'C轮', 'B+轮', 'B轮', '新三板', 'D+轮', '天使轮', 'Pre-A轮', 'Pre-B轮', '新三板定增', 'C+轮', '不明确', 'IPO上市后', 'A+轮', '战略投资', 'A轮', 'F轮-上市前', 'IPO上市']

def compute_RR(date1, date2, val1, val2):
    if val1 == 0: return 0
    year1 = datetime.strptime(date1, '%Y-%m-%d')
    year2 = datetime.strptime(date2, '%Y-%m-%d')
    num_years = (year2 - year1).days/365.25
    if num_years == 0: return 0
    return (val2-val1) / val1 / num_years

syndicated_companies_list = syndicated_companies.keys()

companies = {k: [v] for k,v in companies.items()}
for invest in all_investments:
    if invest["name"] in companies.keys():
        companies[invest["name"]] += [invest]

companies = {k: sorted(v,key=cmp_to_key(sort_date)) for k,v in companies.items() if len(v) > 1}

list_comp = {}
for name,c in companies.items():
    data = []
    for invest_event in c:
        e = invest_event
        round = e["round"]
        if "money" in e.keys():
            money_amt = convert_money(e["money"])
        else:
            money_amt = -1
        if e["type"] == 'final':
            round = 'final'
            val2 = e["valuation"]
            date2 = e["date"]
        if "investors" in e.keys():
            data += [[e["date"], round, e["valuation"], money_amt, e["investors"]]]
        else:
            data += [[e["date"], round, e["valuation"], money_amt, None]]
    # Limiting to companies that have logged >two rounds of investment
    if (len(data) <= 2): continue
    df = pd.DataFrame(data, columns=["date", "round", "valuation", "money", "investor"])

    val2 = df[df["round"] == 'final']["valuation"].values[0]
    date2 = df[df["round"] == 'final']["date"].values[0]
    df['irr'] = df.apply(lambda row: compute_RR(row['date'], date2, row["valuation"], val2), axis=1)


    list_comp[name] = {
        "scope": c[0]["com_scope"],
        "sub_scope": c[0]["com_sub_scope"],
        "df": df
    }

with open("../files/valuations_dfs.bin", "wb+") as f:
    pickle.dump(list_comp, f)

cnt = 0
for k in list_comp.values():
    print(k["df"])
    cnt += 1
    if cnt > 5:
        break
print(companies)
