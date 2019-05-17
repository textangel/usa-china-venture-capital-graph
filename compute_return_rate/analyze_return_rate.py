# -*- coding: utf-8 -*-
import pickle, operator
import numpy as np
from all_categories_subcategories import categories_subcategories

with open("../files/valuations_dfs.bin", "rb+") as f:
    dict_comp = pickle.load(f, encoding="gb2312")

def filter_money(df2):
    return df2[(df2["money"] > 0)]

def filter_investorrow_not_null(df):
    if df is None: return None
    df = df[(df["investor"].notna())]
    df = df[df["investor"].apply(len) & (df["investor"].apply(lambda x: x[0]).notna())]
    return df

def all_vcs_returns(dict_comp):
    investors_irr = {}
    money_irr = {}
    for comp in dict_comp.values():
        scope = comp["scope"]
        sub_scope = comp["sub_scope"]

        df = comp["df"]
        df = filter_investorrow_not_null(df)
        for index, row in df.iterrows():
            investor = row["investor"][0]
            if investor not in investors_irr.keys():
                investors_irr[investor] = [(row["round"], row["irr"], scope, sub_scope)]
            else:
                investors_irr[investor] += [(row["round"], row["irr"], scope, sub_scope)]
        df = filter_money(df)
        for index, row in df.iterrows():
            investor = row["investor"][0]
            if investor not in money_irr.keys():
                money_irr[investor] = [(row["round"], row["money"] * row["irr"], scope, sub_scope)]
            else:
                money_irr[investor] += [(row["round"], row["money"] * row["irr"], scope, sub_scope)]
    with open("../files/irr/investors_irr.bin", "wb+") as f:
        pickle.dump(investors_irr, f)
    with open("../files/irr/money_irr.bin", "wb+") as f:
        pickle.dump(money_irr, f)
    print(investors_irr,"\n", money_irr)
    return investors_irr, money_irr

def generate_summary_irr(roundss, scope= None, sub_scope = None, min_cutoff = 10):
    has_scope = not scope is None
    has_sub_scope = not sub_scope is None
    if not has_scope:
        scope = [k for k in categories_subcategories().keys()]
    if not has_sub_scope:
        sub_scope = [a for k in categories_subcategories().values() for a in k]
    with open("../files/irr/investors_irr.bin", "rb+") as f:
        investors_irr = pickle.load(f)
    with open("../files/irr/money_irr.bin", "rb+") as f:
        money_irr = pickle.load(f)
    for rounds in roundss:
        investors_irr2 = {k:[a[1] for a in v
                             if (a[0] in rounds and a[2] in scope and a[3] in sub_scope)]
                          for k,v in investors_irr.items()}
        investors_irr2 = {k: np.median(v) for k, v in investors_irr2.items() if len(v) > min_cutoff}

        money_irr2 = {k: [a[1] for a in v
                          if (a[0] in rounds and a[2] in scope and a[3] in sub_scope)]
                      for k, v in money_irr.items()}
        money_irr2 = {k: np.median(v) for k, v in money_irr2.items() if
                      len(v) > min_cutoff}

        sum_investors_irr = sum([v for v in investors_irr2.values() if v > -100000000])
        sum_money_irr = sum([v for v in money_irr2.values() if v > -10000000000])
        overall_investors_irr = {k: v / sum_investors_irr + money_irr2[k] / sum_money_irr for k, v in
                                 investors_irr2.items() if k in money_irr2.keys()}
        overall_investors_irr = {k: v * len(overall_investors_irr) for k, v in overall_investors_irr.items()}


        if not has_scope and not has_sub_scope:
            with open("../files/irr/investors_irr"+str(rounds)[1:-1].replace(",' ","")+".bin", "wb+") as f:
                pickle.dump(investors_irr2, f)
            with open("../files/irr/money_irr" + str(rounds)[1:-1].replace(",' ", "") + ".bin", "wb+") as f:
                pickle.dump(money_irr2, f)
            with open("../files/irr/overall_investors_irr" + str(rounds)[1:-1].replace(",' ", "") + ".bin", "wb+") as f:
                pickle.dump(overall_investors_irr, f)
        elif has_sub_scope:
            with open("../files/irr/sub_scope/"+sub_scope+"investors_irr"+str(rounds)[1:-1].replace(",' ","")+".bin", "wb+") as f:
                pickle.dump(investors_irr2, f)
            with open("../files/irr/sub_scope/"+sub_scope+"money_irr" + str(rounds)[1:-1].replace(",' ", "") + ".bin", "wb+") as f:
                pickle.dump(money_irr2, f)
            with open("../files/irr/sub_scope/"+sub_scope+"overall_investors_irr" + str(rounds)[1:-1].replace(",' ", "") + ".bin", "wb+") as f:
                pickle.dump(overall_investors_irr, f)
        elif has_scope:
            with open("../files/irr/scope/"+scope+"investors_irr" + str(rounds)[1:-1].replace(",' ", "") + ".bin", "wb+") as f:
                pickle.dump(investors_irr2, f)
            with open("../files/irr/scope/"+scope+"money_irr" + str(rounds)[1:-1].replace(",' ", "") + ".bin", "wb+") as f:
                pickle.dump(money_irr2, f)
            with open("../files/irr/scope/"+scope+"overall_investors_irr" + str(rounds)[1:-1].replace(",' ", "") + ".bin", "wb+") as f:
                pickle.dump(overall_investors_irr, f)

        print(investors_irr2)
        print(money_irr2)
        print(overall_investors_irr)

all_vcs_returns(dict_comp)
categories = categories_subcategories().keys()
generate_summary_irr([['种子轮', '天使轮', 'Pre-A轮', 'A轮', 'A+轮', 'Pre-B轮', 'B轮', 'B+轮', 'C轮', 'C+轮', 'D轮', 'D+轮', 'E轮']], min_cutoff=0)
for category in categories:
    generate_summary_irr([['种子轮', '天使轮', 'Pre-A轮', 'A轮', 'A+轮', 'Pre-B轮', 'B轮', 'B+轮', 'C轮', 'C+轮', 'D轮', 'D+轮', 'E轮']], scope=category, min_cutoff=0)
    generate_summary_irr([["天使轮","种子轮"],["Pre-A轮","A轮","A+轮"],["Pre-B轮","B轮","B+轮"]], scope=category, min_cutoff=0)
quit()

def filter_year_round(df, years, rnds, required_irr = -1000, max_irr = 10000):
    return df[(df["date"].str[0:4].isin(years)) &
             (df["round"].isin(rnds)) &
             (df["irr"] >= required_irr) &
              (df["irr"] <= max_irr)]

def bool_condition(df, years, rnds, required_irr = -1000, max_irr = 10000):
    df2 = filter_year_round(df, years, rnds, required_irr, max_irr)
    if len(df2) > 0:
        return 1
    return 0

def filter_list(list_comp, years, rounds, industries = None, sub_scopes = None, required_irr = -1000, max_irr = 10000):
    if industries is not  None:
        list_comp = {k:a for k,a in list_comp.items() if a["scope"] in industries}
    if sub_scopes is not None:
        list_comp = {k:a for k,a in list_comp.items() if a["sub_scope"] in sub_scopes}

    list_comp = {k:a for k,a in list_comp.items() if bool_condition(a["df"],years,rounds, required_irr, max_irr)}
    return list_comp

def vcs_money_returns(dict_comp, years, rounds, industries = None, sub_scopes = None):
    years = [str(a) for a in years]
    vcs = {}
    dict_comp = filter_list(dict_comp, years, rnds, industries, sub_scopes)
    for comp in dict_comp.values():
        df = comp["df"]
        df2 = filter_year_round(df, years, rounds)
        df2 = filter_money(df2)
        df2 = filter_investorrow_not_null(df2)
        for id, row in df2.iterrows():
            if row["investor"][0] not in vcs.keys():
                vcs[row["investor"][0]] = 0
            vcs[row["investor"][0]] += row["money"] * row["irr"]
    vcs = sorted(vcs.items(), key=lambda kv: kv[1], reverse=True)
    return vcs

def vcs_mean_returns(dict_comp, years, rounds, industries = None, sub_scopes = None):
    years = [str(a) for a in years]
    vcs = {}
    dict_comp = filter_list(dict_comp, years, rounds, industries, sub_scopes)
    for comp in dict_comp.values():
        df = comp["df"]
        df2 = filter_year_round(df, years, rounds)
        df2 = filter_money(df2)
        df2 = filter_investorrow_not_null(df2)
        for id, row in df2.iterrows():
            if row["investor"][0] not in vcs.keys():
                vcs[row["investor"][0]] = (0,0)
            v = vcs[row["investor"][0]]
            vcs[row["investor"][0]] = (v[0] + 1, (v[0] * v[1] + row["irr"]) / (v[0] + 1))
    vcs = sorted(vcs.items(), key=lambda kv: kv[1][1], reverse=True)
    vcs = [vc for vc in vcs if vc[1][0] >= 10]
    return vcs

def test_syndication(dict_comp, years, rounds, required_irr = -1000, max_irr = 10000,
                     industries = None, sub_scopes = None):
    years = [str(a) for a in years]
    syndicated = []
    unsyndicated = []
    dict_comp = filter_list(dict_comp, years, rnds, industries, sub_scopes, required_irr, max_irr)
    for comp in dict_comp.values():
        df = comp["df"]
        df2 = filter_year_round(df, years, rounds)
        df2 = filter_money(df2)
        df2 = filter_investorrow_not_null(df2)
        for id, row in df2.iterrows():
            if len(row["investor"]) == 1:
                unsyndicated += [row["irr"]]
            if len(row["investor"]) > 1:
                syndicated += [row["irr"]]
    # if len(syndicated) > 5:
    print("type","syndicated", "unsyndicated")
    print("num", len(syndicated), len(unsyndicated))
    print("irr_mean", np.mean(syndicated), np.mean(unsyndicated))
    print("irr_median", np.median(syndicated), np.median(unsyndicated))
    print("stddev", np.std(syndicated), np.std(unsyndicated))
    return syndicated, unsyndicated

def analyze_irr(dict_comp, years, rounds, required_irr,
                max_irr = 10000, industries = None, sub_scopes = None):
    years = [str(a) for a in years]
    syndicated = []
    unsyndicated = []
    dict_comp = filter_list(dict_comp, years, rounds, industries, sub_scopes, required_irr, max_irr)
    for name, comp in dict_comp.items():
        df = comp["df"]
        df2 = filter_year_round(df, years, rounds)
        df2 = filter_money(df2)
        df2 = filter_investorrow_not_null(df2)
        for id, row in df2.iterrows():
            if len(row["investor"]) == 1:
                unsyndicated += [row["irr"]]
            if len(row["investor"]) > 1:
                syndicated += [row["irr"]]
        # print(name, comp["scope"], comp["sub_scope"], "\n", df2[["date", "investor", "irr"]])
    # if len(syndicated) > 5:
    print("TASK: ANALYZE IRR, REQ=", required_irr, ", INDUSTRY: ", industries, "ROUNDS: ", rounds)
    print("type","syndicated", "unsyndicated")
    print("num", len(syndicated), len(unsyndicated))
    print("irr_mean", np.mean(syndicated), np.mean(unsyndicated))
    print("irr_median", np.median(syndicated), np.median(unsyndicated))
    print("stddev", np.std(syndicated), np.std(unsyndicated))
    return len(syndicated), (unsyndicated)

def test_school(dict_comp, years, rounds, industries = None, sub_scopes = None):
    years = [str(a) for a in years]
    #TODO: Implement
    return None

# for i in np.arange(0,40,5):
#     analyze_irr(dict_comp, [2014,2015, 2016, 2017, 2018], ["A轮"], i, max_irr=30)
# quit()

years, rndss = [2014,2015, 2016, 2017, 2018], [["Pre-A轮","A轮","A+轮","Pre-B轮","B轮","B+轮"]]
years, rndss = [2014,2015, 2016, 2017, 2018], [["天使轮","种子轮","Pre-A轮","A轮","A+轮","Pre-B轮","B轮","B+轮"], ["天使轮","种子轮"],["Pre-A轮","A轮","A+轮"],["Pre-B轮","B轮","B+轮"]]
industries_all = ['文化娱乐', '游戏', '旅游', '教育', '医疗健康', '体育运动', '企业服务', '物流', '农业', '电子商务', '房产服务', '汽车交通', '金融', '广告营销', '工具软件', '社交网络', '硬件', '本地生活']
# industries_all = ['物流']
subscopes_all = ['宠物服务','金融信息化',  '旅游工具及社区',  '销售营销',  '安全隐私',  '机器人',  '移动及网络广告',  '其他金融',  '维修服务',  '生鲜食品',  '商务社交', '图片照片',  '数字虚拟商品',  '品牌快递',  '游戏发行及渠道',  '交通出行',  '跨境物流',  '本地综合生活',  '影视',  '汽车电商',  '健康保健',  '投融资',  '虚拟货币',  '消费金融',  '其他房产服务',  '新型农业系统',  '女性社群',  '跨境游',  '系统工具',  '人力资源',  '媒体及阅读',  '外汇期货贵金属',  '美食餐饮',  '同性社交',  '浏览器',  '音乐',  '其他社交',  '农机农资及装备',  '家居家纺',  '广告平台',  '大众健身',  '游戏直播及玩家',  '医疗器械及硬件',  '母婴玩具',  '珠宝首饰',  '芯片半导体',  '服装服饰',  '化妆品',  '其他教育',  '商户服务及信息化',  '飞行器',  '赛事运营及经纪',  '兴趣社区',  '借贷',  '房产电商',  '动漫',  '其他硬件服务',  '办公OA',  '整合营销传播',  '装修装潢',  '实用生活服务',  '股票',  '奢侈品',  '可穿戴设备',  '3C电子',  '优化清理',  '农业生物技术',  '校园服务',  '消费电子',  '其他生活服务',  '图像视频',  '房产金融',  '专科服务',  'B2D开发者服务',  '其他广告',  '种植养殖',  '图书影音',  '生物技术和制药',  'K12',  '主题特色游',  '设计及创意',  '综合企业服务',  '演艺',  '企业安全',  '家政服务',  '艺术',  '游戏媒体及社区',  '农业电商',  '仓储服务',  '搜索引擎',  '汽车金融',  '游戏硬件',  '数据服务',  '事项及效率',  '游戏开发商',  '兴趣教育',  '信用及征信',  '教育综合服务',  '医药电商',  '车联网及硬件',  '彩票',  '国内游',  '广告技术',  '行业信息化及解决方案',  '其他汽车服务',  '语言学习',  '综合工具服务',  '汽车综合服务',  '传统广告',  '医疗综合服务',  '位置定位',  '婚礼婚庆',  '车载及出行',  '综合文娱',  '黑科技及前沿技术',  '综合物流',  '陌生人交友',  '其他企业服务',  '其他电商服务',  '医疗信息化',  '综合社交',  '跨境电商',  '体育媒体及社区',  '财务税务',  '综合电商',  '汽车后服务',  '同城配送',  'IP知识产权',  '传感器及中间件',  '无线通讯',  '体育用品及装备',  '其他农业',  '其他游戏服务',  '校园社交',  '医生服务',  '旅游综合服务',  '其他物流',  '游戏道具衍生品',  '家庭熟人社交',  '美业服务',  '休闲娱乐',  '交通食宿',  '金融综合服务',  '农业媒体及社区',  '租房',  '教育信息化',  '户外及极限运动',  '物流信息化',  '车主工具及服务',  '智能家居',  '货运物流',  'IT基础设施',  '职业培训',  '出国留学',  '电商解决方案',  '支付',  '理财',  '应用商店',  '游戏综合服务',  '其他旅游服务',  '寻医诊疗',  '综合硬件',  '其他医疗服务',  '文件文档',  '旅游信息化',  '其他工具',  '客户服务',  '小区服务',  '房产综合服务',  '综合体育',  '商业房产',  '视频',  '二手车',  '新能源新材料',  '保险',  '其他文娱服务',  '法律服务',  '教辅设备',  '体育设施及场馆',  '3D打印',  '儿童早教',  '其他体育服务',  '婚恋交友',  '房产信息化',  '综合农业',  '大宗商品',  '高等教育']
# test_syndication(dict_comp, year, rnd)

# dict_comp = filter_list(dict_comp, year, rnd)
# print(dict_comp)
# print(vcs_money_returns(dict_comp, years, rnds))
# print(vcs_mean_returns(dict_comp, years, rnds))

# for rnds in rndss:
#     print("INDUSTRY: ", "ALL", "ROUNDS: ", rnds)
#     test_syndication(dict_comp, years, rnds, 30, 10000)
#     print("\n")
#
#     for industry in industries_all:
#         print("INDUSTRY: ", industry, "ROUNDS: ", rnds)
#         test_syndication(dict_comp, years, rnds, industries = [industry], required_irr=30, max_irr=10000)
#         print("\n")
#

counts = {}
for rnds in rndss:
    for subscope in subscopes_all:
        print("SUBSCOPE: ", subscope, "ROUNDS: ", rnds)
        s,u = test_syndication(dict_comp, years, rnds, sub_scopes = [subscope])
        counts[subscope] = (len(s), len(u))
        print("\n")

# categories = categories_subcategories()
# categories = {k:[(a, counts(a)) for a in v] for k,v in categories.items()}
# print(categories)
# input()
