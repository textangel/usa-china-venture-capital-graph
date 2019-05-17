import pandas as pd, math
import numpy as np

filepaths = {
"seed": "../graphs/seed_coinvest_counts.csv",
"A": "../graphs/a_coinvest_counts_.csv",
 "B": "../graphs/b_coinvest_counts_.csv"
}
#
# def safe_corr(l1,l2):
#     []

for k, path in filepaths.items():
    csv = pd.read_csv(path)
    csv2 = csv[["1", 'eigencentrality' , 'Degree', 'closnesscentrality', 'pageranks', 'betweenesscentrality']]
    csv2.columns = ["Return Rate", "Eigenvector Centrality", "Degree Centrality", "Closeness Centrality", "PageRank", "Betweenness Centrality"]
    print(k, "\n", csv2.corr())
    csv2.corr().to_csv("csv_cor_{}.csv".format(k))

    # irr = csv["1"]
    # eigen = csv['eigencentrality']
    # deg = csv['Degree']
    # close = csv['closnesscentrality']
    # page = csv['pageranks']
    # between = csv['betweenesscentrality']


