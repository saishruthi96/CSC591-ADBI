import random
import math
import sys
import pandas as pd
df = pd.read_csv("bidder_dataset.csv", delimiter=",")
df = df.fillna(0)
# df1=df.groupby('Keyword')
p = df.loc[df["Budget"] != 0][['Advertiser', 'Budget']]
id_dict = dict()
item_dict = list()
q_dt = pd.read_csv("queries.txt", sep="\n", header=None)
queries = list()
for index, row in q_dt.iterrows():
    queries.append(row[0])
for index, row in df.iterrows():
    item_dict.append((int(row["Advertiser"]), (row["Keyword"], float(row["Bid Value"]))))
random.seed(0)

def set_data():
    for index, row in p.iterrows():
        id_dict.update({int(row["Advertiser"]): [int(row["Budget"]), int(row["Budget"])]})


def Greedy(queries1):

    set_data()
    max_revenue = 0.0
    for i in queries1:
        id = -1.0
        bid = 0
        for j in item_dict:
                if j[1][0] == i and id_dict[j[0]][0] >= j[1][1]:
                    if j[1][1] > bid:
                        bid = j[1][1]
                        id = j[0]
                    elif j[1][1] == bid:
                        if j[0] < id:
                            bid = j[1][1]
                            id = j[0]
        if id != -1 and bid!=0:
            max_revenue =(max_revenue + bid)
            id_dict[id][0] = (id_dict[id][0] - bid)
    return (max_revenue)

def Mssv(queries):

    set_data()
    max_revenue = 0.0
    for i in queries:
        id = -1
        bid = 0
        bidcon =0
        for j in item_dict:
                if j[1][0] == i and id_dict[j[0]][0] >= j[1][1]:
                    xu = (id_dict[j[0]][1] - id_dict[j[0]][0])/id_dict[j[0]][1]
                    cond = j[1][1] * (1 - math.exp(xu - 1))
                    if (bidcon == 0):
                        bidcon = cond
                        bid = j[1][1]
                        id = j[0]
                    if cond > bidcon:
                        bidcon = cond
                        bid = j[1][1]
                        id = j[0]
                    elif cond > bidcon:
                        if j[0] < id:
                            bidcon = cond
                            bid = j[1][1]
                            id = j[0]
        # set_df = df1.get_group(i).sort_values(by=['Bid Value'], ascending=False)
        # for index, row in set_df.iterrows():
        #     if row['Keyword'] == i and (id_dict[row['Advertiser']][0] >= row['Bid Value']):
        #         xu = float(id_dict[row['Advertiser']][1] - id_dict[row['Advertiser']][0]) / float(id_dict[row['Advertiser']][1])
        #         cond = row['Bid Value'] * (1 - math.exp(xu - 1))
        #         if (bidcon == 0):
        #             bidcon = cond
        #             bid = row['Bid Value']
        #             id = row['Advertiser']
        #         if cond > bidcon:
        #             bidcon = cond
        #             bid = row['Bid Value']
        #             id = row['Advertiser']
        #         elif cond == bidcon:
        #                 if row['Advertiser'] < id:
        #                     bidcon = cond
        #                     bid = row['Bid Value']
        #                     id = row['Advertiser']
        if id != -1 and bid != 0:
            max_revenue+= bid
            id_dict[id][0] -= bid
    return (max_revenue)

def Balance(queries):

    set_data()
    max_revenue = 0.0
    for i in queries:
        id = -1
        bid = 0.0
        bidcon = 0
        for j in item_dict:
                if j[1][0] == i and id_dict[j[0]][0] >= j[1][1]:
                    cond = (id_dict[j[0]][0])
                    if (bidcon == 0):
                        bidcon = cond
                        bid = j[1][1]
                        id = j[0]
                    if cond > bidcon:
                        bidcon = cond
                        bid = j[1][1]
                        id = j[0]
                    elif cond > bidcon:
                        if j[0] < id:
                            bidcon = cond
                            bid = j[1][1]
                            id = j[0]
        #
        # set_df = df1.get_group(i).sort_values(by=['Bid Value'], ascending=False)
        # for index, row in set_df.iterrows():
        #     if row['Keyword'] == i and (id_dict[row['Advertiser']][0] >= row['Bid Value']):
        #         cond = (id_dict[row['Advertiser']][0])
        #         if (bidcon == 0):
        #             bidcon = id_dict[row['Advertiser']][0]
        #             bid = row['Bid Value']
        #             id = row['Advertiser']
        #         if id_dict[row['Advertiser']][0] > bidcon:
        #             bidcon = id_dict[row['Advertiser']][0]
        #             bid = row['Bid Value']
        #             id = row['Advertiser']
        #         elif id_dict[row['Advertiser']][0] == bidcon:
        #             bidcon = id_dict[row['Advertiser']][0]
        #             if row['Advertiser'] < id:
        #                 bidcon = id_dict[row['Advertiser']][0]
        #                 bid = row['Bid Value']
        #                 id = row['Advertiser']
        if id != -1 and bid != 0:
            max_revenue = (max_revenue + bid)
            id_dict[id][0] = (id_dict[id][0] - bid)
    return (max_revenue)

def calculate_budget():
    total =0
    for index, row in p.iterrows():
        total+=int(row["Budget"])
    return total
if len(sys.argv) > 1:
    if(sys.argv[1] == 'greedy'):
        print('Revenue:' + str(round(Greedy(queries),2)))
        revenue=0
        for i in range(0,100):
            copyqueries = queries
            random.shuffle(copyqueries)
            revenue+=Greedy(copyqueries)
        alt = revenue/100

        print('Competitive Ratio:'+str(round(alt / calculate_budget(),2)))

    elif(sys.argv[1] == 'mssv'):
        print('Revenue:'+str(round(Mssv(queries),2)))
        revenue = 0
        for i in range(100):
            copyqueries = queries
            random.shuffle(copyqueries)
            revenue += Mssv(copyqueries)
        alt = (revenue / 100)
        print('Competitive Ratio:' + str(round(alt / calculate_budget(),2)))

    elif (sys.argv[1] == 'balance'):
        print('Revenue:' + str(round(Balance(queries),2)))
        revenue = 0
        for i in range(100):
            copyqueries = queries
            random.shuffle(copyqueries)
            revenue += Balance(copyqueries)
        alt = (revenue / 100)
        print('Competitive Ratio:' + str(round(alt / calculate_budget(),2)))
    else:
        print('Invalid entry.Please follow the format : python adwords.py (greedy|mssv|balance)')
