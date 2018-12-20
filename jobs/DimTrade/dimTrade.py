import MySQLdb
import datetime
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine


columns = ['T_ID', 'T_DTS', 'T_ST_ID', 'T_TT_ID',
           'T_IS_CASH', 'T_S_SYMB', 'T_QTY', 'T_BID_PRICE',
           'T_CA_ID', 'T_EXEC_NAME', 'T_TRADE_PRICE',
           'T_CHRG', 'T_COMM', 'T_TAX']

trade = pd.read_csv('Trade.txt', sep='|', names=columns, parse_dates=['T_DTS'], index_col='T_ID')

columns = ['TH_T_ID', 'TH_DTS', 'TH_ST_ID']

trade_history = pd.read_csv('TradeHistory.txt', sep='|', names=columns, parse_dates=['TH_DTS'], index_col='TH_T_ID')

status_type = pd.read_csv('StatusType.txt', sep='|', names=['ST_ID', 'ST_NAME'], index_col='ST_ID')

trade_type = pd.read_csv('TradeType.txt', sep='|', names=['TT_ID', 'TT_NAME', 'TT_IS_SELL', 'TT_IS_MRKT'], index_col='TT_ID')

db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="root",  # your password
                     db="data5G")        # name of the data base

cur = db.cursor()

joined = trade.join(trade_history)

cur.execute("SELECT SK_SecurityID, SK_CompanyID, Symbol, EffectiveDate, EndDate FROM DimSecurity")
row = cur.fetchall()
dimSecurity = pd.DataFrame(list(row), columns=["SK_SecurityID", "SK_CompanyID", "Symbol", "EffectiveDate", "EndDate"])
dimSecurity.set_index('SK_SecurityID', inplace=True)

cur.execute("SELECT SK_AccountID, SK_CustomerID, SK_BrokerID, AccountID, EffectiveDate, EndDate FROM DimAccount")
row = cur.fetchall()
dimAccount = pd.DataFrame(list(row), columns=["SK_AccountID", "SK_CustomerID", "SK_BrokerID", "AccountID", "EffectiveDate", "EndDate"])
dimAccount.set_index('SK_AccountID', inplace=True)

cur.execute("SELECT SK_DateID, DateValue FROM DimDate")
row = cur.fetchall()
dimDate = pd.DataFrame(list(row), columns=['SK_DateID', 'DateValue'])
dimDate['DateValue'] = pd.Series([datetime.datetime.date(d) for d in dimDate['DateValue']])
dimDate.set_index('SK_DateID', inplace=True)

cur.execute("SELECT SK_TimeID, TimeValue FROM DimTime")
row = cur.fetchall()
dimTime = pd.DataFrame(list(row), columns=['SK_TimeID', 'TimeValue'])
dimTime['TimeValue'] = pd.Series([datetime.datetime.time(d) for d in dimTime['TimeValue']])
dimTime.set_index('SK_TimeID', inplace=True)


columns = ['TradeID', 'SK_BrokerID', 'SK_CreateDateID', 'SK_CreateTimeID', 'SK_CloseDateID',
           'SK_CloseTimeID', 'Status', 'Type', 'CashFlag', 'SK_SecurityID',
           'SK_CompanyID', 'Quantity', 'BidPrice',
           'SK_CustomerID', 'SK_AccountID', 'ExecutedBy', 'TradePrice',
           'Fee', 'Commission', 'Tax', 'BatchID']

dimTrade = pd.DataFrame(columns=columns)
#joined.index.name = 'TradeID'
index_set = set()
nrow = dict()
i = 1

for index, record in joined.iterrows():
    nrow['TradeID'] = index
    nrow['CashFlag'] = record.T_IS_CASH
    nrow['Quantity'] = record.T_QTY
    nrow['BidPrice'] = record.T_BID_PRICE
    nrow['ExecutedBy'] = record.T_EXEC_NAME
    nrow['TradePrice'] = record.T_TRADE_PRICE
    nrow['Fee'] = record.T_CHRG
    nrow['Commission'] = record.T_COMM
    nrow['Tax'] = record.T_TAX
    nrow['Status'] = status_type.loc[record.TH_ST_ID].ST_NAME
    nrow['Type'] = trade_type.loc[record.T_TT_ID].TT_NAME
    nrow['BatchID'] = 1

    if (record.TH_ST_ID == "SBMT" and record.T_TT_ID in ["TMB", "TMS"]) or record.TH_ST_ID == "PNDG":
        nrow['SK_CreateDateID'] = dimDate[dimDate['DateValue'] == record.TH_DTS.date()].index[0]
        nrow['SK_CreateTimeID'] = dimTime[dimTime['TimeValue'] == record.TH_DTS.time()].index[0]
        #nrow['SK_CreateTimeID'] = dimTime[dimTime['TimeValue'] == record.TH_DTS.time().strftime('%T')].index[0]
        if index not in index_set:
            nrow['SK_CloseDateID'] = None
            nrow['SK_CloseTimeID'] = None

    if record.TH_ST_ID in ["CMPT", "CNCL"]:
        nrow['SK_CloseDateID'] = dimDate[dimDate['DateValue'] == record.TH_DTS.date()].index[0]
        nrow['SK_CloseTimeID'] = dimTime[dimTime['TimeValue'] == record.TH_DTS.time()].index[0]
        #nrow['SK_CloseTimeID'] = dimTime[dimTime['TimeValue'] == record.TH_DTS.time().strftime('%T')].index[0]
        if index not in index_set:
            nrow['SK_CreateDateID'] = None
            nrow['SK_CreateTimeID'] = None

    if index not in index_set:
        row = dimSecurity[(dimSecurity['Symbol'] == record.T_S_SYMB) &
                          (dimSecurity['EffectiveDate'] <= record.TH_DTS.date()) &
                          (dimSecurity['EndDate'] > record.TH_DTS.date())]

        if row.shape[0] > 0:
            nrow['SK_SecurityID'] = row.index[0]
            nrow['SK_CompanyID'] = row.iloc[0]['SK_CompanyID']
        print record.T_CA_ID
        print record.TH_DTS
        print (dimAccount['AccountID'] == record.T_CA_ID).sum()
        print ((dimAccount['AccountID'] == record.T_CA_ID) & (dimAccount['EffectiveDate'] <= record.TH_DTS.date())).sum()
        print (dimAccount['EndDate'] > record.TH_DTS).sum()

        row = dimAccount[(dimAccount['AccountID'] == record.T_CA_ID) &
                         (dimAccount['EffectiveDate'] <= record.TH_DTS.date()) &
                         (dimAccount['EndDate'] > record.TH_DTS)]

        if row.shape[0] > 0:
            nrow['SK_AccountID'] = row.index[0]
            nrow['SK_CustomerID'] = row.iloc[0]['SK_CustomerID']
            nrow['SK_BrokerID'] = row.iloc[0]['SK_BrokerID']

        index_set.add(index)
        dimTrade = dimTrade.append(nrow, ignore_index=True)
    else:
        new_row = pd.DataFrame(nrow, index=[index])
        dimTrade.update(new_row)

    nrow.clear()

    if (index+1)%2 == 0:
        print 'progress: %.2f percent' % (i / float(joined.shape[0]) * 100)
        break

    i += 1

print dimTrade.head()

dimTrade.set_index('TradeID', inplace=True)
engine = create_engine("mysql://root:root@localhost/data5G")
con = engine.connect()
sql.to_sql(dimTrade, con=con, name='DimTrade', if_exists='append')
