import MySQLdb
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine

db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="root",  # your password
                     db="data5G")        # name of the data base

cur = db.cursor()

cust = pd.read_csv('customer.csv', sep=',', header=0, parse_dates=['ActionTS'])
acc = cust.fillna(value={'CA_ID': -1})
acc['C_ID'] = acc['C_ID'].astype('int32')
acc['CA_ID'] = acc['CA_ID'].astype('int32')

acc.set_index('CA_ID', inplace=True)

cur.execute("SELECT SK_BROKERID, BrokerID FROM DimBroker")
row = cur.fetchall()
dimBroker = pd.DataFrame(list(row), columns=["SK_BROKERID", "BrokerID"])
dimBroker.set_index('SK_BROKERID', inplace=True)

cur.execute("SELECT SK_CustomerID, CustomerID, EffectiveDate, EndDate FROM DimCustomer")
row = cur.fetchall()
dimCustomer = pd.DataFrame(list(row), columns=['SK_CustomerID', 'CustomerID', 'EffectiveDate', 'EndDate'])
dimCustomer.set_index('SK_CustomerID')

columns = ['AccountID', 'SK_BrokerID', 'SK_CustomerID',
           'Status', 'AccountDesc', 'TaxStatus', 'IsCurrent',
           'BatchID', 'EffectiveDate', 'EndDate']

dimAccount = pd.DataFrame(columns=columns)
nrow = dict()
i = 1
print "Loading the dimension new and updating account this could take several minutes "
EndTime = pd.Timestamp.max

for index, record in acc.iterrows():
    if record.ActionType == 'NEW' or record.ActionType == 'ADDACCT':
        nrow['AccountID'] = index
        nrow['AccountDesc'] = record.CA_NAME
        nrow['TaxStatus'] = record.CA_TAX_ST
        nrow['EffectiveDate'] = record.ActionTS
        nrow['EndDate'] = EndTime
        nrow['IsCurrent'] = 1
        nrow['BatchID'] = 1
        nrow['Status'] = 'Active'

        r = dimBroker.loc[dimBroker['BrokerID'] == record.CA_B_ID]
        if r.shape[0] > 0:
            nrow['SK_BrokerID'] = r.index[0]

        r = dimCustomer[(dimCustomer['CustomerID'] == record.C_ID) &
                        (dimCustomer['EffectiveDate'] <= record.ActionTS) &
                        (dimCustomer['EndDate'] > record.ActionTS)]

        if r.shape[0] > 0:
            nrow['SK_CustomerID'] = r.index[0]

        dimAccount = dimAccount.append(nrow, ignore_index=True)

    elif record.ActionType == "UPDACCT":
        new_account = dimAccount[dimAccount['AccountID'] == index].tail(1).copy()
        old_account = new_account.copy()
        new_account['EffectiveDate'] = record.ActionTS
        new_account['EndDate'] = EndTime

        for field in record.index:
            if record[field] is None:
                continue

            if field == 'C_ID':
                r = dimCustomer[(dimCustomer['CustomerID'] == record.C_ID) &
                                (dimCustomer['EffectiveDate'] <= record.ActionTS) &
                                (dimCustomer['EndDate'] > record.ActionTS)]

                if r.shape[0] > 0:
                    new_account['SK_CustomerID'] = r.index[0]

            if field == 'CA_TAX_ST':
                new_account['TaxStatus'] = record.CA_TAX_ST

            if field == 'CA_B_ID':
                r = dimBroker.loc[dimBroker['BrokerID'] == record.CA_B_ID]

                if r.shape[0] > 0:
                    new_account['SK_BrokerID'] = r.index[0]

            if field == 'CA_NAME':
                new_account['AccountDesc'] = record.CA_NAME

        old_account['EndDate'] = record.ActionTS
        old_account['IsCurrent'] = 0
        dimAccount.update(old_account)
        dimAccount = dimAccount.append(new_account, ignore_index=True)

    elif record.ActionType == 'CLOSEACCT':
        new_accounts = dimAccount[dimAccount['AccountID'] == index].tail(1).copy()
        old_accounts = new_accounts.copy()
        new_accounts['EffectiveDate'] = record.ActionTS
        new_accounts['Status'] = 'INACTIVE'
        old_accounts['EndDate'] = record.ActionTS
        dimAccount.update(old_accounts)
        dimAccount = dimAccount.append(new_accounts, ignore_index=True)

    elif record.ActionType == 'UPDCUST':
        customer = dimCustomer[(dimCustomer['CustomerID'] == record.C_ID) & (
                    (dimCustomer['EffectiveDate'] == record.ActionTS) |
                    (dimCustomer['EndDate'] == record.ActionTS))]

        try:
            old_accounts = dimAccount[dimAccount['SK_CustomerID'] == int(customer.head(1).SK_CustomerID)].copy()
            new_accounts = old_accounts.copy()
            new_accounts.loc[:, 'SK_CustomerID'] = int(customer.tail(1).SK_CustomerID)
            new_accounts.loc[:, 'EffectiveDate'] = record.ActionTS
            new_accounts.loc[:, 'EndDate'] = EndTime
            old_accounts.loc[:, 'EndDate'] = record.ActionTS
            dimAccount.update(old_accounts)
            dimAccount = dimAccount.append(new_accounts, ignore_index=True)
        except Exception:
            print record.C_ID
            print record.ActionTS
            print customer

    elif record.ActionType == 'INACT':
        customer = dimCustomer[(dimCustomer['CustomerID'] == record.C_ID) & (
                (dimCustomer['EffectiveDate'] == record.ActionTS) |
                (dimCustomer['EndDate'] == record.ActionTS))]

        old_accounts = dimAccount[dimAccount['SK_CustomerID'] == int(customer.head(1).SK_CustomerID)].copy()
        new_accounts = old_accounts.copy()
        new_accounts.loc[:, 'SK_CustomerID'] = int(customer.tail(1).SK_CustomerID)
        new_accounts.loc[:, 'Status'] = 'INACTIVE'
        new_accounts.loc[:, 'IsCurrent'] = 0
        new_accounts.loc[:, 'EffectiveDate'] = record.ActionTS
        new_accounts.loc[:, 'EndDate'] = EndTime
        old_accounts.loc[:, 'EndDate'] = record.ActionTS

        dimAccount.update(old_accounts)
        dimAccount = dimAccount.append(new_accounts, ignore_index=True)

    nrow.clear()
    if i%2000 == 0:
        print 'progress: %.2f percent'%(i/float(acc.shape[0])*100)

    i += 1

dimAccount.index.name = 'SK_AccountID'
dimAccount.fillna(value={'SK_BrokerID': -1}, inplace=True)

dimAccount['SK_CustomerID'] = dimAccount['SK_CustomerID'].astype('int32')
dimAccount['SK_BrokerID'] = dimAccount['SK_BrokerID'].astype('int32')
dimAccount.index.astype('int32', copy=False)
dimAccount['AccountID'] = dimAccount['AccountID'].astype('int32')
dimAccount['BatchID'] = dimAccount['BatchID'].astype('int32')
dimAccount['IsCurrent'] = dimAccount['IsCurrent'].astype('int32')

engine = create_engine("mysql://root:root@localhost/data5G")
con = engine.connect()
sql.to_sql(dimAccount, con=con, name='DimAccount', if_exists='append')
