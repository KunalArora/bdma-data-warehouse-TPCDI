import xml.etree.ElementTree as ET
import pandas as pd

customerMgmt = ET.parse('CustomerMgmt.xml')
actions = customerMgmt.getroot().getchildren()

columns = ['ActionType','ActionTS','C_ID','CA_ID','CA_TAX_ST','CA_B_ID','CA_NAME']
accounts = pd.DataFrame(columns=columns)

nrow = dict()
i = 0

for action in actions:
    nrow.update(action.attrib)
    for cust in action:
        nrow['C_ID'] = cust.attrib['C_ID']
        flag = False
        if len(cust.findall('Account')) > 0:
            flag = True
        for acc in cust.findall('Account'):
            nrow.update(acc.attrib)
            for v in acc:
                nrow[v.tag] = v.text
            accounts = accounts.append(nrow, ignore_index=True)

        if not flag:
            accounts = accounts.append(nrow, ignore_index=True)

    nrow.clear()
    if i%1000==0:
        print i
    i+=1

print accounts.head()
print accounts.shape
accounts.to_csv('customer.csv', sep=',', encoding='utf-8', index=False)
