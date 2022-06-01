#### IMPORT NECESSARY LIBRARIES ###
import pandas as pd
pd.options.mode.chained_assignment = None
import requests
from bs4 import BeautifulSoup as Soup
import csv
from io import StringIO
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import AsIs
from datetime import timedelta,datetime
from dateutil import relativedelta
import numpy as np

### XML FORMAT TO GET DATA FROM RUNNING TALLY ###

license_xml = """<ENVELOPE>
            <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>EXPORT</TALLYREQUEST>
            <TYPE>FUNCTION</TYPE>
            <ID>$$LicenseInfo</ID>
            </HEADER>
            <BODY>
            <DESC>
            <FUNCPARAMLIST>
            <PARAM>AccountID</PARAM>
            </FUNCPARAMLIST>
            </DESC>
            </BODY>
            </ENVELOPE>"""

system_xml = """<ENVELOPE>
            <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>EXPORT</TALLYREQUEST>
            <TYPE>FUNCTION</TYPE>
            <ID>$$SysInfo</ID>
            </HEADER>
            <BODY>
            <DESC>
            <FUNCPARAMLIST>
            <PARAM>SystemName</PARAM>
            </FUNCPARAMLIST>
            </DESC>
            </BODY>
            </ENVELOPE>"""

company_xml = """<ENVELOPE>
<HEADER>
<VERSION>1</VERSION>
<TALLYREQUEST>Export</TALLYREQUEST>
<TYPE>Data</TYPE>
<ID>List of Companies</ID>
</HEADER>
<BODY>
<DESC>
<TDL>
<TDLMESSAGE>
<REPORT NAME="List of Companies" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
<FORMS>List of Companies</FORMS>
</REPORT>
<FORM NAME="List of Companies" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
<TOPPARTS>List of Companies</TOPPARTS>
<XMLTAG>"List of Companies"</XMLTAG>
</FORM>
<PART NAME="List of Companies" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
<TOPLINES>List of Companies</TOPLINES>
<REPEAT>List of Companies : Collection of Companies</REPEAT>
<SCROLLED>Vertical</SCROLLED>
</PART>
<LINE NAME="List of Companies" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
<LEFTFIELDS>Name</LEFTFIELDS>
</LINE>
<FIELD NAME="Name" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
<SET>$Name</SET>
<XMLTAG>"NAME"</XMLTAG>
</FIELD>
<COLLECTION NAME="Collection of Companies" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
<TYPE>Company</TYPE>
<FETCH>NAME</FETCH>
</COLLECTION>
</TDLMESSAGE>
</TDL>
</DESC>
</BODY>
</ENVELOPE>"""


def ledger_xml_request(company):
    ledger_xml="""<ENVELOPE><HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>My Ledgers</ID>
            </HEADER>
            <BODY>
            <DESC>
            <STATICVARIABLES>
            <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
            <SVCURRENTCOMPANY>""" +  company + """</SVCURRENTCOMPANY>
            </STATICVARIABLES>
            <TDL>
            <TDLMESSAGE>
            <Collection NAME="My Ledgers" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
            <TYPE>Ledgers</TYPE>
            <NATIVEMETHOD>MASTERID</NATIVEMETHOD>
            <NATIVEMETHOD>ALTERID</NATIVEMETHOD>
            <NATIVEMETHOD>Name</NATIVEMETHOD>
            <NATIVEMETHOD>Parent</NATIVEMETHOD>
            <NATIVEMETHOD>_PrimaryGroup</NATIVEMETHOD>
            <NATIVEMETHOD>GrandParent</NATIVEMETHOD>
            <NATIVEMETHOD>OpeningBalance</NATIVEMETHOD>
            <NATIVEMETHOD>ClosingBalance</NATIVEMETHOD>
            </Collection>
            </TDLMESSAGE>
            </TDL>
            </DESC>
            </BODY>
            </ENVELOPE>"""
    return ledger_xml

def vchtype_xml_request(company):
    vchtype_xml = """<ENVELOPE>
                        <HEADER>
                        <TALLYREQUEST>EXPORT DATA </TALLYREQUEST>
                        </HEADER>
                        <BODY>
                        <EXPORTDATA>
                        <REQUESTDESC>
                        <REPORTNAME>LIST OF ACCOUNTS</REPORTNAME>
                        <STATICVARIABLES>
                        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                        <SVCURRENTCOMPANY>""" +  company + """</SVCURRENTCOMPANY>
                        <ACCOUNTTYPE>VoucherTypes</ACCOUNTTYPE>
                        </STATICVARIABLES>
                        </REQUESTDESC>
                        </EXPORTDATA>
                        </BODY>
                        </ENVELOPE>"""
    return vchtype_xml        
        
def daybook_xml_request(company, from_date, end_date):
    db_xml="""<ENVELOPE><HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>All_Entries</ID>
            </HEADER>
            <BODY>
            <DESC>
            <STATICVARIABLES>
            <SVFROMDATE TYPE="Date">""" + from_date.strftime('%d-%b-%Y') + """</SVFROMDATE>
            <SVTODATE TYPE="Date">""" + end_date + """</SVTODATE>
            <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
            <SVCURRENTCOMPANY>""" +  company + """</SVCURRENTCOMPANY>
            <EXPLODEFLAG>Yes</EXPLODEFLAG>
            </STATICVARIABLES>
            <TDL>
            <TDLMESSAGE>
            <Collection NAME="All_Entries" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
            <TYPE>Vouchers</TYPE>
            <NATIVEMETHOD>Narration</NATIVEMETHOD>
            <FETCH>ALTERID</FETCH>
            <FETCH>FLDVCHCREATIONDATE.LIST</FETCH>
            <FETCH>ALLLEDGERENTRIES</FETCH>
            <FETCH>BILLALLOCATIONS.LIST</FETCH>
            <FETCH>ALLINVENTORYENTRIES.LIST</FETCH>
            <FETCH>CATEGORYALLOCATIONS.LIST</FETCH>
            </Collection>
            </TDLMESSAGE>
            </TDL>
            </DESC>
            </BODY>
            </ENVELOPE>"""
    return db_xml

### FUNCTION SEND THE REQUEST TO TALLY AND OBTAIN XML RESPONSE ###
def tally_request(xml_string):
    ''' Takes the Tally XML request in string format and posts
    the same to the tally webserver. Receives the response from tally 
    and converts to xml using beautiful soup
    '''
    headers = {'Content-Type': 'soap/xml'}
    xml_data=requests.post('http://localhost:9000', data=xml_string, headers=headers).text
    xml_data=xml_data.replace('&amp;','&#38;')
    xml_data=xml_data.replace('&apos;','&#39;')
    xml_data = Soup(xml_data,'xml')
    return xml_data

### HELPER FUNCTION TO GET THE TALLY DETAILS FROM XML RESPONSE ###
def get_account_id(xml_data):
    ''' Takes the Tally XML request in string format and posts
    the same to the tally webserver. Receives the response from tally 
    and returns the account id of the running tally
    '''
    account = xml_data.find('RESULT')
    return account.text.lower()

def get_system_name(xml_data):
    ''' Takes the Tally XML request in string format and posts
    the same to the tally webserver. Receives the response from tally 
    and returns the system name of the running tally
    '''
    system_name = xml_data.find('RESULT')
    return system_name.text.lower()

def get_running_companies(xml_data):
    ''' Takes the Tally XML request in string format and posts
    the same to the tally webserver. Receives the response from tally 
    and returns the list of the running companies in tally
    '''
    company_list = []
    for names in  xml_data.findAll('LISTOFCOMPANIES'):
        for companies in names.findChildren('NAME',recursive=False):
            company_list.append(companies.text.lower())
    return company_list 


def get_ledgers_dataframe(xml_data):
    ledgers=pd.DataFrame([])
    for item in xml_data.findAll('LEDGER'):
        name = item.get('NAME')
        for parent in item.findChildren('PARENT',recursive=False):
            for gparent in item.findChildren('GRANDPARENT',recursive=False):
                for primary in item.findChildren('_PRIMARYGROUP',recursive=False):
                    for obal in item.findChildren('OPENINGBALANCE',recursive=False):
                        for cbal in item.findChildren('CLOSINGBALANCE',recursive=False):
                            for id in item.findChildren('MASTERID',recursive=False):
                                for alterid in item.findChildren('ALTERID',recursive=False):
                                    ledgers=ledgers.append(pd.DataFrame({
                                    'master_id': id.text,
                                    'alter_id': alterid.text,
                                    'primary_group': primary.text,
                                    'grand_parent':gparent.text,
                                    'parent':parent.text,
                                    'ledger': name,
                                    'opening_balance':obal.text,
                                    'closing_balance': cbal.text
                                    },index=[0]),ignore_index=True)
    
    ledgers['master_id']=pd.to_numeric(ledgers['master_id'],errors='coerce',downcast='integer')
    ledgers['alter_id']=pd.to_numeric(ledgers['alter_id'],errors='coerce',downcast='integer')                                            
    ledgers['opening_balance']=pd.to_numeric(ledgers['opening_balance'],errors='coerce',downcast='float')
    ledgers['closing_balance']=pd.to_numeric(ledgers['closing_balance'],errors='coerce',downcast='float')
    ledgers=ledgers[['master_id', 'alter_id','primary_group','grand_parent','parent','ledger','opening_balance','closing_balance']]
    return ledgers

def get_vchtype_dataframe(xml_data):
    vchtypes=pd.DataFrame([])
    for item in xml_data.findAll('TALLYMESSAGE'):
        for name in item.findChildren('VOUCHERTYPE',recursive=False):
            for parent in item.findChildren('PARENT',recursive=True):
                for alter_id in item.findChildren('ALTERID',recursive=True):
                    vchname = name.get('NAME')
                    resname = name.get('RESERVEDNAME')
                    if resname == '':
                        type = 'UserDefined'
                    else:
                        type = 'SystemDefined'            
                    vchtypes=vchtypes.append(pd.DataFrame({
                        'alter_id': alter_id.text,
                        'name': vchname,
                        'parent': parent.text,
                        'type': type,
                        },index=[0]),ignore_index=True)
    vchtypes['alter_id']=pd.to_numeric(vchtypes['alter_id'],errors='coerce')
    return vchtypes

def get_voucher_entries(data, vouchers = pd.DataFrame([])):
    for item in data.findAll('VOUCHER'):
        for vchkey in item.findChildren('VOUCHERKEY',recursive=False):
            for vchdate in item.findChildren('DATE',recursive=False):
                for vchtype in item.findChildren('VOUCHERTYPENAME',recursive=False):
                    for narration in item.findChildren('NARRATION',recursive=False):
                        if item.findChildren('FLDVCHCREATIONDATE.LIST',recursive = False):
                                for creation_date in item.findChildren('FLDVCHCREATIONDATE.LIST',recursive=False):
                                    for create_date in creation_date.findChildren('FLDVCHCREATIONDATE',recursive=False):
                                        create_date = create_date.text
                                        for create_time in item.findChildren('FLDVCHCREATIONTIME.LIST',recursive = False):
                                            create_time = create_time.text                    
                        else:
                            create_date = "19000101"
                            create_time = "00:00"
                        
                        if item.findChildren('FLDVCHALTERATIONDATE.LIST',recursive = False):
                                for altered_date in item.findChildren('FLDVCHALTERATIONDATE.LIST',recursive=False):
                                    for alter_date in altered_date.findChildren('FLDVCHALTERATIONDATE',recursive=False):
                                        alter_date = alter_date.text
                                        for alter_time in item.findChildren('FLDVCHALTERATIONTIME.LIST',recursive = False):
                                            alter_time = alter_time.text                    
                        else:
                            alter_date = "19000101"
                            alter_time = "00:00"

                        for view in item.findChildren('PERSISTEDVIEW',recursive=False):
                            if item.findChildren('VOUCHERNUMBER',recursive=False):
                                for vchno in item.findChildren('VOUCHERNUMBER',recursive=False):
                                    vchno = vchno.text
                            else:
                                vchno = "NA"    

                            for masterid in item.findChildren('MASTERID',recursive=False):                                    
                                for alterid in item.findChildren('ALTERID',recursive=False):
                                    vouchers=vouchers.append(pd.DataFrame({
                                    'master_id' : masterid.text,
                                    'alter_id' : alterid.text,
                                    'voucher_key' : vchkey.text,
                                    'voucher_number':vchno,
                                    'date': vchdate.text,
                                    'voucher_type': vchtype.text,
                                    'voucher_view': view.text,
                                    'create_date': create_date,
                                    'create_time': create_time.strip('\n'),
                                    'alter_date' : alter_date,
                                    'alter_time' : alter_time.strip('\n'),
                                    'narrations': narration.text},
                                    index=[0]),ignore_index=True)
    try:
        vouchers.master_id = pd.to_numeric(vouchers.master_id, errors='coerce',downcast='integer')
        vouchers.alter_id = pd.to_numeric(vouchers.alter_id, errors='coerce',downcast='integer')
        vouchers.date=pd.to_datetime(vouchers.date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        vouchers.create_date=pd.to_datetime(vouchers.create_date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        vouchers.alter_date=pd.to_datetime(vouchers.alter_date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        vouchers.create_time=pd.to_datetime(vouchers.create_time,format="%H:%M").dt.time
        vouchers.alter_time=pd.to_datetime(vouchers.alter_time,format="%H:%M").dt.time
        return vouchers
    except:
        pass

def get_ledger_entries(data, ledgerdetails = pd.DataFrame([])):
    for item in data.findAll('VOUCHER'):
        for vchkey in item.findChildren('VOUCHERKEY',recursive=False):
            for masterid in item.findChildren('MASTERID',recursive=False):
                for alterid in item.findChildren('ALTERID',recursive=False):
                    for vchdate in item.findChildren('DATE',recursive=False):
                        if item.findChildren('VOUCHERNUMBER',recursive=False):
                            for vchno in item.findChildren('VOUCHERNUMBER',recursive=False):
                                vchno = vchno.text
                        else:
                            vchno = "NA"    
                        for ledger_entries in item.findAll('ALLLEDGERENTRIES.LIST'):
                            for ledger in ledger_entries.findChildren('LEDGERNAME',recursive=False):
                                for amount in ledger_entries.findChildren('AMOUNT',recursive=False):
                                    ledgerdetails=ledgerdetails.append(pd.DataFrame({
                                        'master_id' : masterid.text,
                                        'alter_id' : alterid.text,
                                        'voucher_key': vchkey.text,
                                        'voucher_number' : vchno,
                                        'voucher_date' : vchdate.text,
                                        'ledger': ledger.text,
                                        'amount': amount.text
                                        },index=[0]),ignore_index=True)
    try:
        ledgerdetails.master_id = pd.to_numeric(ledgerdetails.master_id, errors='coerce',downcast='integer')
        ledgerdetails.alter_id = pd.to_numeric(ledgerdetails.alter_id, errors='coerce',downcast='integer')
        ledgerdetails.amount = pd.to_numeric(ledgerdetails.amount,downcast="float")
        ledgerdetails.voucher_date = pd.to_datetime(ledgerdetails.voucher_date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        return ledgerdetails
    except:
        pass
    
def get_bill_entries(data, billdetails = pd.DataFrame([])):
    for item in data.findAll('VOUCHER'):
        for vchkey in item.findChildren('VOUCHERKEY',recursive=False):
            for masterid in item.findChildren('MASTERID',recursive=False):
                for alterid in item.findChildren('ALTERID',recursive=False):
                    for vchdate in item.findChildren('DATE',recursive=False):
                        if item.findChildren('VOUCHERNUMBER',recursive=False):
                            for vchno in item.findChildren('VOUCHERNUMBER',recursive=False):
                                vchno = vchno.text
                        else:
                            vchno = "NA"    
                        for ledger_entries in item.findAll('ALLLEDGERENTRIES.LIST'):
                                for ledger in ledger_entries.findChildren('LEDGERNAME',recursive=False):
                                    for bills in ledger_entries.findChildren('BILLALLOCATIONS.LIST'):
                                        for bill_name in bills.findChildren('NAME',recursive=False):
                                                bill_name = bill_name.text
                                                for bill_type in bills.findChildren('BILLTYPE',recursive=False):
                                                    bill_type = bill_type.text
                                                    for bill_amount in bills.findChildren('AMOUNT',recursive=False):
                                                        bill_amount = bill_amount.text
                                                        billdetails=billdetails.append(pd.DataFrame({
                                                            'master_id' : masterid.text,
                                                            'alter_id' :alterid.text,
                                                            'voucher_key': vchkey.text,
                                                            'voucher_number': vchno,
                                                            'voucher_date': vchdate.text,
                                                            'ledger': ledger.text,
                                                            "bill_name": bill_name,
                                                            'bill_type': bill_type,
                                                            'bill_amount': bill_amount
                                                            },index=[0]),ignore_index=True)
    try:
        billdetails.master_id = pd.to_numeric(billdetails.master_id, errors='coerce',downcast='integer')
        billdetails.alter_id = pd.to_numeric(billdetails.alter_id, errors='coerce',downcast='integer')
        billdetails.bill_amount = pd.to_numeric(billdetails.bill_amount,downcast="float")
        billdetails.voucher_date = pd.to_datetime(billdetails.voucher_date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        return billdetails
    except:
        pass
    
def get_cost_center_entries(data, ccdetails = pd.DataFrame([])):
    for item in data.findAll('VOUCHER'):
        for vchkey in item.findChildren('VOUCHERKEY',recursive=False):
            for masterid in item.findChildren('MASTERID',recursive=False):
                for alterid in item.findChildren('ALTERID',recursive=False):
                    for vchdate in item.findChildren('DATE',recursive=False):
                        if item.findChildren('VOUCHERNUMBER',recursive=False):
                            for vchno in item.findChildren('VOUCHERNUMBER',recursive=False):
                                vchno = vchno.text
                        else:
                            vchno = "NA"                                
                        for ledger_entries in item.findAll('ALLLEDGERENTRIES.LIST'):
                                for ledger in ledger_entries.findChildren('LEDGERNAME',recursive=False):
                                    for cc in ledger_entries.findChildren('CATEGORYALLOCATIONS.LIST',recursive=False):
                                        for category in cc.findChildren('CATEGORY',recursive=False):
                                            category = category.text
                                            for cc_name in cc.findChildren('COSTCENTREALLOCATIONS.LIST',recursive=False):
                                                for cost_centre in cc_name.findChildren('NAME',recursive=False):
                                                    cost_centre = cost_centre.text
                                                    for cc_amount in cc_name.findChildren('AMOUNT',recursive=False):
                                                        cc_amount = cc_amount.text
                                                        ccdetails = ccdetails.append(pd.DataFrame({
                                                            'master_id' : masterid.text,
                                                            'alter_id' : alterid.text,
                                                            'voucher_key': vchkey.text,
                                                            'voucher_number': vchno,
                                                            'voucher_date': vchdate.text,
                                                            'ledger': ledger.text,
                                                            'cc_category': category,
                                                            'cc_name': cost_centre,
                                                            'cc_amount': cc_amount
                                                            },index=[0]),ignore_index=True)
    try:
        ccdetails.master_id = pd.to_numeric(ccdetails.master_id, errors='coerce',downcast='integer')
        ccdetails.alter_id = pd.to_numeric(ccdetails.alter_id, errors='coerce',downcast='integer')
        ccdetails.cc_amount = pd.to_numeric(ccdetails.cc_amount,downcast="float")
        ccdetails.voucher_date = pd.to_datetime(ccdetails.voucher_date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        return ccdetails
    except:
        pass        

def get_bank_entries(data, bankdetails = pd.DataFrame([])):
    for item in data.findAll('VOUCHER'):
        for vchkey in item.findChildren('VOUCHERKEY'):
            for masterid in item.findChildren('MASTERID',recursive=False):
                for alterid in item.findChildren('ALTERID',recursive=False):
                    for vchdate in item.findChildren('DATE',recursive=False):
                        if item.findChildren('VOUCHERNUMBER',recursive=False):
                            for vchno in item.findChildren('VOUCHERNUMBER',recursive=False):
                                vchno = vchno.text
                        else:
                            vchno = "NA"                                
                        for ledger_entries in item.findAll('ALLLEDGERENTRIES.LIST'):
                            for ledger in ledger_entries.findChildren('LEDGERNAME',recursive=False):
                                for bank_all in ledger_entries.findChildren('BANKALLOCATIONS.LIST'):
                                    for bank_date in bank_all.findChildren('INSTRUMENTDATE',recursive=False):
                                        for tran_type in bank_all.findChildren('TRANSACTIONTYPE',recursive=False):
                                            for bank_no in bank_all.findChildren('INSTRUMENTNUMBER',recursive=False):
                                                for bank_amount in bank_all.findChildren('AMOUNT',recursive=False):
                                                    bankdetails=bankdetails.append(pd.DataFrame({
                                                        'master_id' : masterid.text,
                                                        'alter_id' : alterid.text,
                                                        'voucher_key': vchkey.text,
                                                        'voucher_number': vchno,
                                                        'voucher_date': vchdate.text,                                                        
                                                        'ledger': ledger.text,
                                                        'bank_date': bank_date.text,
                                                        'transaction_type': tran_type.text,
                                                        'instrument_number': bank_no.text,
                                                        'instrument_amount': bank_amount.text
                                                        },index=[0]),ignore_index=True)
    try:
        bankdetails.master_id = pd.to_numeric(bankdetails.master_id, errors='coerce',downcast='integer')
        bankdetails.alter_id = pd.to_numeric(bankdetails.alter_id, errors='coerce',downcast='integer')
        bankdetails.bank_date = pd.to_datetime(bankdetails.bank_date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        bankdetails.instrument_amount = pd.to_numeric(bankdetails.instrument_amount,downcast="float")
        bankdetails.voucher_date = pd.to_datetime(bankdetails.voucher_date,format="%Y%m%d").dt.strftime("%d-%b-%Y")
        return bankdetails
    except:
        pass                    

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)

### FUNCTION TO COPY DATA FROM PANDAS TO APP DATABASE ###
def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

### FUNCTION TO UPDATE CHANGED DETAILS IN APP DATABASE ###
def alterInBulk_ledger_master(records):
    try:
        ps_connection = psycopg2.connect(user="postgres",
                                         password="karthik",
                                         host="localhost",
                                         port="5433",
                                         database="bolt_dash")
        cursor = ps_connection.cursor()

        # Update multiple records
        sql_update_query = """UPDATE dashboard_ledger_master SET alter_id = %s, category = %s, primary_group = %s,
        grand_parent = %s, parent = %s, ledger = %s, opening_balance = %s, closing_balance = %s WHERE master_id = %s AND company = %s"""
        cursor.executemany(sql_update_query, records)
        ps_connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while updating PostgreSQL table", error)

    finally:
        # closing database connection.
        if ps_connection:
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")

def alterInBulk_voucher_details(records):
    try:
        ps_connection = psycopg2.connect(user="postgres",
                                         password="karthik",
                                         host="localhost",
                                         port="5433",
                                         database="bolt_dash")
        cursor = ps_connection.cursor()

        # Update multiple records
        sql_update_query = """UPDATE dashboard_voucher_details SET alter_id = %s, voucher_key = %s,
        voucher_number = %s, date = %s, voucher_type = %s, voucher_view = %s, create_date = %s,
        create_time = %s, alter_date = %s, alter_time = %s, narrations = %s WHERE master_id = %s AND company = %s"""
        cursor.executemany(sql_update_query, records)
        ps_connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while updating PostgreSQL table- Voucher_Details", error)

    finally:
        # closing database connection.
        if ps_connection:
            cursor.close()
            ps_connection.close()
            print("PostgreSQL connection is closed")

### FUNCTION TO ADD NEW DATA/DELETE OLD DATA TO/FROM APP DATABASE FROM TALLY ###
def upload_new_data(tally_df, existing_id, engine, dbtable):
    to_add = tally_df.merge(
        existing_id, how='outer', indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
    if len(to_add) > 0:
            to_add.to_sql('dashboard_'+dbtable, engine, if_exists='append', index=False, method=psql_insert_copy)
            return to_add, "data"
    else:
        return to_add, "empty"

def delete_old_data_master_id(tally_df, existing_id, engine, dbtable, company):
    to_delete = tuple(
        tally_df.merge(
            existing_id, how='outer',indicator=True).loc[lambda x : x['_merge']=='right_only']['master_id']
        )
    if len(to_delete) > 0:
        if len(to_delete) == 1:
            to_delete = to_delete[0]
            with engine.connect() as conn:
                sql_query = """DELETE FROM dashboard_"""+dbtable+""" WHERE master_id IN (%s) AND company = %s"""
                conn.execute(sql_query,(to_delete,company,))
        else:
            with engine.connect() as conn:
                    sql_query = """DELETE FROM dashboard_"""+dbtable+""" WHERE master_id IN %s AND company = %s"""
                    conn.execute(sql_query, (AsIs(to_delete),company))
                    
def delete_old_data_alter_id(tally_df, existing_id, engine, dbtable, company):
    to_delete = tuple(
        tally_df.merge(
            existing_id, how='outer',indicator=True).loc[lambda x : x['_merge']=='right_only']['alter_id']
        )
    if len(to_delete) > 0:
        if len(to_delete) == 1:
            to_delete = to_delete[0]
            with engine.connect() as conn:
                sql_query = """DELETE FROM dashboard_"""+dbtable+""" WHERE alter_id IN (%s) AND company = %s"""
                conn.execute(sql_query,(to_delete,company,))
        else:
            with engine.connect() as conn:
                    sql_query = """DELETE FROM dashboard_"""+dbtable+""" WHERE alter_id IN %s AND company = %s"""
                    conn.execute(sql_query, (AsIs(to_delete),company))
                    
### CREATE DATABASE CONNECTION USING SQLALCHEMY ###
engine = create_engine('postgresql://postgres:karthik@localhost:5433/bolt_dash',)

### GET ACCOUNT ID SYSTEM NAME AND RUNNING COMPANIES FROM TALLY ###        
# account_id = get_account_id(tally_request(license_xml))
account_id = "karthik@test.com"

# system_name = get_system_name(tally_request(system_xml))
system_name = "karthik kuram"

company_list = get_running_companies(tally_request(company_xml))

### GET TALLY SETTINGS FROM APP DATABASE ###
postgres_tally_settings = pd.read_sql_table(
    "dashboard_tally_detail", con=engine,columns=['name','tally_begin_date','tally_port','account_id','computer_name','organization_id'],
)


### CONVERT ACCOUNT ID, COMPUTER NAME AND COMPANY NAME TO LOWER CASE ###
postgres_tally_settings['account_id'] = postgres_tally_settings['account_id'].str.lower()
postgres_tally_settings['computer_name'] = postgres_tally_settings['computer_name'].str.lower()
postgres_tally_settings['name'] = postgres_tally_settings['name'].str.lower()

postgres_tally_settings = postgres_tally_settings[postgres_tally_settings['name'].isin(company_list)]

### CHECK IF THE TALLY SETTINGS EXIST IN THE APP DATABASE ###
postgres_tally_settings = postgres_tally_settings[(
    postgres_tally_settings['computer_name'] == system_name
    ) & (postgres_tally_settings['account_id'] == account_id
         ) & (postgres_tally_settings['name'].isin(company_list))]

### CONTINUE LOGIC EXECUTION IF TALLY DETAILS MATCH WITH APP DETAILS ###

if len(postgres_tally_settings) > 0:

    ### GET ALL THE TABLES FROM APP DATABASE FOR ANALYSIS ###
    postgres_ledger_category = pd.read_sql_table(
        "dashboard_ledger_category", con=engine,columns=['primary_group','category'],
    )
    
    postgres_ledger_master = pd.read_sql_table(
        "dashboard_ledger_master", con=engine,columns=['master_id','alter_id','company']
    )
    
    postgres_vouchertypes = pd.read_sql_table(
        "dashboard_vouchertypes", con=engine,columns=['alter_id','company']
    )
    
    postgres_voucher_details = pd.read_sql_table(
        "dashboard_voucher_details", con=engine,columns=['master_id','alter_id','company']
    )
    
    postgres_voucher_ledgers = pd.read_sql_table(
        "dashboard_voucher_ledgers", con=engine,columns=['master_id','alter_id','company']
    )
            
    postgres_voucher_bills = pd.read_sql_table(
        "dashboard_voucher_bills", con=engine,columns=['master_id','alter_id','company']
    )
    
    postgres_voucher_costcenters = pd.read_sql_table(
        "dashboard_voucher_costcenters", con=engine,columns=['master_id','alter_id','company']
    )
    
    postgres_voucher_bankdetails = pd.read_sql_table(
        "dashboard_voucher_bankdetails", con=engine,columns=['master_id','alter_id','company']
    )
                
    postgres_tally_settings['tally_begin_date'] = pd.to_datetime(postgres_tally_settings['tally_begin_date']).dt.strftime("%d-%b-%Y")
    
    ### LOOP THROUGH THE COMPANIES AND GET TB OF EACH COMPANY ###
    for company in company_list:
        ### Extract only the companies in the app settings
        if (company in list(postgres_tally_settings['name'])):
            ### GET LEDGER DETAILS FROM TALLY ###
            ledgers_response = tally_request(ledger_xml_request(company))
            ledger_table = get_ledgers_dataframe(ledgers_response)
            ledger_table['closing_balance'] = ledger_table['closing_balance'].fillna(0)
            ledger_table['opening_balance'] = ledger_table['opening_balance'].fillna(0)
            ledger_table = ledger_table.merge(postgres_ledger_category, how='left')
            
            ### SEPARATE DEFAULT CATEGORY AND CUSTOM CATEGORY ###
            valid_category = ledger_table[ledger_table['category'].notna()]
            custom_category = ledger_table[ledger_table['category'].isna()]
            
            ### GET CUSTOM GROUPS FROM TALLY ###
            custom_groups = pd.read_sql_query("SELECT * FROM dashboard_custom_category where company = '{}'".format(company),engine)
            custom_groups = pd.DataFrame(custom_groups, columns = ['custom_group','primary_group'])
            
            ### GET CUSTOM GROUPS RECONCILED BY USERS AND MERGE WITH LEDGER DATA ###
            valid_custom_groups = custom_groups[custom_groups['primary_group'].notnull()]
            valid_custom_groups = valid_custom_groups.merge(postgres_ledger_category,how='left')
            valid_custom_groups.columns = ['primary_group','custom_group','category']
            custom_category.drop('category',axis=1,inplace=True)
            custom_category = custom_category.merge(valid_custom_groups,how='left')
            
            ### SEPERATE LEFT OUT CATEGORIES AND WRITE ONLY NEW ENTRIES TO DATABASE ###
            left_out_category = custom_category[custom_category['category'].isna()][['primary_group']].drop_duplicates()
            left_out_category['custom_group'] = ''
            left_out_category['company'] = company
            left_out_category['organization_id'] = int(postgres_tally_settings['organization_id'])
            left_out_category.columns = ['custom_group','primary_group','company','organization_id']
            left_out_category = left_out_category[~left_out_category.custom_group.isin(custom_groups.custom_group)]
        
            left_out_category.to_sql('dashboard_'+'custom_category', engine, if_exists='append', index=False, method=psql_insert_copy)
            # with engine.connect() as conn:
            #     conn.execute('ALTER TABLE dashboard_custom_category ADD PRIMARY KEY (index);')
        
            ### COMBINE VALID AND CUSTOM LEDGER DETAILS ###        
            # custom_category = custom_category[custom_category['category'].notna()]
            ledger_table = valid_category.append(custom_category,ignore_index=True)
            ledger_table['company'] = company
            ledger_table.drop(['custom_group'],axis=1,inplace=True)
            ledger_table['master_id'] = ledger_table['master_id'].apply(np.int64)
            ledger_table['alter_id'] = ledger_table['alter_id'].apply(np.int64)
                                    
            ### GET VOUCHERTYPE DETAILS FROM TALLY ###
            vchtypes_response = tally_request(vchtype_xml_request(company))
            vchtype_table = get_vchtype_dataframe(vchtypes_response)
            vchtype_table['company'] = company
            
            ### GET DAY BOOK DETAILS FROM TALLY ###
            voucher_entries = []
            ledgeraccount_entries = []
            billallocations_entries = []
            cost_center_entries = []
            bankallocations_entries = []
            inventory_entries = []
                    
            start = datetime.strptime(postgres_tally_settings[postgres_tally_settings['name'] == company]['tally_begin_date'].tolist()[0],"%d-%b-%Y").date()
            end = pd.to_datetime("2022-3-31")
            # end = pd.to_datetime("today").date()
            dur=relativedelta.relativedelta(end,start)
            dur=dur.months+ (dur.years*12) + 1
            from_date = start
            for _ in range(dur):
                end_date=(last_day_of_month(from_date)+timedelta(days=1)).strftime('%d-%b-%Y')
                
                ### GET THE XML RESPONSE FROM TALLY ###
                data = tally_request(daybook_xml_request(company, from_date, end_date))
                
                voucher_details = get_voucher_entries(data)
                voucher_entries.append(voucher_details)
                
                ledger_details = get_ledger_entries(data)
                ledgeraccount_entries.append(ledger_details)
                
                bill_details = get_bill_entries(data)
                billallocations_entries.append(bill_details)
                
                cost_center_details = get_cost_center_entries(data)
                cost_center_entries.append(cost_center_details)
                
                bank_details = get_bank_entries(data)
                bankallocations_entries.append(bank_details)
                
                from_date=last_day_of_month(from_date)+timedelta(days=2)
                
            voucher_entries = pd.concat(voucher_entries,ignore_index=True)
            voucher_entries['company'] = company
            
            ledgeraccount_entries = pd.concat(ledgeraccount_entries,ignore_index=True)
            ledgeraccount_entries = ledgeraccount_entries.merge(ledger_table,how='left',on = 'ledger')
            ledgeraccount_entries['company'] = company
            ledgeraccount_entries.columns = ['master_id','alter_id','voucher_key','voucher_number','voucher_date',
                                            'ledger','amount','ledger_master_id','ledger_alter_id','ledger_primary_group',
                                            'ledger_grand_parent','ledger_parent','opening_balance','closing_balance',
                                            'ledger_category','company']
            ledgeraccount_entries = ledgeraccount_entries[['master_id','alter_id','voucher_key','voucher_number','voucher_date',
                                            'ledger','amount','ledger_master_id','ledger_alter_id','ledger_category','ledger_primary_group',
                                            'ledger_grand_parent','ledger_parent','company']]
            
            # ledgeraccount_entries['ledger_master_id'] = ledgeraccount_entries['ledger_master_id'].apply(np.int64)
            # ledgeraccount_entries['ledger_alter_id'] = ledgeraccount_entries['ledger_alter_id'].apply(np.int64)
            try:
                billallocations_entries = pd.concat(billallocations_entries,ignore_index=True)
                billallocations_entries = billallocations_entries.merge(ledger_table,how='left',on = 'ledger')
                billallocations_entries['company'] = company
                billallocations_entries.columns = ['master_id','alter_id','voucher_key','voucher_number','voucher_date','ledger',
                                                'bill_name','bill_type','bill_amount','ledger_master_id','ledger_alter_id','ledger_primary_group',
                                                'ledger_grand_parent','ledger_parent','opening_balance','closing_balance',
                                                'ledger_category','company']
                billallocations_entries = billallocations_entries[['master_id','alter_id','voucher_key', 'voucher_number','voucher_date',
                                                                'ledger','ledger_master_id','ledger_alter_id','ledger_category','ledger_primary_group',
                                                                'ledger_grand_parent','ledger_parent','bill_name','bill_type','bill_amount','company']]
            except:
                pass    
            
            try:
                cost_center_entries = pd.concat(cost_center_entries,ignore_index=True)
                cost_center_entries = cost_center_entries.merge(ledger_table,how='left',on = 'ledger')
                cost_center_entries['company'] = company
                cost_center_entries.columns = ['master_id','alter_id','voucher_key','voucher_number','voucher_date','ledger','cc_category','cc_name','cc_amount',
                                            'ledger_master_id','ledger_alter_id','ledger_primary_group',
                                                'ledger_grand_parent','ledger_parent','opening_balance','closing_balance',
                                                'ledger_category','company']
                cost_center_entries = cost_center_entries[['master_id','alter_id','voucher_key', 'voucher_number','voucher_date',
                                                                'ledger','ledger_master_id','ledger_alter_id','ledger_category','ledger_primary_group',
                                                                'ledger_grand_parent','ledger_parent','cc_category','cc_name','cc_amount','company']]
            except:
                pass
            
            try:
                bankallocations_entries = pd.concat(bankallocations_entries,ignore_index=True)
                bankallocations_entries = bankallocations_entries.merge(ledger_table,how='left',on = 'ledger')
                bankallocations_entries['company'] = company
                bankallocations_entries.columns = ['master_id','alter_id','voucher_key','voucher_number','voucher_date','ledger','bank_date','transaction_type','instrument_number', 'instrument_amount',
                                            'ledger_master_id','ledger_alter_id','ledger_primary_group',
                                                'ledger_grand_parent','ledger_parent','opening_balance','closing_balance',
                                                'ledger_category','company']
                bankallocations_entries = bankallocations_entries[['master_id','alter_id','voucher_key', 'voucher_number','voucher_date',
                                                                'ledger','ledger_master_id','ledger_alter_id','ledger_category','ledger_primary_group',
                                                                'ledger_grand_parent','ledger_parent','bank_date','transaction_type','instrument_number', 'instrument_amount', 'company']]
            except:
                pass    
            ################################ UPDATION OF DATA IN APP DATABASE ################################
            
            ### GET ALL THE MASTER_ID & ALTER_ID OF LEDGERS PRESENT IN APP DATABASE FOR THE COMPANY ###
            ledgers_app = postgres_ledger_master[postgres_ledger_master['company'] == company]
            existing_master_id_ledgers_app = ledgers_app[['master_id']]
            existing_alter_id_ledgers_app = ledgers_app[['alter_id']]
            
            ### ADD NEW LEDGERS TO APP DATABASE ###
            master_id_to_add_ledgers, add_tracker = upload_new_data(ledger_table, existing_master_id_ledgers_app, engine, 'ledger_master')
            
            ### DELETE OLD LEDGERS FROM APP DATABASE ###
            delete_old_data_master_id(ledger_table, existing_master_id_ledgers_app, engine, 'ledger_master', company)
            
            ### GET ALL MASTER_ID OF LEDGERS TO BE UPDATED IN APP DATABASE ###
            master_id_to_update_ledgers = ledger_table.merge(existing_alter_id_ledgers_app, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)

            ### GET UNIQUE MASTER_ID OF LEDGERS TO BE UPDATED IN APP DATABASE EXCLUDING THE NEW MASTER_ID NOW CREATED ###
            if add_tracker != "empty":
                master_id_to_update_ledgers = master_id_to_update_ledgers.merge(master_id_to_add_ledgers, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
            master_id_to_update_ledgers = master_id_to_update_ledgers[['alter_id','category','primary_group','grand_parent','parent','ledger','opening_balance','closing_balance','master_id','company']]
            update_records = master_id_to_update_ledgers.to_records(index=False).tolist()
            if len(update_records) > 0:
                alterInBulk_ledger_master(update_records)
            
            ### GET ALL THE ALTER_ID OF VOUCHER TYPES PRESENT IN APP DATABASE FOR THE COMPANY ###
            vchtypes_app = postgres_vouchertypes[postgres_vouchertypes['company'] == company]
            existing_alter_id_vchtypes_app = vchtypes_app[['alter_id']]

            ### GET ALL NEW ALTER_ID OF VOUCHER TYPES TO BE ADDED IN APP DATABASE ###
            alter_id_to_add_vchtypes, _ = upload_new_data(vchtype_table, existing_alter_id_vchtypes_app, engine, "vouchertypes")
            
            ### GET ALL ALTER_ID OF VOUCHER TYPES TO BE DELETED IN APP DATABASE ###
            delete_old_data_alter_id(vchtype_table, existing_alter_id_vchtypes_app, engine, "vouchertypes", company)
            
            ### GET ALL THE MASTER_ID & ALTER_ID OF VOUCHERS PRESENT IN APP DATABASE FOR THE COMPANY ###
            vouchers_data = postgres_voucher_details[postgres_voucher_details['company'] == company]
            existing_master_id_vouchers_app = vouchers_data[['master_id']]
            existing_alter_id_vouchers_app = vouchers_data[['alter_id']]

            ### GET ALL NEW MASTER_ID OF VOUCHERS TO BE ADDED IN APP DATABASE ###
            master_id_to_add_vouchers, voucher_tracker = upload_new_data(voucher_entries, existing_master_id_vouchers_app, engine, "voucher_details")
            
            ### GET ALL MASTER_ID OF VOUCHERS TO BE DELETED IN APP DATABASE ###
            delete_old_data_master_id(voucher_entries, existing_master_id_vouchers_app, engine, "voucher_details", company)
            
            ### GET ALL MASTER_ID OF VOUCHERS TO BE UPDATED IN APP DATABASE ###
            master_id_to_update_voucher_details = voucher_entries.merge(existing_alter_id_vouchers_app, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
            
            ### GET UNIQUE MASTER_ID OF VOUCHERS TO BE UPDATED IN APP DATABASE EXCLUDING THE NEW MASTER_ID NOW CREATED ###
            if voucher_tracker != "empty":
                master_id_to_update_voucher_details = master_id_to_update_voucher_details.merge(master_id_to_add_vouchers, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
            master_id_to_update_voucher_details = master_id_to_update_voucher_details[['alter_id','voucher_key','voucher_number','date','voucher_type','voucher_view','create_date',
                                                    'create_time','alter_date','alter_time','narrations','master_id','company']]
            update_records = master_id_to_update_voucher_details.to_records(index=False).tolist()
            if len(update_records) > 0:
                alterInBulk_voucher_details(update_records)
            
            ### GET ALL THE MASTER_ID & ALTER_ID OF lEDGER ACCOUNTS PRESENT IN APP DATABASE FOR THE COMPANY ###
            voucher_ledger_data = postgres_voucher_ledgers[postgres_voucher_ledgers['company'] == company]
            existing_master_id_ledger_accounts = voucher_ledger_data[['master_id']]
            
            existing_alter_id_ledger_accounts = voucher_ledger_data[['alter_id']]
            
            ### GET ALL NEW MASTER_ID OF LEDGER ACCOUNTS TO BE ADDED IN APP DATABASE FOR THE COMPANY ###
            master_id_to_add_ledger_accounts, account_tracker = upload_new_data(ledgeraccount_entries, existing_master_id_ledger_accounts, engine, "voucher_ledgers")
            
            ### GET ALL MASTER_ID OF LEDGER ACCOUNTS TO BE DELETED IN APP DATABASE ###
            delete_old_data_master_id(ledgeraccount_entries, existing_master_id_ledger_accounts, engine, "voucher_ledgers", company)            
            
            # ### GET ALL MASTER_ID OF LEDGER ACCOUNTS TO BE UPDATED IN APP DATABASE ###
            master_id_to_update_ledger_accounts = ledgeraccount_entries.merge(existing_alter_id_ledger_accounts, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
            
            ### GET UNIQUE MASTER_ID OF LEDGER ACCOUNTS TO BE UPDATED IN APP DATABASE EXCLUDING THE NEW MASTER_ID NOW CREATED ###
            if account_tracker != "empty":            
                master_id_to_update_ledger_accounts = master_id_to_update_ledger_accounts.merge(master_id_to_add_ledger_accounts, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
            master_id_to_update_ledger_accounts = master_id_to_update_ledger_accounts.astype({'master_id': int, 'alter_id':int,'ledger_master_id': int, 'ledger_alter_id': int})
            
            ### CREATE NEW LEDGER ACCOUNT DETAILS IN APP DATABASE ###
            if len(master_id_to_update_ledger_accounts) > 0:
                master_id_to_update_ledger_accounts.to_sql('dashboard_voucher_ledgers', engine, if_exists='append', index=False, method=psql_insert_copy)
            
            ### GET ALL ALTER_ID OF LEDGER ACCOUNTS TO BE DELETED IN APP DATABASE WHICH HAVE BEEN UPDATED ###
            delete_old_data_alter_id(ledgeraccount_entries, existing_alter_id_ledger_accounts, engine, "voucher_ledgers", company)
            
            ### GET ALL THE MASTER_ID & ALTER_ID OF BILLS PRESENT IN APP DATABASE FOR THE COMPANY ###
            voucher_bills_data = postgres_voucher_bills[postgres_voucher_bills['company'] == company]
            existing_master_id_bills = voucher_bills_data[['master_id']]
            existing_alter_id_bills = voucher_bills_data[['alter_id']]
            
            try:
                ### GET ALL NEW MASTER_ID OF BILLS TO BE ADDED IN APP DATABASE ###
                master_id_to_add_bills, bills_tracker = upload_new_data(billallocations_entries, existing_master_id_bills, engine, "voucher_bills")
                
                ### GET ALL MASTER_ID OF BILLS TO BE DELETED IN APP DATABASE ###
                delete_old_data_master_id(billallocations_entries, existing_master_id_bills, engine, "voucher_bills", company)
                
                # ### GET ALL MASTER_ID OF BILLS TO BE UPDATED IN APP DATABASE ###
                master_id_to_update_bills = billallocations_entries.merge(existing_alter_id_bills, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
                
                ### GET UNIQUE MASTER_ID OF BILLS TO BE UPDATED IN APP DATABASE EXCLUDING THE NEW MASTER_ID NOW CREATED ###
                if bills_tracker != "empty":            
                    master_id_to_update_bills = master_id_to_update_bills.merge(master_id_to_add_bills, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
                master_id_to_update_bills = master_id_to_update_bills.astype({'master_id': int, 'alter_id':int,'ledger_master_id': int, 'ledger_alter_id': int})
                
                ### CREATE NEW BILLS DETAILS IN APP DATABASE ###
                if len(master_id_to_update_bills) > 0:
                    master_id_to_update_bills.to_sql('dashboard_voucher_bills', engine, if_exists='append', index=False, method=psql_insert_copy)
                
                ### GET ALL ALTER_ID OF BILLS TO BE DELETED IN APP DATABASE WHICH HAVE BEEN UPDATED ###
                delete_old_data_alter_id(billallocations_entries, existing_alter_id_bills, engine, "voucher_bills",company)
            except:
                pass
            
            ### GET ALL THE MASTER_ID & ALTER_ID OF COST CENTERS PRESENT IN APP DATABASE FOR THE COMPANY ###
            voucher_costcenters_data = postgres_voucher_costcenters[postgres_voucher_costcenters['company'] == company]
            existing_master_id_costcenters = voucher_costcenters_data[['master_id']]
            existing_alter_id_costcenters = voucher_costcenters_data[['alter_id']]
            
            try:
                ### GET ALL NEW MASTER_ID OF COST CENTERS TO BE ADDED IN APP DATABASE ###
                master_id_to_add_costcenters, costcenters_tracker = upload_new_data(cost_center_entries, existing_master_id_costcenters, engine, "voucher_costcenters")
                
                ### GET ALL MASTER_ID OF COST CENTERS TO BE DELETED IN APP DATABASE ###
                delete_old_data_master_id(cost_center_entries, existing_master_id_costcenters, engine, "voucher_costcenters", company)
                
                # ### GET ALL MASTER_ID OF COST CENTERS TO BE UPDATED IN APP DATABASE ###
                master_id_to_update_costcenters = cost_center_entries.merge(existing_alter_id_costcenters, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
                
                ### GET UNIQUE MASTER_ID OF COST CENTERS TO BE UPDATED IN APP DATABASE EXCLUDING THE NEW MASTER_ID NOW CREATED ###
                if costcenters_tracker != "empty":            
                    master_id_to_update_costcenters = master_id_to_update_costcenters.merge(master_id_to_add_costcenters, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
                master_id_to_update_costcenters = master_id_to_update_costcenters.astype({'master_id': int, 'alter_id':int,'ledger_master_id': int, 'ledger_alter_id': int})
                
                ### CREATE NEW COST CENTERS DETAILS IN APP DATABASE ###
                if len(master_id_to_update_costcenters) > 0:
                    master_id_to_update_costcenters.to_sql('dashboard_voucher_costcenters', engine, if_exists='append', index=False, method=psql_insert_copy)
                
                ### GET ALL ALTER_ID OF COST CENTERS TO BE DELETED IN APP DATABASE WHICH HAVE BEEN UPDATED ###
                delete_old_data_alter_id(cost_center_entries, existing_alter_id_costcenters, engine, "voucher_costcenters", company)
            except:
                pass    
            ### GET ALL THE MASTER_ID & ALTER_ID OF BANK ALLOCATIONS PRESENT IN APP DATABASE FOR THE COMPANY ###
            voucher_bankdetails_data = postgres_voucher_bankdetails[postgres_voucher_bankdetails['company'] == company]
            existing_master_id_bankdetails = voucher_bankdetails_data[['master_id']]
            existing_alter_id_bankdetails = voucher_bankdetails_data[['alter_id']]
            
            try:
                ### GET ALL NEW MASTER_ID OF BANK ALLOCATIONS TO BE ADDED IN APP DATABASE ###
                master_id_to_add_bankdetails, bankdetails_tracker = upload_new_data(bankallocations_entries, existing_master_id_bankdetails, engine, "voucher_bankdetails")
                
                ### GET ALL MASTER_ID OF BANK ALLOCATIONS TO BE DELETED IN APP DATABASE ###
                delete_old_data_master_id(bankallocations_entries, existing_master_id_bankdetails, engine, "voucher_bankdetails", company)
                
                # ### GET ALL MASTER_ID OF BANK ALLOCATIONS TO BE UPDATED IN APP DATABASE ###
                master_id_to_update_bankdetails = bankallocations_entries.merge(existing_alter_id_bankdetails, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
                
                ### GET UNIQUE MASTER_ID OF BANK ALLOCATIONS TO BE UPDATED IN APP DATABASE EXCLUDING THE NEW MASTER_ID NOW CREATED ###
                if bankdetails_tracker != "empty":            
                    master_id_to_update_bankdetails = master_id_to_update_bankdetails.merge(master_id_to_add_bankdetails, how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only'].drop(['_merge'],axis=1)
                master_id_to_update_bankdetails = master_id_to_update_bankdetails.astype({'master_id': int, 'alter_id':int,'ledger_master_id': int, 'ledger_alter_id': int})
                
                ### CREATE NEW BANK ALLOCATIONS DETAILS IN APP DATABASE ###
                if len(master_id_to_update_bankdetails) > 0:
                    master_id_to_update_bankdetails.to_sql('dashboard_voucher_bankdetails', engine, if_exists='append', index=False, method=psql_insert_copy)
                
                ### GET ALL ALTER_ID OF BANK ALLOCATIONS TO BE DELETED IN APP DATABASE WHICH HAVE BEEN UPDATED ###
                delete_old_data_alter_id(bankallocations_entries, existing_alter_id_bankdetails, engine, "voucher_bankdetails", company)
            except:
                pass    