# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 16:26:32 2024

@author: Tim.Sowinski
"""
import sys
from sys import version
import pyodbc
import pandas as pd
import xlwings as xw
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import time
import string

"""This will be a script to pull all the data for the FMR and do all the analysis"""

"""==========================================================================================================
SETTINGS
============================================================================================================="""

Excel_workbook_name = "FMR Analysis test.xlsx" 

date_from: str = "2024-09-01" # date to begin analysis
date_to: str = "2024-10-01" # date to end analysis

# set these to true if you want the analysis to be done

BM: bool = False
EAC: bool = False
BR: bool = True
STOR: bool = True
SFFR: bool = True
kW_revenue = True # needs to be its own section to deal with inconsistencies in the unit capacity dataset

# Set Load = True if you want the data to be exported to the above Excel file
Load = False

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.reset_option('display.max_rows')

start_time = datetime.now()

def Data_load(data: str, date_from: str = False, date_to: str = False, BMUID_NGUID_dict = False, 
              NGUID_BMUID_dict = False, BMUID_fuel_type_dict = False, NGUID_fuel_type_dict = False, 
              BMU_company_dict = False, NGU_company_dict = False):
     
    class SQL_query: # creates SQL query class
    
        server = "tcp:mssql-01.citg.one"
        database = "CI_DL1"
        connection_string = "DRIVER={SQL Server};SERVER="+server+";DATABASE="+database+";Trusted_Connection=yes"
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor 
    
        def __init__(self):
            print("Init")
            
        def BMU_data(connection = connection):
            query_string = f"""
            SELECT Elexon_BMUnitID, NGC_BMUnitID, PartyName, GSPGroup, ReportName, BMU.FuelTypeID
            
            FROM Meta.tblBMUnit_Managed as BMU
    
            INNER JOIN Meta.tblFuelType as ft ON ft.FuelTypeID = BMU.FuelTypeID
            """
            print("Gathering asset information data from SQL server")
            df = pd.read_sql_query(query_string, connection)
            
            column_renames = {"Elexon_BMUnitID": "BMU ID", "NGC_BMUnitID": "NGU ID", "PartyName": "Company",
                              "GSPGroup": "GSP Group", "ReportName": "Fuel type", "FuelTypeID": "Fuel type ID"}
            df = df.rename(columns = column_renames)
            
            return df
        
        def Capacity_data(connection = connection):
            query_string = f"""
            SELECT *
    
            FROM PowerSystem.tblBMUnitGCDC as Capacity
    
            INNER JOIN Meta.tblBMUnit_Managed as BMU on BMU.BMUnitID = Capacity.BMUnitID
            
            """
            print("Gathering BMU capacity data from SQL server")
            
            
            df = pd.read_sql_query(query_string, connection)
            
            
            # don't use Company as a column in here, as it doesn't come from the BMUManaged table
            column_renames = {"Elexon_BMUnitID": "BMU ID", "Runtime": "Date", "GC": "GC",
                              "DC": "DC", "NGC_BMUnitID": "NGU ID"}
            df = df.rename(columns = column_renames)
            df = df[column_renames.values()]
            return df
        
        def BOD_data(date_from: str, date_to: str, connection = connection):
            query_string = f"""SELECT *
            FROM [PowerSystem].[tblBidOfferData] as BOD
            
            INNER JOIN [Meta].[tblBMUnit_Managed] as BMU
            ON BMU.BMUnitID = BOD.BMUnitID
    
            INNER JOIN [Meta].[tblFuelType] as ft
            ON BMU.FuelTypeID = ft.FuelTypeID
    
            WHERE
            [TimeFromUTC] > '{date_from}' and [TimeFromUTC] < '{date_to}'
            """
    
            print("Gathering submitted bid/offer data from SQL server")
            df = pd.read_sql_query(query_string, connection)
            #print("hello")
            
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP","TimeFromUTC": "Time from",
                              "TimeToUTC": "Time to", "Elexon_BMUnitID": "BMU ID", "NGC_BMUnitID": "NGU ID",
                              "ReportName": "Fuel type", "PartyName": "Company","LevelFrom": "MW from", 
                              "LevelTo": "MW to", "PairId": "Pair ID", "Bid": "Bid price", "Offer": "Offer price"}
            df.rename(columns = column_renames, inplace = True)
            print(df)
            
            df = df[column_renames.values()]
            
            return df
        
        def BOA_data(date_from: str, date_to: str, connection = connection):
            pass
        
        
        def DSP_data(date_from: str, date_to: str, connection = connection):
            
            query_string = f"""SELECT TOP 20 *
            FROM PowerSystem.tblDetailedSystemPrices as DSP
            
            WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'
            
            """
            
            print("Gathering submitted Detailed System Prices data from SQL server")
            df = pd.read_sql_query(query_string, connection)
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "ID": "BMU ID", "BidOfferPairId": "Pair ID",
                              "CadlFlag": "CADL Flag", "SoFlag": "SO Flag", "StorFlag": "STOR Flag", 
                              "Price": "Price (£/MWh)", "Volume": "Volume (MWh)"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values(by = ["Date", "SP"]).reset_index(drop = True)
            
            return df
        
        def DISBSAD_data(date_from: str, date_to: str, connection = connection):
            query_string = f"""SELECT *
            FROM PowerSystem.tblBalancingServicesAdjustment
            
            WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'
            
            """
            
            print("Gathering DISBSAD data from SQL server")
            df = pd.read_sql_query(query_string, connection)
            
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "ID": "ID", "Elexon_AssetID": "NGU ID",
                              "SoFlag": "SO Flag", "BsaaSTORProviderFlag": "STOR Flag", "Elexon_PartyID": "Company ID",
                              "Price": "Price (£/MWh)", "Volume": "Volume (MWh)", "Cost": "Cost (£)", "TenderedStatus": "Tendered Status", 
                              "ServiceType": "Service type", "StartTime": "Start time"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            df["Date"] == pd.to_datetime(df["Date"])
            df = df.sort_values(by = ["Date", "SP"]).reset_index(drop = True)
            
            return df
            
        
        def EAC_data(date_from: str, date_to: str, connection = connection):
            query_string = f"""SELECT Unit_NGESOID, BasketID, ServiceType, DeliveryStartDate, DeliveryEndDate, OrderType, AuctionProduct, Volume, 
            PriceLimit, LoopedBasketID, ExecutedVolume, ClearingPrice, NGU.CompanyName
            
            FROM PowerSystem.tblEACAuctionResultsSell as EAC
            
            INNER JOIN Meta.tblNGTUnit_Managed as NGU on NGU.NGESO_NGTUnitID = EAC.Unit_NGESOID"""
            
            print("Gathering EAC data from SQL server...")
            df = pd.read_sql_query(query_string, connection)
            column_renames = {"Unit_NGESOID": "NGU ID", "BasketID": "Basket ID", "ServiceType": "Service type",
                              "DeliveryStartDate": "Start time", "DeliveryEndDate": "End time", "OrderType": "Order type",
                              "AuctionProduct": "Service", "Volume": "Volume (MW)", "PriceLimit": "Submitted price (£/MW/hr)",
                              "LoopedBasketID": "Looped Basket ID", "ExecutedVolume": "Executed Volume (MW)",
                              "ClearingPrice": "Clearing price (£/MW/hr)", "CompanyName": "Company"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            return df
        
        def STOR_data(self, date_from: str, date_to: str):
            pass
        
        def SFFR_data(self, date_from: str, date_to: str):
            pass
    
    def load(date_from, date_to, csv_file_name, date_col_name):
        # date_col_name is the name of the datetime column in the dataset (it's used to find the max date)
        if csv_file_name not in [i for i in os.listdir() if i.endswith(".csv")]:
            # if csv file not in directory, loads from SQL server
            df = getattr(SQL_query, data)(date_from = date_from, date_to = date_to) # gets the SQL data using the correct method
            export = True
        else:
            export = False
            print(f"Loading data from {csv_file_name}...")
            df = pd.read_csv(csv_file_name)
            df[date_col_name] = pd.to_datetime(df[date_col_name])
            
            max_pre_loaded_date = df[date_col_name].max() # finds max date in the dataset
            max_pre_loaded_date_str = datetime.strftime(max_pre_loaded_date, "%Y-%m-%d")
            
            # if the max date in the csv is less than user input date_to, pulls the remaining data off the server
            if max_pre_loaded_date < datetime.strptime(date_to, "%Y-%m-%d"):
                df_temp = getattr(SQL_query, data)(max_pre_loaded_date + relativedelta(days = 1), date_to = date_to)
                df = pd.concat([df, df_temp])
                export = True
            else:
                pass
        return df, export
    
    # gets a list of all methods in the SQL class
    SQL_methods = sorted([i for i in dir(SQL_query) if i.endswith("__") == False])
    print(SQL_methods)
    if data not in SQL_methods:
        raise TypeError(f"Please ener a dataset from the following list to load: {SQL_methods}")
    else:
        pass
    
    if data == "DSP_data":
        csv_file_name = "All DSP data.csv"
        df, export = load(date_from, date_to, csv_file_name, "Date")
        df["Month"] = df["Date"].dt.strftime("%b-%y")
        df["Volume ABS"] = df["Volume (MWh)"].abs()
        df["Order type"] = "Offer"
        df["Order type"] = df["Order type"].where(df["Pair ID"] > 0, "Bid")
        df["Energy/System"] = "System"
        df["Energy/System"] = df["Energy/System"].where(df["SO Flag"] == "T", "Energy")
        df["Month start"] = pd.to_datetime(df["Month"], format = "%b-%y").dt.date
    elif data == "DISBSAD_data":
        csv_file_name = "All DISBSAD data.csv"
        df, export = load(date_from, date_to, csv_file_name, "Date")
        df["Month"] = df["Date"].dt.strftime("%b-%y")
        df["Order type"] = "Offer"
        df["Order type"] = df["Order type"].where(df["Volume (MWh)"] > 0, "Bid")
    elif data == "BMU_data":
        csv_file_name = "BMU Info.csv"
        df, export = load(date_from, date_to, csv_file_name)
        df["Company"] = df["Company"].where(df["Company"] != "EDF", "EDF Energy")
    elif data == "Capacity_data":
        csv_file_name = "BMU Capacity data.csv"
        df, export = load(date_from, date_to, csv_file_name)
        df["BMU Capacity ID"] = df["BMU ID"] + df["Date"].astype(str)
        df["NGU Capacity ID"] = df["NGU ID"] + df["Date"].astype(str)
    
    param_names = list(locals().keys())
    
    if isinstance(BMUID_NGUID_dict, dict): # if BMUID_NGUID dict has been input it will add NGU ID based on BMU ID column
        df["NGU ID"] = df["BMU ID"].map(BMUID_NGUID_dict)
    elif isinstance(NGUID_BMUID_dict, dict):
        df["BMU ID"] = df["NGU ID"].map(NGUID_BMUID_dict)
    elif isinstance(BMUID_fuel_type_dict, dict):
        df["Fuel type"] = df["BMU ID"].map(BMUID_fuel_type_dict)
    elif isinstance(NGUID_fuel_type_dict, dict):
        df["Fuel type"] = df["NGU ID"].map(NGUID_fuel_type_dict)
    elif isinstance(BMU_company_dict, dict):
        df["Company"] = df["BMU ID"].map(BMU_company_dict)
    elif isinstance(NGU_company_dict, dict):
        df["Company"] = df["NGU ID"].map(NGU_company_dict)
    
    if export == True:
        print(f"Exporting {data} to csv file as {csv_file_name}...")
        df.to_csv(csv_file_name, index = False)
    else:
        pass
    
    return df

Data_load(data = "DSP_data", date_from = date_from, date_to = date_to)


raise TypeError("I need to go through and check all the loads are working as intended (they haven't all been updated in the load function yet)")
sys.exit()
def num_to_col(num): # give it a number and it will return its corresponding column, up to column 703
    col_nums = {i + 1: j for i, j in enumerate(string.ascii_uppercase)}
    #print(col_nums)
    
    if num == 0:
        raise TypeError("number cannot equal 0")
    
    elif num < 27:
        #print(str(col_nums[num]))
        return f"{col_nums[num]}"
    
    elif num > 703:
        raise TypeError(f"Columns must not exceed 703. You have entered {num}")
    else:
        #print(num/26)
        x = int(num/26)
        r = num % 26
        
        if r == 0:
            r = 26
            x = x - 1
        else:
            pass
        
        return f"{col_nums[x]}{col_nums[r]}"  

"""==========================================================================================================
Balancing Mechanism
============================================================================================================="""

if __name__ == "__main__":
    
    def Excel_load(sheet_name, data, cell_ref, name = False, clear_range = False): #if using coordinates, needs to be entered as a list like [(row1, col1), (row2, col2)]
        """For cell_ref coordinates, must be in the format [(row, col)]. For clear range coordinates, must be in the format [(row1, col1), (row2, col2)]"""     
        current_sheets = []
        for i in range(len(workbook.sheets)):
            current_sheets.append(workbook.sheets[i].name) #this returns a new list of just the sheet names
        
        #adds in sheets if it doesn't already exist
        if sheet_name not in current_sheets:
            workbook.sheets.add("{}".format(sheet_name))
            current_sheets.append("{}".format(sheet_name))
        else:
            pass
        
        sheet = workbook.sheets["{}".format(sheet_name)]
        
        if name == False:
            name = " "
        else:
            name = name
        
        if clear_range != False:
            
            if type(clear_range) == list and type(cell_ref) == list:
                #print("A")
                sheet.range(clear_range[0], clear_range[1]).clear_contents()
                #print("Cleared")
                sheet[cell_ref[0]].value = data
                sheet[cell_ref[0]].value = name
            elif type(clear_range) == list and type(cell_ref) == str:
                #print("B")
                sheet.range(clear_range[0], clear_range[1]).clear_contents()
                sheet["{}".format(cell_ref)].value = data
                sheet["{}".format(cell_ref)].value = name
            elif type(clear_range) == str and type(cell_ref) == list:
                #print("C")
                sheet.range("{}".format(clear_range)).clear_contents()
                sheet[cell_ref[0]].value = data
                sheet[cell_ref[0]].value = name
            elif isinstance(clear_range, str) and isinstance(cell_ref, tuple):
                #print("D")
                sheet.range("{}".format(clear_range)).clear_contents()
                sheet[cell_ref].value = data
                sheet[cell_ref].value = name
            elif isinstance(clear_range, list) and isinstance(cell_ref, tuple):
                #print("E")
                sheet.range(clear_range[0], clear_range[1]).clear_contents()
                sheet[cell_ref].value = data
                sheet[cell_ref].value = name
            else:
                #print("F")
                sheet.range(clear_range).clear_contents()
                sheet[cell_ref].value = data
                sheet[cell_ref].value = name
            
        else:
            if type(cell_ref) == list:
                sheet[cell_ref[0]].value = data
                sheet[cell_ref[0]].value = name
            elif isinstance(cell_ref, tuple):
                sheet[cell_ref].value = data
                sheet[cell_ref].value = name
            elif type(cell_ref) == str:
                sheet["{}".format(cell_ref)].value = data
                sheet["{}".format(cell_ref)].value = name
            else:
                print("Loading cell reference for {} not in the required format of [(row1, col1)] or string".format(sheet_name))
    
    if Load == True:
        print(f"Opening {Excel_workbook_name} file")
        workbook = xw.Book(Excel_workbook_name)
    else:
        pass  
    
    # csv files in current directory
    csv_files = [i for i in os.listdir() if i.endswith(".csv")]
    
    # name of dataframe and their associated csv files
    csv_file_dict = {"DSP_data": "All DSP data.csv", "EAC_data": "EAC Sell Order data.csv"}
    
    """=======================================================================================================
    Datetime dates for the code
    =========================================================================================================="""
    
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d") # datetime of date_from
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") # datetime of date_to
    
    month_str = date_from_dt.strftime("%b-%y")
    
    date_from_prev_dt = date_from_dt + relativedelta(months = -1, day = 1) # previous month start
    date_to_prev_dt = date_from_dt + relativedelta(days = -1) # previous month end
    
    month_str_prev = date_from_prev_dt.strftime("%b-%y")
    
    
    """=======================================================================================================
    Creates filename to export or check for already downloaded data
    =========================================================================================================="""
    # checks if date_from is the first of a month and date_to is the end of the month
    if date_to_dt == date_from_dt + relativedelta(months = 1, days = -1):
        # suffix will go at the end of all file names
        file_name_suffix = str(datetime.strftime(date_from_dt, "%b-%y")) + ".csv"
        file_name_suffix_prev = str(datetime.strftime(date_from_prev_dt, "%b-%y")) + ".csv"
    else:
        file_name_suffix = f"{datetime.strftime(date_from_dt, '%b-%y')} to {datetime.strftime(date_to_dt, '%b-%y')}.csv"
        file_name_prev = False
    
    # loads BMU data
    if "BMU info.csv" not in csv_files:
        BMU_data = SQL_query().BMU_data()
        # there's some entries which are under EDF instead of EDF Energy for some reason so this fixes that
        BMU_data["Company"] = BMU_data["Company"].where(BMU_data["Company"] != "EDF", "EDF Energy")
        BMU_data.to_csv("BMU info.csv", index = False)
    else:
        BMU_data = pd.read_csv("BMU info.csv")
        
    if "BMU Capacity data.csv" not in csv_files:
        BMU_capacity_data = SQL_query().Capacity_data()
        BMU_capacity_data["Date"] = pd.to_datetime(BMU_capacity_data["Date"]).dt.date
        BMU_capacity_data["BMU Capacity ID"] = BMU_capacity_data["BMU ID"] + BMU_capacity_data["Date"].astype(str)
        BMU_capacity_data["NGU Capacity ID"] = BMU_capacity_data["NGU ID"] + BMU_capacity_data["Date"].astype(str)
        BMU_capacity_data.to_csv("BMU Capacity data.csv", index = False)
    else:
        path = os.getcwd() + "//BMU Capacity data.csv"
        details = os.path.getctime("BMU Capacity data.csv") # gets time created
        created_time = time.strptime(time.ctime(details)) # turns time into annoying time format
        created_time = time.strftime("%Y-%m-%d") # useful string format
        created_time = datetime.strptime(created_time, "%Y-%m-%d") # into datetime format
        
        if created_time < datetime.now() - relativedelta(days = 30): # updates capacity data if it's over 30 days old
            print("Updating capacity data")
            BMU_capacity_data = SQL_query().Capacity_data()
            BMU_capacity_data["Date"] = pd.to_datetime(BMU_capacity_data["Date"]).dt.date
            BMU_capacity_data["BMU Capacity ID"] = BMU_capacity_data["BMU ID"] + BMU_capacity_data["Date"].astype(str)
            BMU_capacity_data["NGU Capacity ID"] = BMU_capacity_data["NGU ID"] + BMU_capacity_data["Date"].astype(str)
            BMU_capacity_data.to_csv("BMU Capacity data.csv", index = False)
        else:
            BMU_capacity_data = pd.read_csv("BMU Capacity data.csv")
            BMU_capacity_data["Date"] = pd.to_datetime(BMU_capacity_data["Date"])
        
    """Could I turn these into classes?"""
    # creates dictionaries to help analysis
    BMUID_fuel_type_dict = BMU_data.set_index("BMU ID")["Fuel type"].to_dict()
    BMUID_NGUID_dict = BMU_data.set_index("BMU ID")["NGU ID"].to_dict()
    NGUID_BMUID_dict = BMU_data.set_index("NGU ID")["BMU ID"].to_dict()
    NGUID_fuel_type_dict = BMU_data.set_index("NGU ID")["Fuel type"].to_dict()
    
    
    BMU_company_dict = BMU_data.set_index("BMU ID")["Company"].to_dict()
    NGU_company_dict = BMU_data.set_index("NGU ID")["Company"].to_dict()
    
    # doesn't appear as though the capacities change through time, so I'll just use the max volume
    BMU_capacity_dict = BMU_capacity_data.groupby(by = ["BMU ID"])["GC"].max().to_dict() # capacity of BMU ID
    NGU_capacity_dict = BMU_capacity_data.groupby(by = ["NGU ID"])["GC"].max().to_dict() # capacity of NGU ID
    BMU_capacity_data["Company"] = BMU_capacity_data["BMU ID"].map(BMU_company_dict)
    company_capacity_dict = BMU_capacity_data.groupby(by = "Company")["GC"].sum().to_dict() # total capacity by company
    
    if BM == True:
        """===================================================================================================
        Loads Detailed system prices
        ======================================================================================================"""
        #file_name_DSP = "DSP data " + file_name_suffix
        file_name_DSP = "All DSP data.csv"
        # checks if data has already been loaded, if not will import it from the SQL server
        if file_name_DSP not in csv_files:
            print("\nLoading detailed system prices from SQL server...")
            BM_date_from = "2023-11-01"
            DSP_data = SQL_query().DSP_data(BM_date_from, date_to)
            DSP_data["Fuel type"] = DSP_data["BMU ID"].map(BMUID_fuel_type_dict)
            DSP_data["NGU ID"] = DSP_data["BMU ID"].map(BMUID_NGUID_dict)
            DSP_data["Company"] = DSP_data["BMU ID"].map(BMU_company_dict)
            DSP_data["Month"] = DSP_data["Date"].dt.strftime("%b-%y")
            DSP_data["Volume ABS"] = DSP_data["Volume (MWh)"].abs()
            DSP_data["Order type"] = "Offer"
            DSP_data["Order type"] = DSP_data["Order type"].where(DSP_data["Pair ID"] > 0, "Bid")
            DSP_data["Energy/System"] = "System"
            DSP_data["Energy/System"] = DSP_data["Energy/System"].where(DSP_data["SO Flag"] == "T", "Energy")
            DSP_data["Month start"] = pd.to_datetime(DSP_data["Month"], format = "%b-%y").dt.date
            print("Exporting detailed system prices to csv...")
            DSP_data.to_csv(file_name_DSP, index = False)
        else: # if data has been loaded, will load it in via csv
            print("\nLoading detailed system prices from csv file...")
            DSP_data = pd.read_csv(os.getcwd() + "//" + file_name_DSP)
            DSP_data["Date"] = pd.to_datetime(DSP_data["Date"])
            #print(DSP_data)
            
            # checks if all the data's there
            max_pre_loaded_date = DSP_data["Date"].max()
            if max_pre_loaded_date < date_to_dt:
                # loads the additional data if needed
                print("Loading additional detailed system prices from SQL server...")
                DSP_new = SQL_query().DSP_data(max_pre_loaded_date + relativedelta(days = 1), date_to)
                DSP_new["Fuel type"] = DSP_new["BMU ID"].map(BMUID_fuel_type_dict)
                DSP_new["NGU ID"] = DSP_new["BMU ID"].map(BMUID_NGUID_dict)
                DSP_new["Company"] = DSP_new["BMU ID"].map(BMU_company_dict)
                DSP_new["Month"] = DSP_new["Date"].dt.strftime("%b-%y")
                DSP_new["Volume ABS"] = DSP_new["Volume (MWh)"].abs()
                DSP_new["Order type"] = "Offer"
                DSP_new["Order type"] = DSP_new["Order type"].where(DSP_new["Pair ID"] > 0, "Bid")
                DSP_new["Energy/System"] = "System"
                DSP_new["Energy/System"] = DSP_new["Energy/System"].where(DSP_new["SO Flag"] == "T", "Energy")
                DSP_data["Month start"] = pd.to_datetime(DSP_new["Month"], format = "%b-%y").dt.date
                
                DSP_data = pd.concat([DSP_data, DSP_new]).reset_index(drop = True)
                
                DSP_data.to_csv(file_name_DSP, index = False)
            else:
                pass

        """===================================================================================================
        Begins BM Analysis
        ======================================================================================================"""
        DSP_data["Month start"] = pd.to_datetime(DSP_data["Month"], format = "%b-%y").dt.date
        BM_techs = DSP_data["Fuel type"].unique().tolist()
        BM_techs = sorted([str(i) for i in BM_techs if str(i) != 'nan']) # sorted list of technologies
        
        """Training question on number of reversed BOAs (server was down so I couldn't get BOA data) :(
        
        print(DSP_data)
        # groups by unit, SP, and date and counts the number of bids/offers utilised in each period
        df = pd.pivot_table(DSP_data, values = "NGU ID", index = ["Date", "SP", "BMU ID"], columns = "Order type", aggfunc = "count")
        df["sum"] = 0
        # finds the abs difference between offers and bids per SP
        df["sum"] = (df["Offer"].abs()-df["Bid"].abs()).where((df["Offer"].isna() == False) & (df["Bid"].isna() == False), df["sum"]).abs()
        print(len(DSP_data.index))
        print(100*df["sum"].sum()/len(DSP_data.index))
        """
        
        """Data for daily dispatch graph"""
        for i, j in enumerate(BM_techs):
            print(j)
            if i == 0:
                tech_BOAs = DSP_data[DSP_data["Fuel type"] == j]
                tech_BOAs.reset_index(drop = True, inplace = True)
                dispatch_graph = tech_BOAs.groupby("Date", as_index = False)["Fuel type"].count()
                dispatch_graph.rename(columns = {"Fuel type": j}, inplace = True)
            else:
                tech_BOAs_temp = DSP_data[DSP_data["Fuel type"] == j]
                tech_BOAs_temp.reset_index(drop = True, inplace = True)
                dispatch_graph_temp = tech_BOAs_temp.groupby("Date", as_index = False)["Fuel type"].count()
                dispatch_graph = dispatch_graph.merge(dispatch_graph_temp, on = "Date", how = "left")
                dispatch_graph.rename(columns = {"Fuel type": j}, inplace = True)
        
        dispatch_graph.set_index("Date", inplace = True)
        if Load == True:
            Excel_load("BM Daily Dispatches", dispatch_graph, "A1", "Daily dispatches", "A:G")
        else:
            pass
        
        """Volume share graph"""
        print("Loading BM volume share data...")
        BM_volume_share = pd.pivot_table(DSP_data, index = "Month", values = "Volume ABS", columns = "Fuel type", aggfunc = "sum", margins = True, margins_name = "Total volume")
        
        for i in BM_volume_share.columns.tolist()[:-1]:
            BM_volume_share[i] = BM_volume_share[i]/BM_volume_share[BM_volume_share.columns.to_list()[-1]]

        BM_volume_share.drop(BM_volume_share.columns.to_list()[-1], inplace = True, axis = 1) #removes total vol column
        BM_volume_share.drop(BM_volume_share.index.to_list()[-1], inplace = True, axis = 0) #removes total vol row
        
        
        # this stuff here is because Excel's a pain in the ass and changes the date format from mmm-yy to something stupid
        BM_volume_share["Start date"] = "01-" + BM_volume_share.index
        BM_volume_share["Start date"] = pd.to_datetime(BM_volume_share["Start date"], format = "%d-%b-%y")
        BM_volume_share.sort_values(by = "Start date", inplace = True)
        BM_volume_share.set_index("Start date", drop = True)
        #print(BM_volume_share)
        
        if Load == True:
            Excel_load("BM Volume share", BM_volume_share, "A1", "Volume share", "A:O")
        else:
            pass

        """Battery bid/offer spreads"""
        print("Calculating battery bid/offer spreads")
        battery_bids = DSP_data[(DSP_data["Fuel type"] == "Battery") & (DSP_data["Order type"] == "Bid")]
        battery_bids.reset_index(drop = True, inplace = True)
        battery_bids = pd.pivot_table(battery_bids, values = ["Price (£/MWh)", "Volume (MWh)"], index = "Date", aggfunc = {"Price (£/MWh)": "mean", "Volume (MWh)": "sum"})
        battery_bids.rename(columns = {"Price (£/MWh)": "Average bid price (£/MWh)", "Volume (MWh)": "Bid volume (MWh)"})
        #print(battery_bids)

        battery_offers = DSP_data[(DSP_data["Fuel type"] == "Battery") & (DSP_data["Order type"] == "Offer")]
        battery_offers.reset_index(drop = True, inplace = True)
        battery_offers = pd.pivot_table(battery_offers, values = ["Price (£/MWh)", "Volume (MWh)"], index = "Date", aggfunc = {"Price (£/MWh)": "mean", "Volume (MWh)": "sum"})
        battery_offers.rename(columns = {"Price (£/MWh)": "Average offer price (£/MWh)", "Volume (MWh)": "Offer volume (MWh)"})
        #print(battery_offers)
        
        if Load == True:
            col = 0
            row = 0
            Excel_load("Battery BM spreads", battery_offers, (row, col), name = "Battery offers", clear_range = [(1, 1), (len(battery_offers.index) + 1, 2*len(battery_offers.columns.tolist()) + 1)])
            col = len(battery_offers.columns.tolist()) + 2
            Excel_load("Battery BM spreads", battery_bids, (row, col), name = "Battery bids")
        else:
            pass
        
        """Monthly dispatch graph"""
        print("Calculating monthly BM dispatches")
        monthly_dispatch = DSP_data[DSP_data["Month"].isin([month_str, month_str_prev])]
        monthly_dispatch.reset_index(drop = True, inplace = True)
        
    
        total_dispatch = pd.pivot_table(monthly_dispatch, values = "Volume ABS", columns = "Month", index = "Fuel type", aggfunc = "count")
        total_dispatch.rename(columns = {month_str: f"{month_str} total dispatches", month_str_prev: f"{month_str_prev} total dispatches"}, inplace = True)
        total_dispatch["Change in total dispatches"] = (total_dispatch[f"{month_str} total dispatches"]/total_dispatch[f"{month_str_prev} total dispatches"]) - 1
        
        
        average_dispatch = pd.pivot_table(monthly_dispatch, values = "Volume ABS", columns = ["Date", "Month"], index = "Fuel type", aggfunc = "count")
        total_dispatch.rename(columns = {month_str: f"{month_str} average dispatches", month_str_prev: f"{month_str_prev} average dispatches"}, inplace = True)
        
        # average daily dispatch rates by month and previous month
        total_dispatch[f"Average daily dispatches {month_str}"] = average_dispatch[[i for i in average_dispatch.columns.tolist() if i[1] == month_str]].mean(axis = 1)
        total_dispatch[f"Average daily dispatches {month_str_prev}"] = average_dispatch[[i for i in average_dispatch.columns.tolist() if i[1] == month_str_prev]].mean(axis = 1)
        
        total_dispatch["Change in average daily dispatches"] = (total_dispatch[f"Average daily dispatches {month_str}"]/total_dispatch[f"Average daily dispatches {month_str_prev}"]) - 1
        
        if Load == True:
            Excel_load("Monthly dispatches", total_dispatch, "A1", "Monthly dispatches", "A:G")
        else:
            pass
        
        """BOA by technology breakdown"""
        print("Calculating BOA by technology breakdown")
        #print(monthly_dispatch)
        
        def BM_tech_breakdown(Type = "Volume"):
            """Use this to get the total volume, price and count separated by tech type and order type"""
            
            if Type == "Volume":
                vals = "Volume ABS"
                operation = "sum"
            elif Type == "Price":
                vals = "Price (£/MWh)"
                operation = "mean"
            elif Type == "Count":
                vals = "Volume ABS"
                operation = "count"
            else:
                raise TypeError(f"{Type} not a recognised function, please enter either Volume, Price, or Count")
            
            df = pd.DataFrame()
            df_prev = pd.DataFrame()            
            print(Type)
            for a, i in enumerate(["Energy", "System"]):
                DSP_temp = monthly_dispatch[monthly_dispatch["Energy/System"] == i].reset_index(drop = True)
                #print(DSP_temp.columns.tolist())
                for b, j in enumerate(["Offer", "Bid"]):                  
                    print(i, j)
                    DSP_temp1 = DSP_temp[DSP_temp["Order type"] == j].reset_index(drop = True)
                    DSP_temp1 = pd.pivot_table(DSP_temp1, values = vals, columns = "Month", index = "Fuel type", aggfunc = operation)
                    
                    DSP_temp1.rename(columns = {c: f"{i} {j} {Type.lower()} {c}" for c in DSP_temp1.columns.tolist()}, inplace = True)
                    
                    if (a == 0) and (b == 0): # if it's the first df, add it into the blank dataframes
                        df = DSP_temp1[f"{i} {j} {Type.lower()} {month_str}"]
                        df_prev = DSP_temp1[f"{i} {j} {Type.lower()} {month_str_prev}"]
                    else:
                        df = pd.merge(df, DSP_temp1[f"{i} {j} {Type.lower()} {month_str}"], left_index = True, right_index = True, how = "outer")
                        df_prev = pd.merge(df_prev, DSP_temp1[f"{i} {j} {Type.lower()} {month_str_prev}"], left_index = True, right_index = True, how = "outer")
            
                change_df = pd.merge(df, df_prev, left_index = True, right_index = True, how = "outer")
            
                change_df[f"Change in {i} {j} {Type.lower()}"] = (change_df[f"{i} {j} {Type.lower()} {month_str}"]/change_df[f"{i} {j} {Type.lower()} {month_str_prev}"]) - 1
                
                change_df = change_df[[c for c in change_df.columns.tolist() if "change in " in c]]
                
                print(change_df)
                print("Change_df not complete for BM tech breakdown")
                
            return df, df_prev, change_df
        
        if Load == True:
            print("Calculating BOAs by tech type...")
            row = 0
            col = 0
            
            Excel_load("BOA Technology Breakdown", BM_tech_breakdown("Volume")[0], (row, col), name = "Total volume", clear_range = "A:Q")
            row = len(BM_tech_breakdown("Volume")[0].index) + 2
            Excel_load("BOA Technology Breakdown", BM_tech_breakdown("Volume")[1], (row, col), name = "Total volume")
            
            #row = row + len(BM_tech_breakdown("Volume")[1].index) + 2
            #Excel_load("BOA Technology Breakdown", BM_tech_breakdown("Volume")[2], (row, col), name = "Change in volume")
            
            col = len(BM_tech_breakdown("Volume")[0].columns.tolist()) + 2
            Excel_load("BOA Technology Breakdown", BM_tech_breakdown("Count")[0], (0, col), name = "Total count")
            Excel_load("BOA Technology Breakdown", BM_tech_breakdown("Count")[1], (row, col), name = "Total count")
            
            col = col + len(BM_tech_breakdown("Count")[0].columns.tolist()) + 2
            Excel_load("BOA Technology Breakdown", BM_tech_breakdown("Price")[0], (0, col), name = "Average price")
            Excel_load("BOA Technology Breakdown", BM_tech_breakdown("Price")[1], (row, col), name = "Average price")
            
        else:
            pass
        
        """===================================================================================================
        Begins DISBSAD Analysis
        ======================================================================================================"""
        file_name_DISBSAD = "All DISBSAD data.csv"
        # checks if data has already been loaded, if not will import it from the SQL server
        if file_name_DISBSAD not in csv_files:
            print("\nLoading DISBSAD from SQL server...")
            BM_date_from = "2023-11-01"
            DISBSAD_data = SQL_query().DISBSAD_data(BM_date_from, date_to)
            DISBSAD_data["Fuel type"] = DISBSAD_data["NGU ID"].map(NGUID_fuel_type_dict)
            DISBSAD_data["BMU ID"] = DISBSAD_data["NGU ID"].map(NGUID_BMUID_dict)
            DISBSAD_data["Month"] = DISBSAD_data["Date"].dt.strftime("%b-%y")
            DISBSAD_data["Order type"] = "Offer"
            DISBSAD_data["Order type"] = DISBSAD_data["Order type"].where(DISBSAD_data["Volume (MWh)"] > 0, "Bid")
            print("Exporting detailed system prices to csv...")
            DISBSAD_data.to_csv(file_name_DISBSAD, index = False)
        else:
            print("\nLoading detailed system prices from csv file...")
            DISBSAD_data = pd.read_csv(os.getcwd() + "//" + file_name_DISBSAD)
            DISBSAD_data["Date"] = pd.to_datetime(DISBSAD_data["Date"])
            
            # checks if all the data's there
            max_pre_loaded_date = DISBSAD_data["Date"].max()
            if max_pre_loaded_date < date_to_dt:
                # loads the additional data if needed
                print("Loading additional DISBSAD data from SQL server...")
                DISBSAD_data_temp = SQL_query().DISBSAD_data(max_pre_loaded_date + relativedelta(days = 1), date_to)
                DISBSAD_data_temp["Fuel type"] = DISBSAD_data_temp["NGU ID"].map(NGUID_fuel_type_dict)
                DISBSAD_data_temp["BMU ID"] = DISBSAD_data_temp["NGU ID"].map(NGUID_BMUID_dict)
                DISBSAD_data_temp["Month"] = DISBSAD_data_temp["Date"].dt.strftime("%b-%y")
                DISBSAD_data_temp["Order type"] = "Offer"
                DISBSAD_data_temp["Order type"] = DISBSAD_data_temp["Order type"].where(DISBSAD_data_temp["Volume (MWh)"] > 0, "Bid")
                DISBSAD_data = pd.concat([DISBSAD_data, DISBSAD_data_temp]).reset_index(drop = True)
                print("Exporting new DISBSAD data...")
                DISBSAD_data.to_csv(file_name_DISBSAD, index = False)
            else:
                pass
        DISBSAD_data["Month start"] = pd.to_datetime(DISBSAD_data["Month"], format = "%b-%y")
        DISBSAD_data["Date"] = pd.to_datetime(DISBSAD_data["Date"], format = "%Y-%m-%d")
        # line below removes any NAs in the price value column
        DISBSAD_data["Price (£/MWh)"] = (DISBSAD_data["Cost (£)"].div(DISBSAD_data["Volume (MWh)"]).where(DISBSAD_data["Price (£/MWh)"].isna(), DISBSAD_data["Price (£/MWh)"])) 
        
        DISBSAD_vol_by_service = pd.pivot_table(DISBSAD_data, index = ["Month start", "Month", "Service type"], 
                                                values = "Volume (MWh)", columns = "Order type", aggfunc = "sum").reset_index()
        
        
        DISBSAD_summary_vol_by_service = pd.merge(DISBSAD_vol_by_service[DISBSAD_vol_by_service["Month"] == month_str],
                                                  DISBSAD_vol_by_service[DISBSAD_vol_by_service["Month"] == month_str_prev], 
                                                  on = "Service type", how = "outer", suffixes = [f" vol {month_str}", f" vol {month_str_prev}"])
        # only gets the columns we're interested in
        DISBSAD_summary_vol_by_service = DISBSAD_summary_vol_by_service[[i for i in DISBSAD_summary_vol_by_service.columns.tolist() if "Month" not in i]]
        
        DISBSAD_vol_by_tech = pd.pivot_table(DISBSAD_data, index = ["Month start", "Month", "Fuel type"], 
                                                values = "Volume (MWh)", columns = "Order type", aggfunc = "sum").reset_index()
        
        DISBSAD_summary_vol_by_tech = pd.merge(DISBSAD_vol_by_tech[DISBSAD_vol_by_tech["Month"] == month_str],
                                               DISBSAD_vol_by_tech[DISBSAD_vol_by_tech["Month"] == month_str_prev], 
                                               on = "Fuel type", how = "outer", suffixes = [f" vol {month_str}", f" vol {month_str_prev}"])
        # only gets the columns we're interested in
        DISBSAD_summary_vol_by_tech = DISBSAD_summary_vol_by_tech[[i for i in DISBSAD_summary_vol_by_tech.columns.tolist() if "Month" not in i]]
        
        
        DISBSAD_prices_by_tech = pd.pivot_table(DISBSAD_data, index = ["Month start", "Month", "Service type"], 
                                                values = "Price (£/MWh)", columns = "Fuel type", aggfunc = "mean").reset_index()
        
        
        DISBSAD_data_daily = pd.pivot_table(DISBSAD_data[DISBSAD_data["Month"] == month_str], index = ["Date"], columns = "Fuel type", values = "Volume (MWh)", aggfunc = "sum")
        DISBSAD_max_price = DISBSAD_data[DISBSAD_data["Month"] == month_str].groupby(by = "Date")["Price (£/MWh)"].max().reset_index()
        DISBSAD_data_daily = pd.merge(DISBSAD_data_daily, DISBSAD_max_price, on = "Date", how = "inner").set_index("Date", drop = True)
        
        
        if Load == True:
            row, col = 0, 0
            clear_range_cols = f"A:{num_to_col(len(DISBSAD_vol_by_service.columns.tolist()) + 2 + len(DISBSAD_summary_vol_by_service.columns.tolist()) + 2)}"
            Excel_load("DISBSAD", DISBSAD_vol_by_service.set_index(["Month start", "Service type"]), "A1", name = "DISBSAD Volume by Service", clear_range = "A:I")
            col = len(DISBSAD_vol_by_service.columns.tolist()) + 1
            Excel_load("DISBSAD", DISBSAD_summary_vol_by_service.set_index("Service type"), (row, col), "Volume summary table")
            
            row = 0
            col = 0
            clear_range_cols = f"A:{num_to_col(len(DISBSAD_data_daily.columns.tolist()) + len(DISBSAD_summary_vol_by_tech.columns.tolist()) + 2)}"
            Excel_load("DISBSAD Graphs", DISBSAD_data_daily, "A1", name = "DISBSAD Volume by Fuel Type", clear_range = clear_range_cols)
            col = len(DISBSAD_data_daily.columns.tolist()) + 2
            Excel_load("DISBSAD Graphs", DISBSAD_summary_vol_by_tech.set_index("Fuel type"), (row, col), "DISBSAD Volume by Fuel Type Summary")
    
    
    if EAC == True:
        file_name_EAC = "EAC Sell Order data.csv"
        # checks if data has already been loaded, if not will import it from the SQL server
        if file_name_EAC not in csv_files:
            print("Loading EAC data from SQL server. This will take a long time...")
            BM_date_from = "2023-11-01"
            EAC_data = SQL_query().EAC_data(BM_date_from, date_to)
            EAC_data["BMU ID"] = EAC_data["NGU ID"].map(NGUID_BMUID_dict)
            EAC_data["Fuel type"] = EAC_data["BMU ID"].map(NGUID_fuel_type_dict)
            EAC_data["Month"] = EAC_data["Start time"].dt.strftime("%b-%y")
            
            print("Exporting detailed system prices to csv...")
            EAC_data.to_csv(file_name_EAC, index = False)
        else: # if data has been loaded, will load it in via csv
            print("Loading EAC data from csv file...")
            EAC_data = pd.read_csv(os.getcwd() + "//" + file_name_EAC)
            EAC_data["Start time"] = pd.to_datetime(EAC_data["Start time"])
            EAC_data["End time"] = pd.to_datetime(EAC_data["End time"])
            #print(DSP_data)
            max_pre_loaded_date = EAC_data["Start time"].max()
            if max_pre_loaded_date < date_to_dt:
                # loads the additional data if needed
                print("Loading additional detailed system prices from SQL server...")
                EAC_data_temp = SQL_query().DSP_data(max_pre_loaded_date + relativedelta(days = 1), date_to)
                EAC_data_temp["BMU ID"] = EAC_data_temp["NGU ID"].map(NGUID_BMUID_dict)
                EAC_data_temp["Fuel type"] = EAC_data_temp["BMU ID"].map(NGUID_fuel_type_dict)
                EAC_data_temp["Month"] = EAC_data_temp["Start time"].dt.strftime("%b-%y")
                
                EAC_data = pd.concat([EAC_data, EAC_data_temp]).reset_index(drop = True)
                EAC_data.to_csv(file_name_EAC, index = False)
        
        
    if kW_revenue == True:
        def Capacity_finder(NGU_ID_list, EAC_data):
            print("Finding capacities")
            
            NGU_capacities = pd.DataFrame(NGU_ID_list, columns = ["NGU ID"])
            NGU_capacities["Capacity"] = NGU_capacities["NGU ID"].map(NGU_capacity_dict)
            # some unit capacities will be listed as 0, this sets them to nan so they get replaced later
            NGU_capacities["Capacity"] = NGU_capacities["Capacity"].where(NGU_capacities["Capacity"] != 0, np.nan)
            
            
            """Estimates capacity from EAC data"""
            # this will aim to work out the capacity of NGUs which currently have no capacity attributed to them
            EAC_data = EAC_data[["Basket ID", "NGU ID", "Executed Volume (MW)", "Service"]]
            unit_capacity = EAC_data.groupby(["Basket ID", "NGU ID", "Service"], as_index = False)["Executed Volume (MW)"].sum()
            unit_capacity = unit_capacity.groupby("NGU ID", as_index = False)["Executed Volume (MW)"].max()

            unit_capacity = unit_capacity.groupby("NGU ID", as_index = False)["Executed Volume (MW)"].max()
            
            # if the capacity values are nan, it replaces them with the values found from the EAC
            NGU_capacities["Capacity"] = NGU_capacities["Capacity"].where(NGU_capacities["Capacity"].isna() == False, NGU_capacities["NGU ID"].map(unit_capacity.set_index("NGU ID")["Executed Volume (MW)"].to_dict()))
            
            zero_mw = NGU_capacities[NGU_capacities["Capacity"] == 0]["NGU ID"].tolist()
            
            return NGU_capacities
        
        
        if BM == False:
            print("Loading DSP data...")
            DSP_data = pd.read_csv(os.getcwd() + "//" + csv_file_dict["DSP_data"])
        if EAC == False:
            print("Loading EAC data...")
            EAC_data = pd.read_csv(os.getcwd() + "//" + csv_file_dict["EAC_data"])
        
        NGU_list = list(set(BMU_data["NGU ID"].unique().tolist() + EAC_data["NGU ID"].unique().tolist()))
        # gets dictionary of capacity by NGU
        NGU_capacity_dict = Capacity_finder(NGU_list, EAC_data).set_index("NGU ID").to_dict()["Capacity"]   

        """===================================================================================================
        Begins BM £/kW Analysis
        ======================================================================================================"""
    
        DSP_data["Capacity"] = DSP_data["NGU ID"].map(NGU_capacity_dict)
        DSP_data["Revenue"] = DSP_data["Price (£/MWh)"].mul(DSP_data["Volume (MWh)"])
        #print(DSP_data)
        BM_revenue_by_unit_total = pd.pivot_table(DSP_data, values = "Revenue", index = ["NGU ID", "BMU ID", "Company", "Fuel type", "Capacity"], columns = "Month start", aggfunc = "sum").reset_index()
        print(BM_revenue_by_unit_total)

    print(f"Code finished in: {datetime.now() - start_time}")
else:
    pass
    
