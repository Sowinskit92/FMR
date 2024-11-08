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
import requests
import json

"""This will be a script to pull all the data for the FMR and do all the analysis"""

"""==========================================================================================================
SETTINGS
============================================================================================================="""

Excel_workbook_name = "FMR Analysis - October 2024.xlsx" 
Frequency_data = "October 2024 freq.csv"

date_from: str = "2024-10-01" # date to begin analysis
date_to: str = "2024-10-31" # date to end analysis

# set these to True if you want the analysis to be done

Market_fundementals: bool = False # generation, inertia, MIP, forecast wind/solar data
BM: bool = False
EAC: bool = True
STOR: bool = False
SFFR: bool = False
kW_revenue = False

# Set Load = True if you want the data to be exported to the above Excel file
Load = False

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.reset_option('display.max_rows')

start_time = datetime.now()

# get data off the server
def Data_load(data: str, date_from: str = False, date_to: str = False, BMUID_NGUID_dict = False, 
              NGUID_BMUID_dict = False, BMUID_fuel_type_dict = False, NGUID_fuel_type_dict = False, 
              BMU_company_dict = False, NGU_company_dict = False):
    
    """=======================================================================================================
    SQL Loading
    =========================================================================================================="""
    
    class SQL_query: # creates SQL query class
    
        server = "tcp:mssql-01.citg.one"
        database = "CI_DL1"
        connection_string = "DRIVER={SQL Server};SERVER="+server+";DATABASE="+database+";Trusted_Connection=yes"
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor 
    
        def __init__(self):
            print("Init")
        
        def MIP_data(date_from: str, date_to: str, connection = connection):
            query_string = f"""
            SELECT SettlementDate, HHPeriod, Value, Description
            
            FROM PowerSystem.tblSystemPrice as Price
    
            INNER JOIN Meta.tblDataDescription as DD on DD.DataDescriptionID = Price.DataDescriptionID
            
            WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'
            """
            df = pd.read_sql_query(query_string, connection)
            
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "Value": "Price"}
            df = df.rename(columns = column_renames)
            df = df.sort_values(by = ["Date", "SP"]).reset_index(drop = True)
            
            return df
            
        def BMU_data(connection = connection):
            query_string = f"""
            SELECT Elexon_BMUnitID, NGC_BMUnitID, PartyName, GSPGroup, ReportName, BMU.FuelTypeID
            
            FROM Meta.tblBMUnit_Managed as BMU
    
            INNER JOIN Meta.tblFuelType as ft ON ft.FuelTypeID = BMU.FuelTypeID
            """
            print("Gathering asset information data from SQL server...")
            print(" ")
            df = pd.read_sql_query(query_string, connection)
            
            column_renames = {"Elexon_BMUnitID": "BMU ID", "NGC_BMUnitID": "NGU ID", "PartyName": "Company",
                              "GSPGroup": "GSP Group", "ReportName": "Fuel type", "FuelTypeID": "Fuel type ID"}
            df = df.rename(columns = column_renames)
            
            return df
        
        def NGU_data(connection = connection):
            query_string = """
            SELECT NGESO_NGTUnitID, CompanyName, [BM/NBM], ReportName 
            
            FROM Meta.tblNGTUnit_Managed as NGU
            
            LEFT JOIN Meta.tblFuelType as ft on ft.FuelTypeID = NGU.FuelTypeID
            """
            print("Gathering asset information data from SQL server...")
            print(" ")
            df = pd.read_sql_query(query_string, connection)
            column_renames = {"NGESO_NGTUnitID": "NGU ID", "CompanyName": "Company",
                              "ReportName": "Fuel type", "BM/NBM": "BM/NBM"}
            df = df.rename(columns = column_renames)
            df = df.sort_values(by = "NGU ID").reset_index(drop = True)
            return df
            
        def Capacity_data(connection = connection):
            query_string = f"""
            SELECT *
    
            FROM PowerSystem.tblBMUnitGCDC as Capacity
    
            INNER JOIN Meta.tblBMUnit_Managed as BMU on BMU.BMUnitID = Capacity.BMUnitID
            
            """
            print("Gathering BMU capacity data from SQL server...")
            print(" ")
            
            df = pd.read_sql_query(query_string, connection)
            
            
            # don't use Company as a column in here, as it doesn't come from the BMUManaged table
            column_renames = {"Elexon_BMUnitID": "BMU ID", "Runtime": "Date", "GC": "GC",
                              "DC": "DC", "NGC_BMUnitID": "NGU ID"}
            df = df.rename(columns = column_renames)
            df = df[column_renames.values()]
            return df
        
        def BOD_data(date_from: str, date_to: str, connection = connection):
            date_to = datetime.strftime(datetime.strptime(date_to, "%Y-%m-%d") + relativedelta(days = 1), "%Y-%m-%d")
            query_string = f"""SELECT *
            FROM [PowerSystem].[tblBidOfferData] as BOD
            
            INNER JOIN [Meta].[tblBMUnit_Managed] as BMU
            ON BMU.BMUnitID = BOD.BMUnitID
    
            INNER JOIN [Meta].[tblFuelType] as ft
            ON BMU.FuelTypeID = ft.FuelTypeID
    
            WHERE
            [TimeFromUTC] >= '{date_from}' and [TimeToUTC] <= '{date_to}'
            """
    
            print("Gathering submitted bid/offer data from SQL server...")
            print(" ")
            df = pd.read_sql_query(query_string, connection)
            #print("hello")
            
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP","TimeFromUTC": "Time from",
                              "TimeToUTC": "Time to", "Elexon_BMUnitID": "BMU ID", "NGC_BMUnitID": "NGU ID",
                              "ReportName": "Fuel type", "PartyName": "Company","LevelFrom": "MW from", 
                              "LevelTo": "MW to", "PairId": "Pair ID", "Bid": "Bid price", "Offer": "Offer price"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            df = df.sort_values(by = "Time from").reset_index(drop = True)
            
            return df
        
        def BOA_data(date_from: str, date_to: str, connection = connection):
            pass
        
        
        def DSP_data(date_from: str, date_to: str, connection = connection):
            
            query_string = f"""SELECT *
            FROM PowerSystem.tblDetailedSystemPrices as DSP
            
            WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'
            
            """
            
            print("Gathering submitted Detailed System Prices data from SQL server...")
            print(" ")
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
            
            print("Gathering DISBSAD data from SQL server...")
            print(" ")
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
            
            INNER JOIN Meta.tblNGTUnit_Managed as NGU on NGU.NGESO_NGTUnitID = EAC.Unit_NGESOID
            
            WHERE DeliveryStartDate >= '{date_from}' AND DeliveryEndDate <= '{date_to}'"""
            
            print("Gathering EAC data from SQL server...")
            print(" ")
            df = pd.read_sql_query(query_string, connection)
            column_renames = {"Unit_NGESOID": "NGU ID", "BasketID": "Basket ID", "ServiceType": "Service type",
                              "DeliveryStartDate": "Start time", "DeliveryEndDate": "End time", "OrderType": "Order type",
                              "AuctionProduct": "Service", "Volume": "Volume (MW)", "PriceLimit": "Submitted price (£/MW/hr)",
                              "LoopedBasketID": "Looped Basket ID", "ExecutedVolume": "Executed Volume (MW)",
                              "ClearingPrice": "Clearing price (£/MW/hr)", "CompanyName": "Company"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            return df
        
        def STOR_data(date_from: str, date_to: str, connection = connection):
            date_to = datetime.strftime(datetime.strptime(date_to, "%Y-%m-%d") + relativedelta(days = 1), "%Y-%m-%d")
            query_string = f"""SELECT

            ServiceDeliveryFromDate, ServiceDeliveryToDate, Unit_NGESOID, NGU.CompanyName, NGU.[BM/NBM], FuelType, TenderedMW, ContractedMW,
            TenderedAvailabilityPrice, MarketClearingPrice, Status
            
            FROM PowerSystem.tblSTORDayAheadAuctionResults as STOR
            
            INNER JOIN Meta.tblNGTUnit_Managed as NGU on NGU.NGTUnitID = STOR.NGTUnitID
            
            WHERE ServiceDeliveryFromDate >= '{date_from}' and ServiceDeliveryFromDate <= '{date_to}' """
            print("Gathering STOR data from the SQL server...")
            print()
            column_renames = {"ServiceDeliveryFromDate": "Start time", "ServiceDeliveryToDate": "End time", 
                              "Unit_NGESOID": "NGU ID", "CompanyName": "Company", "BM/NBM": "BM/NBM", 
                              "FuelType": "Fuel type", "TenderedMW": "Submitted MW", "ContractedMW": "Accepted MW",
                              "TenderedAvailabilityPrice": "Availability price", "MarketClearingPrice": "Clearing price",
                              "Status": "Status"}
            df = pd.read_sql_query(query_string, connection)
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            df = df.sort_values(by = "Start time").reset_index(drop = True)
            return df
            
        
        def SFFR_data(date_from: str, date_to: str, connection = connection):
            
            # adds on one day to get all the data
            date_to = datetime.strftime(datetime.strptime(date_to, "%Y-%m-%d") + relativedelta(days = 1), "%Y-%m-%d")
            
            query_string = f"""SELECT DeliveryStart, Status, NGESO_NGTUnitID, NGU.CompanyName, 
            TechnologyType, EFA, [Volume(MW)], [AcceptedVolume(MW)], [Price(£/MWh)], [ClearingPrice(£/MWh)]
            
            FROM PowerSystem.tblFFRStaticAuctionResults as SFFR
            
            INNER JOIN Meta.tblNGTUnit_Managed as NGU on NGU.NGTUnitID = SFFR.NGTUnitID

            WHERE DeliveryStart >= '{date_from}' and DeliveryStart <= '{date_to}'
            
            """
            print("Gathering SFFR data from SQL server...")
            print()
            column_renames = {"DeliveryStart": "Start time", "NGESO_NGTUnitID": "NGU ID", "CompanyName": "Company",
                              "TechnologyType": "Fuel type", "EFA": "EFA", "Volume(MW)": "Submitted MW",
                              "AcceptedVolume(MW)": "Accepted MW", "Price(£/MWh)": "Submitted price (£/MW/hr)",
                              "ClearingPrice(£/MWh)": "Clearing price", "Status": "Status"}
            df = pd.read_sql_query(query_string, connection)
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            df = df.sort_values(by = "Start time").reset_index(drop = True)
            return df
        
        def Inertia_data(date_from: str, date_to: str, connection = connection):
            query_string = f"""SELECT *
            
            FROM PowerSystem.tblSystemInertia
            
            WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'

            ORDER BY SettlementDate, HHPeriod"""
            
            df = pd.read_sql_query(query_string, connection)
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "OutturnInertia": "Outturn Inertia",
                              "MarketProvidedInertia": "Market Provided Inertia"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            return df
        
        def Generation_data(date_from: str, date_to: str, connection = connection):
            query_string = f"""SELECT * 
            
            FROM PowerSystem.tblGenerationByFuel as gen

            INNER JOIN Meta.tblFuelType as Fuel on Fuel.FuelTypeID = gen.FuelTypeID
            
            WHERE SettlementDate >= '{date_from}' and SettlementDate <= '{date_to}'
            
            ORDER BY SettlementDate, HHPeriod"""
            
            print("Gathering gen mix data from the SQL server...")
            print(" ")
            df = pd.read_sql_query(query_string, connection)
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "Value": "MW",
                              "ReportName": "Fuel type"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            return df
        
        def Demand_data(date_from: str, date_to: str, connection = connection):
            query_string = f"""SELECT SettlementDate, HHPeriod, Value, Description
            
            FROM PowerSystem.tblDemandOutturn as demand
            
            INNER JOIN Meta.tblDataDescription as DD on DD.DataDescriptionID = demand.DataDescriptionID
            
            WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'
            """
            df = pd.read_sql_query(query_string, connection)
            column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "Value": "MW", "Description": "Demand type"}
            df.rename(columns = column_renames, inplace = True)
            df = df[column_renames.values()]
            return df
    
    def load(date_from, date_to, csv_file_name, date_col_name):
        # print(date_from, date_to)
        # date_col_name is the name of the datetime column in the dataset (it's used to find the max date)
        if csv_file_name not in [i for i in os.listdir() if i.endswith(".csv")]:
            # if csv file not in directory, loads from SQL server
            # df = getattr(SQL_query, data)(date_from = date_from, date_to = date_to)
            try:
                # print("Hello")
                df = getattr(SQL_query, data)(date_from = date_from, date_to = date_to) # gets the SQL data using the correct method
                
            except:
                # print("ISBD")
                df = getattr(SQL_query, data)()
            export = True
        else:
            export = False
            print(f"Loading data from {csv_file_name}...")
            df = pd.read_csv(csv_file_name)
            
            time_update_list = ["BMU Info.csv", "BMU Capacity data.csv", "NGU Info.csv"] # data which updates regularly
            
            if csv_file_name in time_update_list: # if file was created over 5 days ago, will update
                path = os.getcwd() + f"//{csv_file_name}"
                details = os.path.getctime(path) # gets time created
                created_time = time.strptime(time.ctime(details)) # turns time into annoying time format
                created_time = time.strftime("%Y-%m-%d") # useful string format
                created_time = datetime.strptime(created_time, "%Y-%m-%d") # into datetime format
                
                if created_time < datetime.now() - relativedelta(days = 5):
                    print(f"Updating {csv_file_name}...")
                    try:
                        df = getattr(SQL_query, data)(date_from = date_from, date_to = date_to) # gets the SQL data using the correct method
                    except:
                        df = getattr(SQL_query, data)()
                    export = True
                else:
                    pass
            
            if date_col_name == False:
                pass
            else:
                
                if isinstance(date_col_name, str):
                    df[date_col_name] = pd.to_datetime(df[date_col_name])
                    max_pre_loaded_date = df[date_col_name].max() # finds max date in the dataset
                    min_pre_loaded_date = df[date_col_name].min()
                    
                elif isinstance(date_col_name, list):
                    for i in date_col_name:
                        df[i] = pd.to_datetime(df[i])
                    max_pre_loaded_date = df[date_col_name].max() # finds max date across multiple columns
                    min_pre_loaded_date = df[date_col_name].min() 
                    
                else:
                    raise TypeError("Please enter the datetime columns to check for the max date as either a string or a list")
                
                max_pre_loaded_date_str = datetime.strftime(max_pre_loaded_date, "%Y-%m-%d")
                
                # pulls additional data if the csv file data doesn't go back to date_from
                if datetime.strptime(date_from, "%Y-%m-%d") < min_pre_loaded_date:
                    df_temp1 = getattr(SQL_query, data)(date_from, datetime.strftime(min_pre_loaded_date - relativedelta(days = 1), "%Y-%m-%d"))
                    df = pd.concat([df, df_temp1])
                    export = True
                
                # if the max date in the csv is less than user input date_to, pulls the remaining data off the server
                if max_pre_loaded_date < datetime.strptime(date_to, "%Y-%m-%d") - relativedelta(hours = 2): # -2hrs is there because it would keep pulling from the SQL server when it didn't need to for the EAC data
                    df_temp = getattr(SQL_query, data)(max_pre_loaded_date + relativedelta(days = 1), date_to = date_to)
                    df = pd.concat([df, df_temp])
                    export = True
                else:
                    pass
                
        return df, export
    
    # gets a list of all methods in the SQL class
    SQL_methods = sorted([i for i in dir(SQL_query) if i.endswith("__") == False])
    #API_methods = 
    #print(SQL_methods)
    if data not in SQL_methods:
        raise TypeError(f"Please ener a dataset from the following list to load: {SQL_methods}")
    else:
        pass
    
    if data == "DSP_data":
        csv_file_name = "All DSP data.csv"
        df, export = load(date_from, date_to, csv_file_name, "Date")
        df["Month"] = df["Date"].dt.strftime("%b-%y")
        df["Volume ABS"] = df["Volume (MWh)"].abs()
        
        # works out bid/offer status by splitting value by DISBSAD and normal BOAs
        nums = [str(i) for i in range(2000)]
        DSP_data_temp = df[~df["BMU ID"].isin(nums)].reset_index(drop = True)
        DISBSAD_temp = df[df["BMU ID"].isin(nums)].reset_index(drop = True) # BMU ID is a number for DISBSAD
        
        DSP_data_temp["Order type"] = "Offer"
        DSP_data_temp["Order type"] = DSP_data_temp["Order type"].where(DSP_data_temp["Pair ID"] > 0, "Bid")
        
        DISBSAD_temp["Order type"] = "Offer"
        DISBSAD_temp["Order type"] = DISBSAD_temp["Order type"].where(DISBSAD_temp["Volume (MWh)"] > 0, "Bid") 
        
        df = pd.concat([DSP_data_temp, DISBSAD_temp])
        
        df["Energy/System"] = "System"
        df["Energy/System"] = df["Energy/System"].where((df["SO Flag"] == "T") | (df["CADL Flag"] == "T"), "Energy")
        
        df = df.sort_values(by = ["Date", "SP", "BMU ID"], ascending = True).reset_index(drop = True)
        
        #df["Energy/System"] = "System"
        #df["Energy/System"] = df["Energy/System"].where((df["SO Flag"] == "T"), "Energy")
        df["Month start"] = pd.to_datetime(df["Month"], format = "%b-%y").dt.date
    elif data == "DISBSAD_data":
        csv_file_name = "All DISBSAD data.csv"
        df, export = load(date_from, date_to, csv_file_name, "Date")
        df["Month"] = df["Date"].dt.strftime("%b-%y")
        df["Order type"] = "Offer"
        df["Order type"] = df["Order type"].where(df["Volume (MWh)"] > 0, "Bid")
    elif data == "BMU_data":
        csv_file_name = "BMU Info.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = False)
        df["Company"] = df["Company"].where(df["Company"] != "EDF", "EDF Energy")
    elif data == "Capacity_data":
        csv_file_name = "BMU Capacity data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = False)
        df["BMU Capacity ID"] = df["BMU ID"] + df["Date"].astype(str)
        df["NGU Capacity ID"] = df["NGU ID"] + df["Date"].astype(str)
    elif data == "EAC_data":
        csv_file_name = "EAC Sell Order data.csv"
        # ups the date by one to make the query gather all the data
        date_to = datetime.strftime((datetime.strptime(date_to, "%Y-%m-%d") + relativedelta(days = 1)).date(), "%Y-%m-%d")
        df, export = load(date_from, date_to, csv_file_name, date_col_name = "Start time")
        df["Month"] = df["Start time"].dt.strftime("%b-%y")
    elif data == "Inertia_data":
        csv_file_name = "Inertia data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = "Date")
    elif data == "Generation_data":
        csv_file_name = "Generation data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = "Date")
    elif data == "STOR_data":
        csv_file_name = "STOR data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = "Start time")
    elif data == "SFFR_data":
        csv_file_name = "SFFR data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = "Start time")
    elif data == "BOD_data":
        csv_file_name = "BOD data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = "Date")
    elif data == "NGU_data":
        csv_file_name = "NGU Info.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = False)
    elif data == "Demand_data":
        csv_file_name = "Demand data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = False)
    elif data == "MIP_data":
        csv_file_name = "System prices data.csv"
        df, export = load(date_from, date_to, csv_file_name, date_col_name = False)
        rename_MIP = {"Main Price Summary": "SIP", "APXMIDP": "APX", "N2EXMIDP": "N2EX", "Market Price Summary": "Market Price Summary"}
        df["Description"] = df["Description"].map(rename_MIP).where(df["Description"].isin(rename_MIP.keys()), df["Description"])
        
    
    
    param_names = list(locals().keys())
    
    if isinstance(BMUID_NGUID_dict, dict): # if BMUID_NGUID dict has been input it will add NGU ID based on BMU ID column
        df["NGU ID"] = df["BMU ID"].map(BMUID_NGUID_dict)
    if isinstance(NGUID_BMUID_dict, dict):
        df["BMU ID"] = df["NGU ID"].map(NGUID_BMUID_dict)
    if isinstance(BMUID_fuel_type_dict, dict):
        df["Fuel type"] = df["BMU ID"].map(BMUID_fuel_type_dict)
    if isinstance(NGUID_fuel_type_dict, dict):
        df["Fuel type"] = df["NGU ID"].map(NGUID_fuel_type_dict)
    if isinstance(BMU_company_dict, dict):
        df["Company"] = df["BMU ID"].map(BMU_company_dict)
    if isinstance(NGU_company_dict, dict):
        df["Company"] = df["NGU ID"].map(NGU_company_dict)
    
    if export == True:
        print(f"Exporting {data} to csv file as {csv_file_name}...")
        df.to_csv(csv_file_name, index = False)
    else:
        pass

    return df

# gathers data from Elexon, could put this into a class in future
def Elexon_gather(code: str, date_from: str = False, date_to: str = False, n_days: int = 7, 
               BMU_ID = False, SP = False, message_IDs: list = False, physical_code: str = "PN", file_check: str = False):
    
    # file_check is the name of a file to check for, or something shared in multiple files which already contains you dont want to be recollected
    
    
    # creates API to query
    def API(code: str, date_from = False, date_to = False, BMU_ID: str = False, SP = False, message_IDs: list = False, 
            physical_code = physical_code) -> str:
        
        """code = specific code for the dataset you're wanting to look at
           date_from = string of the starting date
           date_to = string of the end date
           BMU_ID = string of BMU ID
           SP  = int of settlement period"""
           
        
        # starts constructing API
        API = f"https://data.elexon.co.uk/bmrs/api/v1/{code}"
        
        # deals with the unique case when the code is for remit messages
        if code == "remit":
            if isinstance(message_IDs, bool):
                raise TypeError("Please insert message IDs for REMIT as a list")
            elif isinstance(message_IDs, str):
                API = f"{API}?messageId={messageID}&latestRevisionOnly=true&format=json"
                return API
            elif isinstance(message_IDs, list):
                for i, j in enumerate(message_IDs):
                    if i == 0:
                        API = f"{API}?messageId={j}"
                    else:
                        API = f"{API}&messageId={j}"
                API = f"{API}&format=json"
                return API
        
        elif code == "balancing/dynamic/rates": # run up/run down rates
            if (date_from == False) and (isinstance(date_to, str)):
                API = f"{API}?bmUnit={BMU_ID}&snapshotAt={date_to}&format=json"
            elif (isinstance(date_from, str)) and (isinstance(date_to, str)):
                API = f"{API}?bmUnit={BMU_ID}&snapshotAt={date_from}&until={date_to}&format=json"
            else:
                raise TypeError("For BMU rate data please ensure a date_from and/or a date_to has been submitted")
            
            return API
        
        elif code == "datasets/FUELHH":
            API = f"{API}?&settlementDatefrom={date_from}&settlementDateto={date_to}&format=json"
            return API
        
        elif code == "balancing/physical":
            API = f"{API}?bmUnit={BMU_ID}&from={date_from}&to={date_to}&dataset={physical_code}&format=json"
            return API
        elif code == "/forecast/generation/wind-and-solar/day-ahead":
            API = f"{API}?from={date_from}&to={date_to}&processType=day%20ahead&format=json"
            return API
        # deals with when there's only the code to be input
        elif (date_from == False) and (date_to == False) and (BMU_ID == False) and (SP == False): 
            return API
        else:
            # This condition is when both date_from and date_to are input
            if isinstance(date_from, bool) == False:
                if isinstance(date_from, str) == False: #checks to make sure the date is a string
                    raise TypeError(f"Please enter DATE_FROM {date_from} as a string in the format YYYY-MM-DD in {code}")
                elif isinstance(date_to, str) == False:
                    raise TypeError(f"Please enter DATE_TO {date_to} as a string in the format YYYY-MM-DD in {code}")
                else:
                    API = f"{API}?from={date_from}&to={date_to}"
            
            if isinstance(BMU_ID, str) == True:
                raise TypeError("API creation for BMU ID not complete")
            
            if isinstance(SP, str) == True:
                raise TypeError("API creation for SP not complete")
                
            API = f"{API}&format=json"
            
            return API
    
    # will rename columns if they're in the imported dataset, so column names are standardised
    def column_rename(df):
        column_renames = {"nationalGridBmUnit": "NGU ID", "elexonBmUnit": "BMU ID", 
                          "settlementDate": "Date", "settlementPeriod": "SP", 
                          "publishTime": "Publish time", "bmUnit": "BMU ID", 
                          "fuelType": "Fuel type", "normalCapacity": "Normal MW", 
                          "unavailableCapacity": "Unavailable MW", "availableCapacity": "Available MW", 
                          "assetId": "BMU ID", "eventStartTime": "Start time", 
                          "eventEndTime": "End time", "cause": "Issue", 
                          "unavailabilityType": "Unavailability type", "timeFrom": "Time from", "timeTo": "Time to",
                          "levelFrom": "MW from", "levelTo": "MW to", "leadPartyName": "Company", "startTime": "Start time",
                          "price": "Price", "volume": "Volume", "quantity": "MW", "psrType": "Fuel type", 
                          "transmissionSystemDemand": "Transmission demand (MW)", "nationalDemand": "National demand (MW)"}
        
        df = df.rename(columns = column_renames)
        return df
    
    # depending on inputs, will run the API the necessary number of times to get the required data
    def expand(code, date_from: str, date_to: str, BMU_ID = False, SP = False, 
               message_IDs = False, n_days: int = 7, physical_code = physical_code):
        
        # deals with the case where both dates are needed
        if (isinstance(date_from, str)) and (isinstance(date_to, str)):
            check = 0
            # print("HI")
            # this needs to make sure that things aren't concated if the query is only run once
            if (datetime.strptime(date_to, "%Y-%m-%d") - datetime.strptime(date_from, "%Y-%m-%d")).days >= n_days:
                #raise TypeError("Expand does not currently work for a single API query")
                check = 1
                date_from = datetime.strptime(date_from, "%Y-%m-%d")
                date_to = datetime.strptime(date_to, "%Y-%m-%d")
            
                date_from_temp = date_from 
                date_to_temp = date_from_temp + timedelta(days = n_days)
                
                df = 0
                count = 0
                while date_to_temp <= date_to: # gets main bulk of queries
                    print(date_from_temp, date_to_temp)
                    
                    # run queries here
                    data = requests.get(API(code = code, date_from = datetime.strftime(date_from_temp,"%Y-%m-%d"),
                                            date_to = datetime.strftime(date_to_temp, "%Y-%m-%d"),
                                            BMU_ID = BMU_ID, SP = SP, message_IDs = message_IDs, physical_code = physical_code)).json()
                    try:
                        data = pd.json_normalize(data, record_path = 'data')
                    except:
                        data = pd.json_normalize(data)
                    #print(data)
                    
                    date_from_temp += timedelta(days = n_days)
                    date_to_temp += timedelta(days = n_days)
                    count += 1
                    if isinstance(df, int):
                        df = data
                    else:
                        df_temp = data
                        df = pd.concat([df, df_temp])
            else:
                pass
            
            if check == 0:
                date_to = datetime.strptime(date_to, "%Y-%m-%d")
                date_from_temp = datetime.strptime(date_from, "%Y-%m-%d")
            else:
                pass
            
            date_to = date_to + relativedelta(days = 1) # adds extra day to date_to to make sure all data is gathered
            
            # runs either final query between date_from_temp & date_to or the first one depending on if multiple need to be run
            data = requests.get(API(code = code, date_from = datetime.strftime(date_from_temp,"%Y-%m-%d"),
                                    date_to = datetime.strftime(date_to, "%Y-%m-%d"),
                                    BMU_ID = BMU_ID, SP = SP, message_IDs = message_IDs, physical_code = physical_code)).json()
            try:
                data = pd.json_normalize(data, record_path = 'data')
            except:
                data = pd.json_normalize(data)
            
            
            if check == 1: # check == 1 if the time range is longer than n_days
                df = pd.concat([df, data])
            else:
                df = data
                
            df = df.drop_duplicates(keep = "first")
            
            df.reset_index(drop = True, inplace = True)
            df = column_rename(df)
            print(date_from_temp, date_to)
            
            return df
        
        # deals with the case where only date_to is input (for BMU rates)
        elif isinstance(date_to, str) and (date_from == False):
            data = requests.get(API(code = code, date_from = False, date_to = date_to, 
                                    BMU_ID = BMU_ID, SP = False, message_IDs = message_IDs, physical_code = physical_code)).json()
            data = pd.json_normalize(data, record_path = ["data"])
            data = column_rename(data)
            return data
            
        # deals with the case where there's only the code input (for BMU info, REMIT)
        else:
            data = requests.get(API(code = code, date_from = False, date_to = False, 
                                    BMU_ID = False, SP = False, message_IDs = message_IDs, physical_code = physical_code)).json()
            
            try: # checks to see if the json data is nested
                data = pd.json_normalize(data, record_path = "data")
            except:
                data = pd.json_normalize(data)
            
            data = column_rename(data)
            return data
    
    """Potential to put a function here which will check for a given file name and if it's found, only collect data
    from Elexon which isn't already in the file"""
    
    if file_check != False:
        # I've already done something similar in Physical_data
        file_list = [i for i in os.listdir() if file_check in i]
        print(file_list)
        
    else:
        pass
    
    data = expand(code = code, date_from = date_from, date_to = date_to, BMU_ID = BMU_ID, 
                  SP = SP, message_IDs = message_IDs, n_days = n_days, physical_code = physical_code)
    
    return data

def DA_Renewable_Generation_Forecast(date_from: str, date_to: str):
    print("Gathering DA Renewable generation data...")
    code = "/forecast/generation/wind-and-solar/day-ahead"
    

    df = Elexon_gather(code, date_from, date_to)
    
    df["Start time"] = pd.to_datetime(df["Start time"])
    df = pd.pivot_table(df, values = "MW", index = "Start time", columns = "Fuel type")
    df["Total forecast renewable generation"] = df[df.columns.tolist()].sum(axis = 1)
    
    return df

def DA_Demand_Forecast(date_from: str, date_to: str):
    print("Gathering DA Demand data...")
    code = "/forecast/demand/day-ahead/latest"
    df = Elexon_gather(code, date_from, date_to)
    df["Start time"] = pd.to_datetime(df["Start time"])
    
    df = df.sort_values(by = "Start time")[["Start time", "Date", "SP", "Transmission demand (MW)", "National demand (MW)"]]
    return df

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
    
    def add_chart(sheet_name, data_cell_ref, left_pos, top_pos, chart_type: str, Help = False) -> None:
        
        if Help == True:
            chart_types = ["column_stacked", "column_stacked_100", "line", "bar", "bar_stacked", "pie", "area", 
                           "line_markers", "xy_scatter", "xy_scatter_lines", "xy_scatter_lines_no_markers",
                           "xy_scatter_smooth", "xy_scatter_smooth_no_markers", "area_stacked", "area_stacked_100",
                           "column_clustered"]
            print(f"Available chart types are: {sorted(chart_types)}")
        else:
            pass
        
        """left_pos = col_number*55 # position from the left of the screen (each column's standard width is 55)
        top_pos = row_num*15 # position from the top of the screen (each row's standard width is 15)
        """
        Graph = workbook.sheets[sheet_name].charts.add(left = left_pos, top = top_pos, width = 355, height = 211)
        Graph.set_source_data(workbook.sheets[sheet_name].range(data_cell_ref).expand())
        Graph.chart_type = chart_type
        
    # opens workbook if Load is True
    if Load == True:
        print(f"Opening {Excel_workbook_name} file...")
        workbook = xw.Book(Excel_workbook_name)
    else:
        pass  
    
    """=======================================================================================================
    Datetime dates for the code
    =========================================================================================================="""
    
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d") # datetime of date_from
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") # datetime of date_to
    
    month_str = date_from_dt.strftime("%b-%y")
        
    date_from_prev_dt = date_from_dt + relativedelta(months = -1, day = 1) # previous month start
    date_to_prev_dt = date_from_dt + relativedelta(days = -1) # previous month end
    
    date_from_prev = datetime.strftime(date_from_prev_dt, "%Y-%m-%d")
    date_to_prev = datetime.strftime(date_to_prev_dt, "%Y-%m-%d")
    
    month_str_prev = date_from_prev_dt.strftime("%b-%y")
    
    """=======================================================================================================
    Initial data loading
    =========================================================================================================="""
    
    # loads BMU data
    BMU_data = Data_load("BMU_data", date_from = date_from, date_to = date_to)
    NGU_data = Data_load("NGU_data", date_from = date_from, date_to = date_to)
    # loads BMU capacity data
    BMU_capacity_data = Data_load("Capacity_data", date_from = date_from, date_to = date_to)
    BMU_capacity_data["DC"] = BMU_capacity_data["DC"].abs()
    BMU_capacity_data["Capacity"] = BMU_capacity_data[["GC", "DC"]].max()
    BMU_capacity_dict = BMU_capacity_data.set_index("BMU ID").to_dict()["GC"]
    #BMU_capacity_dict = sorted(BMU_capacity_dict)
    
    
    """=======================================================================================================
    Dictionaries to help
    =========================================================================================================="""
    
    hour_to_EFA_dict = {22: 1, 23: 1, 
                        2: 2, 3: 2,
                        6: 3, 7: 3, 
                        10: 4, 11: 4, 
                        14: 5, 15: 5, 
                        18: 6, 19: 6}
    
    SP_to_EFA_dict = {45: 1, 5: 2, 13: 3, 21: 4, 29: 5, 37:6}
    
    # creates dictionaries to help analysis
    BMUID_fuel_type_dict = BMU_data.set_index("BMU ID")["Fuel type"].to_dict()
    BMUID_NGUID_dict = BMU_data.set_index("BMU ID")["NGU ID"].to_dict()
    NGUID_BMUID_dict = BMU_data.set_index("NGU ID")["BMU ID"].to_dict()
    
    
    NGUID_fuel_type_dict1 = NGU_data.set_index("NGU ID")["Fuel type"].to_dict()
    NGUID_fuel_type_dict2 = BMU_data.set_index("NGU ID")["Fuel type"].to_dict()
    NGUID_fuel_type_dict = {}
    
    BMU_capacity_data["NGU ID"] = BMU_capacity_data["BMU ID"].map(BMUID_NGUID_dict)
    BMU_capacity_dict_by_NGUID = BMU_capacity_data.set_index("NGU ID").to_dict()["GC"]  # used in BM £/kW
    
    """Below updates the NGU fuel types into 1 dictionary"""
    keys_1 = set(NGUID_fuel_type_dict1.keys())
    keys_2 = set(NGUID_fuel_type_dict2.keys())
    all_keys = keys_1 | keys_2
    for key in all_keys:
        a = 0
        if key in keys_1:
            a = NGUID_fuel_type_dict1[key]
        if key in keys_2:
            a = NGUID_fuel_type_dict2[key]
        
        NGUID_fuel_type_dict[key] = a
    
            
    BMU_company_dict = BMU_data.set_index("BMU ID")["Company"].to_dict()
    NGU_company_dict1 = NGU_data.set_index("NGU ID")["Company"].to_dict()
    NGU_company_dict2 = BMU_data.set_index("NGU ID")["Company"].to_dict()
    NGU_company_dict = {}
    
    keys_1 = set(NGU_company_dict1.keys())
    keys_2 = set(NGU_company_dict2.keys())
    all_keys = keys_1 | keys_2
    for key in all_keys:
        a = 0
        if key in keys_1:
            a = NGU_company_dict1[key]
        if key in keys_2:
            a = NGU_company_dict2[key]
        
        NGU_company_dict[key] = a
    
    
    if Market_fundementals == True:
        
        """=======================================================================================================
        Inertia data
        =========================================================================================================="""
        print("Gathering inertia data...")
        
        inertia_data = Data_load("Inertia_data", date_from = "2023-01-01", date_to = date_to)
        
        inertia_data["Month"] = inertia_data["Date"].dt.strftime("%b-%y")
        inertia_data["Month start"] = pd.to_datetime(inertia_data["Month"], format = "%b-%y")
        
        inertia_col_list = ["Outturn Inertia", "Market Provided Inertia"]
        
        inertia_table = inertia_data.groupby(["Month start", "Month"])[inertia_col_list].mean()
        
        for i in inertia_col_list:
            inertia_table[f"{i} volatility"] = inertia_data.groupby(["Month start", "Month"])[i].std()
        
        #print(inertia_table)
        
        # m-o-m change in inertia
        inertia_change = inertia_table/inertia_table.shift(1) - 1
        
        if Load == True:
            Excel_load("Market fundementals", inertia_table, "A1", name = "Inertia data", clear_range = "A:F")
            mf_row = len(inertia_table.index) + 2
            Excel_load("Market fundementals", inertia_change.tail(1), (mf_row, 0), name = "m-o-m change")
            mf_col = len(inertia_table.columns.tolist()) + 3
            
            #add_chart("Market fundemantals", "A1", 500, 10, "column_clustered")
        
        """=======================================================================================================
        Generation data
        =========================================================================================================="""
        
        gen_mix = Data_load("Generation_data", date_from = date_from_prev, date_to = date_to)
        #print(gen_mix)
        gen_mix["Month"] = gen_mix["Date"].dt.strftime("%b-%y")
        #print(gen_mix)
        gen_mix["Month start"] = pd.to_datetime(gen_mix["Month"], format = "%b-%y")
        
        gen_mix_table = pd.pivot_table(gen_mix, index = ["Month start", "Month"], columns = "Fuel type", values = "MW", aggfunc = "sum")/2000000
        
        gen_mix_table["Solar"] = gen_mix_table[["Solar", "Solar (Embedded)"]].mean(axis = 1) # takes average between the two solar values in the data set as there's a slight discrepancy between the two
        # print(gen_mix_table)
        gen_mix_cols = [i for i in gen_mix_table.columns.tolist()]
        
        for i in ["Embedded", "Offshore", "Onshore"]: # removes columns if they include these values
            gen_mix_table = gen_mix_table.drop(gen_mix_table.filter(regex = i).columns, axis=1)
        
        gen_mix_change = gen_mix_table/gen_mix_table.shift(1) - 1
        
        if Load == True:
            clear_range = f"{num_to_col(mf_col + 1)}:{num_to_col(mf_col + len(gen_mix_table.columns.tolist()) + 2)}"
            print(clear_range)
            Excel_load("Market fundementals", gen_mix_table, (0, mf_col), name = "Generation by fuel type (TWh)", clear_range = clear_range)
            mf_row = len(gen_mix_table.index) + 2
            Excel_load("Market fundementals", gen_mix_change.tail(1), (mf_row, mf_col), name = "m-o-m change")

        
        """=======================================================================================================
        Wind/solar DA forecasts
        =========================================================================================================="""
        
        """=======================================================================================================
        Demand data
        =========================================================================================================="""
        
        demand_data = Data_load("Demand_data", date_from = date_from, date_to = date_to)
        demand_data["Date"] = pd.to_datetime(demand_data["Date"])
        demand_data["Month"] = pd.to_datetime(demand_data["Date"].dt.strftime("%b-%y"), format = "%b-%y")
        
        demand_summary = pd.pivot_table(demand_data, values = "MW", index = "Month", columns = "Demand type", aggfunc = ["sum", "mean"])
        
        
        # demand_col_renames = {i: (f"{i[1]} (TWh)" if i[0] == "sum" else f"{i[1]} (MW)") for i in demand_summary.columns.tolist()}
        demand_summary.columns = list(map("-".join, demand_summary.columns))
        
        for i in demand_summary.columns.tolist():
            if "sum" in i:
                demand_summary[i] = demand_summary[i]/2E6 # TWh
                demand_summary.rename(columns = {i: f"{i.replace('sum-', 'Total ')} (TWh)"}, inplace = True)
            elif "mean" in i:
                demand_summary[i] = demand_summary[i]/1E3 # GW
                demand_summary.rename(columns = {i: f"{i.replace('mean-', 'Average ')} (GW)"}, inplace = True)
            else:
                pass
        
        # demand_data_dr = demand_data[(demand_data["Date"] >= date_from_dt) & (demand_data["Date"] <= date_to_dt)].reset_index(drop = True)
        demand_summary_mom = demand_summary/demand_summary.shift(1) - 1
        if Load == True:
            mf_row = mf_row + 6
            Excel_load("Market fundementals", demand_summary, (mf_row, mf_col), "Demand summary")
            mf_row = mf_row + 4
            Excel_load("Market fundementals", demand_summary_mom.tail(1), (mf_row, mf_col), "m-o-m change")
            mf_col = mf_col + len(demand_summary.columns.tolist()) + 3
        
        """=======================================================================================================
        MIP data
        =========================================================================================================="""
        
        System_price_data = Data_load("MIP_data", date_from = date_from, date_to = date_to)

        MIP_data = System_price_data[System_price_data["Description"] == "APX"].reset_index(drop = True)
        MIP_data["Date"] = pd.to_datetime(MIP_data["Date"])

        MIP_data["Month"] = pd.to_datetime(MIP_data["Date"].dt.strftime("%b-%y"), format = "%b-%y")
        
        MIP_data["EFA"] = MIP_data["SP"].map(SP_to_EFA_dict).ffill().bfill() # ffill fills out most, bfill fills out the very beginning NA EFA block values
        MIP_data_summary = MIP_data.groupby(by = "Month").agg(Mean = ("Price", "mean"), Volatility = ("Price", "std"))
        
        Spread_data = MIP_data.groupby(["Month", "Date", "EFA"])["Price"].max() - MIP_data.groupby(["Month", "Date", "EFA"])["Price"].min()
        Spread_data = Spread_data.reset_index()
        Spread_data = Spread_data.groupby("Month").agg(Average_EFA_spread = ("Price", "mean"))
        
        MIP_data_summary = pd.merge(MIP_data_summary, Spread_data["Average_EFA_spread"], left_index = True, right_index = True)
        MIP_data_summary_mom = MIP_data_summary/MIP_data_summary.shift(1) - 1
        
        if Load == True:
            Excel_load("Market fundementals", MIP_data_summary, (mf_row - 4, mf_col), "MIP Summary")
            Excel_load("Market fundementals", MIP_data_summary_mom.tail(1), (mf_row, mf_col), "MIP Summary")
        
    if BM == True:
        """===================================================================================================
        Loads Detailed system prices
        ======================================================================================================"""
        
        BM_date_from = "2023-11-01"
        
        DSP_data = Data_load("DSP_data", date_from = BM_date_from, date_to = date_to, BMUID_NGUID_dict = BMUID_NGUID_dict,
                             BMUID_fuel_type_dict = BMUID_fuel_type_dict)
        
        SIP = Data_load("MIP_data", date_from = BM_date_from, date_to = date_to)
        SIP = SIP[SIP["Description"] == "SIP"].reset_index(drop = True)
        SIP["Month"] = pd.to_datetime(SIP["Date"]).dt.strftime("%b-%y")
        SIP["Month start"] = pd.to_datetime(SIP["Month"], format = "%b-%y").dt.date
        # SIP = pd.pivot_table(SIP, index = "Month start", values = "Price (£/MWh)", )
        SIP = SIP.groupby("Month start").agg(Average = ("Price", "mean"), Volatility = ("Price", "std"))
        
        """===================================================================================================
        Begins BM Analysis
        ======================================================================================================"""
        DSP_data["Month start"] = pd.to_datetime(DSP_data["Month"], format = "%b-%y").dt.date
        
        # below works out the different bids/offers by normal BOAs and DISBSAD
        tech_vol_summary = pd.pivot_table(DSP_data, columns = "Month start", values = "Volume ABS", index = "Fuel type", aggfunc = "sum")
        tech_price_summary = pd.pivot_table(DSP_data, columns = "Month start", values = "Price (£/MWh)", index = ["Order type", "Fuel type"], aggfunc = "mean")
        #print(tech_vol_summary)
        
        DSP_data_dr = DSP_data[(DSP_data["Date"] >= date_from_dt) & (DSP_data["Date"] <= date_to_dt)].reset_index(drop = True)
        active_BMUs_during_period = DSP_data_dr[["BMU ID", "Fuel type"]].drop_duplicates(keep = "first").set_index("BMU ID")
        
        if Load == True:
            Excel_load("Units active in BM", active_BMUs_during_period, "A1", "Active BMUs during period", "A:C")
            Excel_load("BM prices", tech_price_summary, "A1", "Average prices by tech type by month", f"A:{num_to_col(len(tech_price_summary) + 1)}")
            row = len(tech_price_summary.index) + 2
            Excel_load("BM prices", SIP, (row, 0), "Average SIP")
            
            
            
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
        BM_volume_share = pd.pivot_table(DSP_data, index = "Month start", values = "Volume ABS", columns = "Fuel type", aggfunc = "sum", margins = True, margins_name = "Total volume")
        
        for i in BM_volume_share.columns.tolist()[:-1]:
            BM_volume_share[i] = BM_volume_share[i]/BM_volume_share[BM_volume_share.columns.to_list()[-1]]

        BM_volume_share.drop(BM_volume_share.columns.to_list()[-1], inplace = True, axis = 1) #removes total vol column
        BM_volume_share.drop(BM_volume_share.index.to_list()[-1], inplace = True, axis = 0) #removes total vol row
        
        if Load == True:
            col = len(tech_vol_summary.columns.tolist())
            Excel_load("BM Volumes", tech_vol_summary, "A1", "Abs volume by tech type by month", f"A:{num_to_col(col)}")
            row = len(tech_vol_summary.index) + 2
            Excel_load("BM Volumes", BM_volume_share, (row, 0), "Abs volume share")
            
            #add_chart("BM Volume share", "A1", 500, 10, chart_type = "column_stacked")
            """chart1 = workbook.sheets["BM Volume share"].charts.add(left = 500, top = 15)
            chart1.set_source_data(workbook.sheets["BM Volume share"].range("A1").expand())
            chart1.chart_type = 'column_stacked'
            """
            
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
                
                #print(change_df)
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
        DISBSAD_data = Data_load("DISBSAD_data", date_from = "2023-11-01", date_to = date_to, NGUID_BMUID_dict = NGUID_BMUID_dict, 
                                 NGUID_fuel_type_dict = NGUID_fuel_type_dict2)
        
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
        
        DISBSAD_data_dr = DISBSAD_data[(DISBSAD_data["Date"] >= date_from_dt) & (DISBSAD_data["Date"] <= date_to_dt)].reset_index(drop = True)
        
        DISBSAD_prices_by_tech = pd.pivot_table(DISBSAD_data_dr, index = "Fuel type", columns = "Order type", values = "Price (£/MWh)", aggfunc = "mean")
        
        DISBSAD_data_daily = pd.pivot_table(DISBSAD_data[DISBSAD_data["Month"] == month_str], index = ["Date"], columns = "Fuel type", values = "Volume (MWh)", aggfunc = "sum")
        DISBSAD_max_price = DISBSAD_data[DISBSAD_data["Month"] == month_str].groupby(by = "Date")["Price (£/MWh)"].max().reset_index()
        DISBSAD_data_daily = pd.merge(DISBSAD_data_daily, DISBSAD_max_price, on = "Date", how = "inner").set_index("Date", drop = True)
        
        
        if Load == True:
            row, col = 0, 0
            Excel_load("DISBSAD data", DISBSAD_data_dr, "A1", name = "DISBSAD data", clear_range = f"A:{num_to_col(len(DISBSAD_data_dr.columns.tolist()) + 1)}")
            
            clear_range_cols = f"A:{num_to_col(len(DISBSAD_vol_by_service.columns.tolist()) + 2 + len(DISBSAD_summary_vol_by_service.columns.tolist()) + 2 + len(DISBSAD_prices_by_tech) + 2)}"
            Excel_load("DISBSAD", DISBSAD_vol_by_service.set_index(["Month start", "Service type"]), "A1", name = "DISBSAD Volume by Service", clear_range = "A:I")
            col = len(DISBSAD_vol_by_service.columns.tolist()) + 1
            Excel_load("DISBSAD", DISBSAD_summary_vol_by_service.set_index("Service type"), (row, col), "Volume summary table")
            row = 0
            col = 0
            clear_range_cols = f"A:{num_to_col(len(DISBSAD_data_daily.columns.tolist()) + len(DISBSAD_summary_vol_by_tech.columns.tolist()) + 2)}"
            Excel_load("DISBSAD Graphs", DISBSAD_data_daily, "A1", name = "DISBSAD Volume by Fuel Type", clear_range = clear_range_cols)
            col = len(DISBSAD_data_daily.columns.tolist()) + 2
            Excel_load("DISBSAD Graphs", DISBSAD_summary_vol_by_tech.set_index("Fuel type"), (row, col), "DISBSAD Volume by Fuel Type Summary")
            
            col = col + len(DISBSAD_summary_vol_by_tech) + 2 
            Excel_load("DISBSAD Graphs", DISBSAD_prices_by_tech, (row, col), "DISBSAD Prices by Fuel Type Summary")
            
    if EAC == True:
        
        EAC_data = Data_load("EAC_data", date_from = date_from, date_to = date_to, 
                             NGUID_fuel_type_dict = NGUID_fuel_type_dict2, NGUID_BMUID_dict = NGUID_BMUID_dict)
        print("Analysing EAC data...")
        EAC_data["Start time"] = pd.to_datetime(EAC_data["Start time"])
        EAC_data["End time"] = pd.to_datetime(EAC_data["End time"])
        EAC_data["Month"] = EAC_data["Start time"].dt.strftime("%b-%y")
        EAC_data["Month start"] = pd.to_datetime(EAC_data["Month"], format = "%b-%y")
                   
        EAC_data["Date"] = EAC_data["Start time"].dt.date
        EAC_data["BMU?"] = "NBM"
        EAC_data["BMU?"] = EAC_data["BMU?"].where(EAC_data["BMU ID"].isnull(), "BM")
        
        # price summaries by service
        EAC_price_summary = EAC_data.groupby(["Month start", "Date", "Start time", "Service"], as_index = False).agg({"Clearing price (£/MW/hr)": "mean", "Volume (MW)": "sum", "Executed Volume (MW)": "sum"})
        
        EAC_price_summary_mean = pd.pivot_table(EAC_price_summary, values = "Clearing price (£/MW/hr)", columns = "Service", index = "Month start", aggfunc = "mean")
        EAC_price_summary_std = pd.pivot_table(EAC_price_summary, values = "Clearing price (£/MW/hr)", columns = "Service", index = "Month start", aggfunc = "std")
        
        EAC_sub_vol_summary_mean = pd.pivot_table(EAC_price_summary, values = "Volume (MW)", columns = "Service", index = "Month start", aggfunc = "mean")
        EAC_acc_vol_summary_mean = pd.pivot_table(EAC_price_summary, values = "Executed Volume (MW)", columns = "Service", index = "Month start", aggfunc = "mean")
        
        
        EAC_data_dr = EAC_data[(EAC_data["Date"] >= date_from_dt.date()) & (EAC_data["Date"] <= date_to_dt.date())].reset_index(drop = True)
        # EAC_data_dr = EAC_data[(EAC_data["Date"] >= date_from_dt.date() - relativedelta(days = 1)) & (EAC_data["Date"] <= date_to_dt.date())].reset_index(drop = True)
        EAC_data_dr = EAC_data_dr.sort_values("Start time", ascending = True)
        
        # EAC_data_dr["BMU ID"] = EAC_data_dr["NGU ID"].map(NGUID_BMUID_dict)

        
        volumes_by_unit_type = pd.pivot_table(EAC_data, index = ["Month start", "BMU?"], columns = "Service", values = "Executed Volume (MW)", aggfunc = "sum")
        
        if Load == True:
            Excel_load("Clearing price graph", volumes_by_unit_type, "Z12", name = "Accepted volume by unit type")
        
        #EAC_data_dr["Date"] = EAC_data["Start time"].dt.date
        EAC_data_dr["Submitted £/hr"] = EAC_data_dr["Volume (MW)"].mul(EAC_data_dr["Submitted price (£/MW/hr)"])
        
        EAC_services = EAC_data_dr["Service"].unique().tolist() # list of services on the EAC
        
        
        """=======================================================================================================
        Response service £/MWh calculation
        =========================================================================================================="""
        if isinstance(Frequency_data, str):
            
            upper_lims = {"DRH": 50.2, "DMH": 50.2, "DCH": 50.5,
                          "DRL": 49.985, "DML": 49.9, "DCL": 49.8}

            lower_lims = {"DRH": 50.015, "DMH": 50.1, "DCH": 50.2, 
                          "DRL": 49.8, "DML": 49.8, "DCL": 49.5}
            
            print("Analysing frequency data...")
            freq = pd.read_csv(Frequency_data)
            freq["dtm"] = pd.to_datetime(freq["dtm"])

            freq["Date"] = freq["dtm"].dt.date
            freq["EFA"] = freq["dtm"].dt.hour.map(hour_to_EFA_dict)
            
            response_services = [i for i in EAC_services if "D" in i]
            
            if str(freq.loc[0, "dtm"].time()) == "00:00:00":
                # if the first item in the df is from the beginning of a month, it will be in EFA1 but won't be picked up
                # in the analysis so far, so this amends that
                freq.loc[0, "EFA"] = 1
                # forward fills data to make sure all of the first EFA block of the df is filled in
                freq["EFA"] = freq["EFA"].ffill()
                
            for i in response_services:
                print(f"Calculating energy from {i}...")
                freq["MW"] = 1 # reference MW size is 1MW
                
                # sets frequency limits
                upper_lim = upper_lims[i]
                lower_lim = lower_lims[i]
                
                seconds_in_hour = 60*60
                
                c = 0.95/(upper_lim - lower_lim)
                
                col_name = f"{i}"
                # adds in the MWhs used based on service and frequency deviation
                if i.endswith('L') == True:
                    
                    # for each frequency deviation, works out the MWh for 1MW if the frequency is within the service limits
                    freq[col_name] = (freq['MW']*((c*freq['f']) - (0.05 + c*upper_lim))).where((freq['f'] <= upper_lim) & (freq['f'] >= lower_lim), 0)
                    
                    # any frequency above the upper limit puts the volume at MW and keeps the rest as they are
                    freq[col_name] = (freq["MW"]).where(freq["f"] < lower_lim, freq[col_name]) 
                else:
                    
                    freq[col_name] = (freq['MW']*((c*freq['f']) + (1 - c*upper_lim))).where((freq['f'] <= upper_lim) & (freq['f'] >= lower_lim), 0)
                    freq[col_name] = (freq["MW"]).where(freq["f"] > upper_lim, freq[col_name])
                
                
                freq[col_name] = freq[col_name].div(seconds_in_hour) #gets values into MWh
            
            energy_by_EFA_temp = freq.groupby(["Date", "EFA"])[response_services].sum().reset_index()
            
            energy_by_EFA = pd.melt(energy_by_EFA_temp, id_vars = ["Date", "EFA"], 
                                    value_vars = response_services, 
                                    value_name = "MWh")
            energy_by_EFA.rename(columns = {"variable": "Service"}, inplace = True)
            energy_by_EFA["ID"] = energy_by_EFA["Date"].astype(str) + (energy_by_EFA["EFA"].astype(int)).astype(str) + energy_by_EFA["Service"]
            energy_by_EFA_dict = energy_by_EFA.set_index("ID").to_dict()["MWh"]
            print(energy_by_EFA)
            print("Getting response service revenue by EFA block...")
            # gets the response clearing prices
            freq_cp = EAC_data_dr[EAC_data_dr["Service"].isin(response_services)]
            freq_cp["EFA"] = freq_cp["Start time"].dt.hour.map(hour_to_EFA_dict)
            
            freq_cp["dt"] = (freq_cp["End time"] - freq_cp["Start time"]).dt.total_seconds()/3600
            freq_cp["Revenue"] = freq_cp["Executed Volume (MW)"].mul(freq_cp["Clearing price (£/MW/hr)"]).mul(freq_cp["dt"])
            
            freq_cp = freq_cp.groupby(["Date", "EFA", "Service"])["Revenue"].mean().reset_index()
            freq_cp["ID"] = freq_cp["Date"].astype(str) + (freq_cp["EFA"].astype(int)).astype(str) + freq_cp["Service"]
            freq_cp["MWh"] = freq_cp["ID"].map(energy_by_EFA_dict)
            # finds £/MWh values but sets this to 0 if MWh == 0
            freq_cp["£/MWh"] = freq_cp["Revenue"].div(freq_cp["MWh"]).where(freq_cp["MWh"] != 0, 0)
            freq_cp = pd.pivot_table(freq_cp, index = ["Date", "EFA"], values = "£/MWh", columns = "Service")
            print(freq_cp)
            sys.exit()
            if Load == True:
                Excel_load("Response volume", freq_cp, "A1", name = "£/MWh")
        else:
            pass
        
        
        """=======================================================================================================
        EAC Response/Reserve services
        =========================================================================================================="""
        
        for i in EAC_services:
            print(i)
            df_filt = EAC_data_dr[EAC_data_dr["Service"] == i].reset_index(drop = True) # filters dataset to only include the service and the analysis month
            df_filt_service_only = EAC_data[EAC_data["Service"] == i].reset_index(drop = True) # filters dataset to only include the service
            
            if i in [j for j in EAC_services if ("P" in i) or ("N" in i)]: # reserve services
                
                reserve_volume_table = pd.pivot_table(df_filt, columns = "Fuel type", index = ["Start time", "Clearing price (£/MW/hr)"], values = "Executed Volume (MW)", aggfunc = "sum").reset_index()
                reserve_volume_table_sub = pd.pivot_table(df_filt, columns = "Fuel type", index = ["Start time", "Clearing price (£/MW/hr)"], values = "Volume (MW)", aggfunc = "sum").reset_index()
                reserve_volume_table_rej = (reserve_volume_table_sub - reserve_volume_table).mul(-1) # rejected volume
                
                #print(reserve_volume_table_sub)
                
                reserve_sub = pd.pivot_table(df_filt, index = "Start time", columns = "Fuel type", values = "Volume (MW)", aggfunc = "sum")
                reserve_wav = pd.pivot_table(df_filt, index = "Start time", columns = "Fuel type", values = "Submitted £/hr", aggfunc = "sum")
                reserve_wav = reserve_wav/reserve_sub #weighted average submitted price per SP per tech type
                reserve_wav.reset_index(inplace = True)
                if Load == True:
                    row = 0
                    col = 0
                    clear_col = len(reserve_volume_table_sub.columns.tolist()) + len(reserve_volume_table.columns.tolist()) + len(reserve_wav.columns.tolist()) + 3
                    # clear_col = num_to_col(clear_col)
                    # print(clear_col)
                    clear_range = f"A:{num_to_col(clear_col)}"
                    Excel_load(i, reserve_volume_table_sub.set_index("Start time"), "A1", name = f"Submitted volume {i} ({month_str})", clear_range = clear_range)
                    col = len(reserve_volume_table_sub.columns.tolist()) + 1
                    Excel_load(i, reserve_volume_table.set_index("Start time"), (row, col), name = f"Accepted volume {i} ({month_str})")
                    col = col + len(reserve_volume_table.columns.tolist()) + 1
                    Excel_load(i, reserve_wav.set_index("Start time"), (row, col), name = f"Weighted average submitted price {i} ({month_str})")
                    
            elif i in [j for j in EAC_services if "D" in i]: # response services
                # EFA block start hours during British Summer Time and UTC    
                
                df_filt["EFA"] = df_filt["Start time"].dt.hour.map(hour_to_EFA_dict)
                df_filt_service_only["EFA"] = df_filt_service_only["Start time"].dt.hour.map(hour_to_EFA_dict)

                
                response_volume_table = df_filt.groupby("Start time")[["Volume (MW)", "Executed Volume (MW)"]].sum()
                response_volume_table["Acceptance rate"] = response_volume_table["Executed Volume (MW)"].div(response_volume_table["Volume (MW)"])
                response_volume_table["Clearing price"] = df_filt.groupby("Start time")["Clearing price (£/MW/hr)"].mean()
                response_volume_table["sum of submitted £/hr"] = df_filt.groupby("Start time")["Submitted £/hr"].sum()
                response_volume_table["Weighted average submitted price"] = response_volume_table["sum of submitted £/hr"].div(response_volume_table["Volume (MW)"])
                # below is only used to get the Excel graphs to show the right thing, it isn't technically the submitted MW
                response_volume_table["Submitted MW"] = response_volume_table["Volume (MW)"] - response_volume_table["Executed Volume (MW)"]
                
                """=======================================================================================================
                Response summary tables
                =========================================================================================================="""
                
                # accepted volume by EFA by month
                acc_vol_EFA_month =  pd.pivot_table(df_filt_service_only, values = "Executed Volume (MW)", index = ["Date", "Month start"], columns = "EFA", aggfunc = "sum").reset_index()
                sub_vol_EFA_month =  pd.pivot_table(df_filt_service_only, values = "Volume (MW)", index = ["Date", "Month start"], columns = "EFA", aggfunc = "sum").reset_index()
                #print(acc_vol_EFA_month)
                
                acc_vol_EFA_month = acc_vol_EFA_month.groupby("Month start")[acc_vol_EFA_month.columns.tolist()[2:]].mean()
                sub_vol_EFA_month = sub_vol_EFA_month.groupby("Month start")[sub_vol_EFA_month.columns.tolist()[2:]].mean()
                
                acc_vol_EFA_month_change = (acc_vol_EFA_month/acc_vol_EFA_month.shift(1) - 1).tail(1)
                sub_vol_EFA_month_change = (sub_vol_EFA_month/sub_vol_EFA_month.shift(1) - 1).tail(1)
                
                
                index = ["Average accepted volume per EFA block", "m-o-m change", "Average submitted volume per EFA block", "m-o-m change"]
                summary_table_vol = pd.DataFrame([acc_vol_EFA_month.tail(1).values[0], acc_vol_EFA_month_change.values[0], sub_vol_EFA_month.tail(1).values[0], sub_vol_EFA_month_change.values[0]], 
                                                 columns = [1, 2, 3, 4, 5, 6], index = index)
                
                """Vol summary table is done"""
                # print(summary_table_vol)
                clearing_price_month =  pd.pivot_table(df_filt_service_only, values = "Clearing price (£/MW/hr)", index = ["Date", "Month start"], columns = "EFA").reset_index()
                # print(clearing_price_month.columns.tolist()[2:])
                cols = clearing_price_month.columns.tolist()[2:]
                
                clearing_price_month_max = clearing_price_month.groupby("Month start")[cols].max()
                clearing_price_month_max_change = (clearing_price_month_max/clearing_price_month_max.shift(1) - 1).tail(1)
                
                clearing_price_month_av = clearing_price_month.groupby("Month start")[cols].mean()
                clearing_price_month_av_change = (clearing_price_month_av/clearing_price_month_av.shift(1) - 1).tail(1)
                
                clearing_price_month_min = clearing_price_month.groupby("Month start")[cols].min()
                clearing_price_month_min_change = (clearing_price_month_min/clearing_price_month_min.shift(1) - 1).tail(1)
                
                
                price_index = ["Max clearing price", "m-o-m change", "Average clearing price", "m-o-m change", "Min clearing price", "m-o-m change"]
                summary_table_price = pd.DataFrame([clearing_price_month_max.tail(1).values[0], clearing_price_month_max_change.values[0],
                                                  clearing_price_month_av.tail(1).values[0], clearing_price_month_av_change.values[0], 
                                                  clearing_price_month_min.tail(1).values[0], clearing_price_month_min_change.values[0]], index = price_index, 
                                                  columns = [1, 2, 3, 4, 5, 6])
                
                if Load == True:
                    clear_range = f"A:{num_to_col(len(response_volume_table.columns.tolist()) + 2 + len(summary_table_price) + 1)}"
                    # print(clear_range)
                    
                    Excel_load(i, response_volume_table, "A1", f"Volumes in {i} ({month_str})", clear_range = clear_range)
                    #add_chart(i, "A1", 500, 10, 'line')
                    col = len(response_volume_table.columns.tolist()) + 2
                    Excel_load(i, summary_table_price, (0, col), "Average prices")
                    row = len(summary_table_price.index) + 2
                    
                    Excel_load(i, summary_table_vol, (row, col), "Average volumes") 
                    
            else:
                raise TypeError(f"{i} not a type of service on the EAC. Please enter a service in {EAC_services}")
        """=======================================================================================================
        Clearing price graph tables
        =========================================================================================================="""
        # print(EAC_data_dr)
        if Load == True: # only here so that it doesn't run the API each time as that's slow
            clearing_prices = pd.pivot_table(EAC_data_dr, values = "Clearing price (£/MW/hr)", columns = "Service", index = "Start time")
    
    
            DA_renewable_forecast = DA_Renewable_Generation_Forecast(date_from, date_to).reset_index()
            DA_demand_forecast = DA_Demand_Forecast(date_from, date_to)
            DA_demand_forecast["Start time"] = DA_demand_forecast["Start time"].dt.tz_localize(None) # removes BST from time
            DA_renewable_forecast["Start time"] = DA_renewable_forecast["Start time"].dt.tz_localize(None) 
    
            DA_demand_forecast = pd.merge(DA_demand_forecast, DA_renewable_forecast, on = "Start time", how = "outer")
            DA_demand_forecast["% of forecast demand from forecast renewables"] = DA_demand_forecast["Total forecast renewable generation"].div(DA_demand_forecast["Transmission demand (MW)"])
    
            clearing_prices = pd.merge(clearing_prices, DA_demand_forecast[["Start time", "Transmission demand (MW)", "Total forecast renewable generation", "% of forecast demand from forecast renewables"]], on = "Start time", how = "outer")
            clearing_prices = clearing_prices.sort_values(by = "Start time")
            clearing_prices = clearing_prices.ffill()
            
            clear_range = f"A:{num_to_col(len(clearing_prices.columns.tolist()))}"
            print(clear_range)
            Excel_load("Clearing price graph", clearing_prices.set_index("Start time"), "A1", "Clearing prices", clear_range)
            col = len(clearing_prices.columns.tolist()) + 2
            Excel_load("Clearing price graph", EAC_price_summary_mean, (0, col), name = "Average prices by service")
            row = len(EAC_price_summary_mean.index) + 3
            Excel_load("Clearing price graph", EAC_price_summary_std, (row, col), name = "Average price volatility by service")
            col = len(EAC_price_summary_mean.columns.tolist()) + col + 3
            Excel_load("Clearing price graph", EAC_acc_vol_summary_mean, (0, col), name = "Average accepted volume by service")
            Excel_load("Clearing price graph", EAC_sub_vol_summary_mean, (row, col), name = "Average submitted volume by service")
            
            
        else:
            pass
    
    if STOR == True:
        # loads STOR data
        STOR_data = Data_load("STOR_data", date_from, date_to)
        STOR_data["Date"] = pd.to_datetime(STOR_data["Start time"].dt.date)
        STOR_data["Month"] = STOR_data["Date"].dt.strftime("%b-%y")
        STOR_data["Month start"] = pd.to_datetime(STOR_data["Month"], format = "%b-%y")
        
        STOR_data = STOR_data[(STOR_data["Date"] >= date_from_prev_dt) & (STOR_data["Date"] <= date_to)].reset_index(drop = True)
        
        if BM == False:
            STOR_data_BM = Data_load("DSP_data", date_from, date_to)
        else:
            STOR_data_BM = DSP_data
        
        print("Analysing STOR data...")
        STOR_data_BM = STOR_data_BM[(STOR_data_BM["Date"] >= date_from_dt) & 
                                    (STOR_data_BM["Date"] <= date_to_dt) & 
                                    (STOR_data_BM["STOR Flag"] == "T")].reset_index(drop = True)
        
        STOR_utilisation_data = STOR_data_BM.groupby("Date").agg({"Volume (MWh)": "sum", 
                                                                  "Price (£/MWh)": "mean"})

        STOR_utilisation_data_mom = STOR_data_BM.groupby("Month").agg({"Volume (MWh)": "sum", 
                                                                             "Price (£/MWh)": "mean"})
        

        
        STOR_data["Price*vol"] = STOR_data["Submitted MW"].mul(STOR_data["Availability price"])
        
        STOR_summary = STOR_data.groupby("Date").agg({"Submitted MW": "sum", "Accepted MW": "sum", 
                                                      "Availability price": "mean", "Clearing price": "mean"})
        
        STOR_summary["Weighted submitted average price"] = STOR_data.groupby("Date")["Price*vol"].sum().div(STOR_summary["Submitted MW"])
        STOR_summary["Utilised volume (MWh)"] = STOR_summary.index.map(STOR_utilisation_data.to_dict()["Volume (MWh)"])
        
        
        STOR_by_fuel = pd.pivot_table(STOR_data, values = "Accepted MW", index = "Date", columns = "Fuel type", aggfunc = "sum")
        
        STOR_assets = pd.DataFrame({"Number of active assets": [len(STOR_data[STOR_data["Month"] == month_str_prev]["NGU ID"].unique().tolist()), 
                                                                len(STOR_data[STOR_data["Month"] == month_str]["NGU ID"].unique().tolist())]}, 
                                   index = [month_str_prev, month_str])
        
        if Load == True:
            col = len(STOR_summary.columns.tolist()) + 1
            col2 = len(STOR_by_fuel.columns.tolist()) + 1
            col3 = len(STOR_utilisation_data.columns.tolist()) + 1
            clear_range = f"A:{num_to_col(col + col2 + col3)}"
            Excel_load("STOR", STOR_summary, "A1", name = "STOR Summary", clear_range = clear_range)
            Excel_load("STOR", STOR_by_fuel, (0, col + 1), name = "Accepted STOR volumes by fuel type")
            Excel_load("STOR", STOR_utilisation_data, (0, col + col2 + 2), name = "STOR Utilisation data")
            Excel_load("STOR", STOR_assets, (0, col + col2 + col3 + 3))
            
    if SFFR == True:
        
        SFFR_data = Data_load("SFFR_data", date_from, date_to)
        print("Analysing SFFR data...")
        SFFR_data["Start time"] = pd.to_datetime(SFFR_data["Start time"])
        SFFR_data["Date"] = SFFR_data["Start time"].dt.date
        SFFR_data["Month"] = SFFR_data["Start time"].dt.strftime("%b-%y")
        SFFR_data["Month start"] = pd.to_datetime(SFFR_data["Month"], format = "%b-%y")
        SFFR_data["price*vol"] = SFFR_data["Submitted MW"].mul(SFFR_data["Submitted price (£/MW/hr)"])
        
        
        SFFR_data = SFFR_data[(SFFR_data["Start time"] >= date_from_prev_dt) & (SFFR_data["Start time"] <= date_to_dt)].reset_index(drop = True)
        

        SFFR_vol_summary_table_fuel = pd.pivot_table(SFFR_data, values = "Accepted MW", index = "Month start", columns = "Fuel type", aggfunc = "sum", margins = True, margins_name = "Total MW").drop("Total MW")
        SFFR_vol_summary_table_period = SFFR_data.groupby(["Date", "Month start"]).agg({"Accepted MW": "sum", 
                                                                                              "Submitted MW": "sum",
                                                                                              "Clearing price": "mean", 
                                                                                              "price*vol": "sum"})
        SFFR_vol_summary_table_period["% of MW accepted"] = SFFR_vol_summary_table_period["Accepted MW"].div(SFFR_vol_summary_table_period ["Submitted MW"])
        SFFR_vol_summary_table_period["Weighted average price"] = SFFR_vol_summary_table_period["price*vol"].div(SFFR_vol_summary_table_period["Submitted MW"])
        SFFR_vol_summary_table_period.drop("price*vol", axis = 1, inplace = True)
        SFFR_vol_summary_table_period.rename(columns = {"Submitted MW": "Submitted Volume (MW)"}, inplace = True)
        SFFR_vol_summary_table_period["Submitted MW"] = SFFR_vol_summary_table_period["Submitted Volume (MW)"] - SFFR_vol_summary_table_period["Accepted MW"]
        
        SFFR_assets = pd.DataFrame({"Number of active assets": [len(SFFR_data[SFFR_data["Month"] == month_str_prev]["NGU ID"].unique().tolist()), 
                                                                len(SFFR_data[SFFR_data["Month"] == month_str]["NGU ID"].unique().tolist())]}, 
                                   index = [date_from_prev, date_from])

        if Load == True:
            Excel_load("SFFR", SFFR_vol_summary_table_period, "A1", name = "SFFR summary table", clear_range = f"A:{num_to_col(len(SFFR_vol_summary_table_period.columns.tolist()) + 5)}")
            Excel_load("SFFR", SFFR_assets, (0, len(SFFR_vol_summary_table_period.columns.tolist()) + 3))
        else:
            pass
        
    if kW_revenue == True:       
        
        def kW_revenue_by_service(DSP_data, EAC_data, STOR_data, SFFR_data):
            
            """This finds the maximum submitted MW during the time period for each unit by service"""

            """STOR"""
            STOR_capacities = STOR_data.groupby(["Start time", "NGU ID"])["Submitted MW"].sum().reset_index()
            STOR_capacities = STOR_capacities.groupby("NGU ID")["Submitted MW"].max().to_dict()
            # print(STOR_capacities)
            
            STOR_availability_revenue = STOR_data.groupby("NGU ID")["Revenue"].sum()
            STOR_utilisation_data_revenue = DSP_data[DSP_data["STOR Flag"] == "T"].reset_index(drop = True)
            STOR_utilisation_data_revenue = STOR_utilisation_data_revenue.groupby(["NGU ID"])["Revenue"].sum()

            """SFFR"""
            SFFR_capacities = SFFR_data.groupby(["Start time", "NGU ID"])["Submitted MW"].sum().reset_index()
            SFFR_capacities = SFFR_data.groupby("NGU ID")["Submitted MW"].max().to_dict()
            
            SFFR_revenue = SFFR_data.groupby("NGU ID")["Revenue"].sum()
            
            """EAC services"""
            EAC_capacities = EAC_data[["Basket ID", "NGU ID", "Executed Volume (MW)", "Service"]]
            EAC_capacities = EAC_capacities.groupby(["Basket ID", "NGU ID", "Service"], as_index = False)["Executed Volume (MW)"].sum()
            # EAC_capacities = EAC_capacities.groupby(["NGU ID", "Service"], as_index = False)["Executed Volume (MW)"].max()
            EAC_capacities = pd.pivot_table(EAC_capacities, values = "Executed Volume (MW)", index = "NGU ID", columns = "Service", aggfunc = "max")
            
            EAC_revenue = pd.pivot_table(EAC_data, index = "NGU ID", values = "Revenue", columns = "Service", aggfunc = "sum")
            # print(EAC_revenue)
            
            # print(EAC_capacities)
            
            """BM"""
            # use submitted bids/offer data as the MW reference
            """This method doesn't work for batteries without including their FPN, as they can offer to bid/offer
            at a level higher than their capacity because they can pass through 0MW"""
            """
            # removes uncompetitively priced offers
            # need to separate it out by bids and offers as the reversal prices would be included too otherwse
            offers = BOD_data[(BOD_data["Pair ID"] > 0) & (BOD_data["Offer price"].abs() < 1500)]
            bids = BOD_data[(BOD_data["Pair ID"] < 0) & (BOD_data["Offer price"].abs() < 1500)]
            
            BOD_data = pd.concat([offers, bids]).reset_index(drop = True)
            
            BOD_data["Order type"] = "Offer"
            BOD_data["Order type"] = BOD_data["Order type"].where(BOD_data["Pair ID"] > 0, "Bid")
            BOD_data = BOD_data.sort_values(by = ["Order type", "Time from", "NGU ID"]).reset_index(drop = True)
            BOD_data["Cumulative capacity"] = BOD_data.groupby(["Order type", "Date", "SP", "BMU ID"])["MW to"].cumsum().abs()
            print(BOD_data)
            BOD_data.to_csv("test1.csv", index = False)
            """
            
            
            BM_capacities = BMU_capacity_dict_by_NGUID            
            
            DSP_data = DSP_data[DSP_data["STOR Flag"] == "F"].reset_index(drop = True)
            BM_revenue = DSP_data.groupby(["NGU ID"])["Revenue"].sum()
            
            units = sorted(list(set(list(STOR_capacities.keys()) + list(SFFR_capacities.keys())
                                    + EAC_capacities.index.values.tolist() + list(map(str, list(BM_capacities.keys())))
                                    )))
            
            capacity_df = pd.DataFrame({"NGU ID": units})
            revenue_df = pd.DataFrame({"NGU ID": units})
            
            capacity_df["BM"] = capacity_df["NGU ID"].map(BM_capacities)
            revenue_df["BM"] = revenue_df["NGU ID"].map(BM_revenue.to_dict())
            
            # adds capacity/revenue by EAC services into the capacity_df/revenue_df
            for i in EAC_capacities.columns.tolist():
                print(i)
                dict_temp_cap = EAC_capacities.to_dict()[i]
                dict_temp_rev = EAC_revenue.to_dict()[i]
                capacity_df[f"{i}"] = capacity_df["NGU ID"].map(dict_temp_cap)
                revenue_df[f"{i}"] = revenue_df["NGU ID"].map(dict_temp_rev)
            
            capacity_df["STOR"] = capacity_df["NGU ID"].map(STOR_capacities)
            revenue_df["STOR"] = (revenue_df["NGU ID"].map(STOR_availability_revenue.to_dict())) + revenue_df["NGU ID"].map(STOR_utilisation_data_revenue.to_dict())
            
            capacity_df["SFFR"] = capacity_df["NGU ID"].map(SFFR_capacities)
            revenue_df["SFFR"] = revenue_df["NGU ID"].map(SFFR_revenue.to_dict())
            
            
            capacity_df.set_index("NGU ID", inplace = True)
            revenue_df.set_index("NGU ID", inplace = True)
            
            # gets it into £/kW
            capacity_df = capacity_df.mul(1000)
            kW_revenue = revenue_df.div(capacity_df)
            kW_revenue["Total"] = kW_revenue.sum(axis = 1)
            
            # adds company and fuel type
            # neither company dicts pick up all assets on their own
            
            for i in [kW_revenue, capacity_df, revenue_df]:
            
                i["Company"] = i.index.map(NGU_company_dict2)
                i["Company"] = i.index.map(NGU_company_dict1).where(i["Company"].isna(), i["Company"])
                
                # once again, neither fuel type dicts get all assets by themselves
                i["Fuel type"] = i.index.map(NGUID_fuel_type_dict2)
                i["Fuel type"] = i.index.map(NGUID_fuel_type_dict1).where(i["Fuel type"].isna(), i["Fuel type"])
            
            # kW_revenue["BMU ID"] = kW_revenue.index.map(NGUID_BMUID_dict) 
            
            kW_revenue = kW_revenue.sort_values(by = "Total", ascending = False)
            
            kW_revenue = kW_revenue[kW_revenue["Total"] != 0] # removes those with £0/kW across all services
            
            # print(kW_revenue)
            
            return kW_revenue, capacity_df, revenue_df
        
        if EAC == False:
            # print("Loading EAC data...")
            EAC_data = Data_load("EAC_data", date_from = date_from, date_to = date_to)
            EAC_data["Start time"] = pd.to_datetime(EAC_data["Start time"])
            EAC_data["End time"] = pd.to_datetime(EAC_data["End time"])
            
        if STOR == False:
            # print("Loading STOR data...")
            STOR_data = Data_load("STOR_data", date_from = date_from, date_to = date_to)
            # print(STOR_data)
            
        if SFFR == False:
            # print("Loading SFFR data...")
            SFFR_data = Data_load("SFFR_data", date_from = date_from, date_to = date_to)
            # print(SFFR_data)
        
        if BM == False:
            DSP_data = Data_load("DSP_data", date_from = date_from, date_to = date_to)
            DSP_data["Date"] = pd.to_datetime(DSP_data["Date"])
            DSP_data = DSP_data[(DSP_data["Date"] >= date_from_dt) & (DSP_data["Date"] <= date_to_dt)].reset_index(drop = True)
        
        # loads submitted bid/offer data
        # BOD_data = Data_load("BOD_data", date_from = date_from, date_to = date_to)
        print("Calculating £/kW revenues...")
        # gets data so it's only in the time range
        STOR_data = STOR_data[(STOR_data["Start time"] >= date_from_dt) & (STOR_data["Start time"] <= date_to_dt + relativedelta(days = 1))]
        STOR_data["Revenue"] = STOR_data["Clearing price"].mul(STOR_data["Accepted MW"])*24
        #print(STOR_data)
        SFFR_data = SFFR_data[(SFFR_data["Start time"] >= date_from_dt) & (SFFR_data["Start time"] <= date_to_dt + relativedelta(days = 1))]
        SFFR_data["Revenue"] = SFFR_data["Clearing price"].mul(SFFR_data["Accepted MW"])*4
        #print(SFFR_data)
        EAC_data = EAC_data[(EAC_data["Start time"] >= date_from_dt) & (EAC_data["Start time"] <= date_to_dt + relativedelta(days = 1))]
        EAC_data["dt"] = (EAC_data["End time"] - EAC_data["Start time"]).dt.total_seconds()/3600
        EAC_data["Revenue"] = EAC_data["Executed Volume (MW)"].mul(EAC_data["Clearing price (£/MW/hr)"]).mul(EAC_data["dt"])
        
        DSP_data["Revenue"] = DSP_data["Volume (MWh)"].mul(DSP_data["Price (£/MWh)"])
        
        group_columns = ["Start time", "NGU ID", "Company", "Service", "Submitted price (£/MW/hr)"]
        
        bidding_data = EAC_data.groupby(group_columns).agg(sub_MW = ("Volume (MW)", "sum"), 
                                                           acc_MW = ("Executed Volume (MW)", "sum"),
                                                           av_price = ("Submitted price (£/MW/hr)", "mean"),
                                                           revenue = ("Revenue", "sum")).reset_index()
        
        company_bidding_strategies_sub = pd.pivot_table(bidding_data, values = "sub_MW", index = "Company", columns = "Service", aggfunc = "sum")
        company_bidding_strategies_acc = pd.pivot_table(bidding_data, values = "acc_MW", index = "Company", columns = "Service", aggfunc = "sum")
        company_bidding_strategies_price = pd.pivot_table(bidding_data, values = "av_price", index = "Company", columns = "Service", aggfunc = "mean")
        
        
        if Load == True:
            col = 2*len(company_bidding_strategies_acc.columns.tolist())
            Excel_load("Company bidding", company_bidding_strategies_acc, "A1", "Accepted volume during period", f"A:{num_to_col(col + 1)}")
            row = len(company_bidding_strategies_acc.index) + 2
            Excel_load("Company bidding", company_bidding_strategies_sub, (row, 0), "Submitted volume during period")
            Excel_load("Company bidding", company_bidding_strategies_price, (0, (int(col/2)) + 3), "Average submitted prices during period")
            
        sys.exit()
        
        
        
        kW_revenue, capacity_by_service, revenue_by_service = kW_revenue_by_service(DSP_data, EAC_data, STOR_data, SFFR_data)

        non_flex_fuel_types = ["CCGT", "Wind", "Biomass", "NPSHYD", "INTELEC", "INTEW", "INTFR", "INTIFA2", "INTIRL",
                               "INTNED", "INTNEM", "INTNSL", "INTVKL", "COAL", "Non-PS Hydro", "Nuclear", "Coal"]
        
        # removes non-flex fuel types from the dataframe
        kW_revenue = kW_revenue[~kW_revenue["Fuel type"].isin(non_flex_fuel_types)]
        capacity_by_service = capacity_by_service[~capacity_by_service["Fuel type"].isin(non_flex_fuel_types)]
        revenue_by_service = revenue_by_service[~revenue_by_service["Fuel type"].isin(non_flex_fuel_types)]
        
        capacity_by_service["Capacity"] = capacity_by_service[capacity_by_service.columns.tolist()[:-2]].max(axis = 1)      
        
        """This bit sets the capacity of units which made money in the BM but have a registered capacity in the BM
        of 0MW to their max capacity in other services"""
        
        # sets NA values in the BM column to 0 to make the filter easier
        revenue_by_service["BM"] = revenue_by_service["BM"].where(~revenue_by_service["BM"].isna(), 0)
        capacity_by_service["BM"] = capacity_by_service["BM"].where(~capacity_by_service["BM"].isna(), 0)
        
        assets_with_BM_revenue = revenue_by_service[revenue_by_service["BM"] != 0].index.tolist()
        assets_with_no_BM_capacity =  capacity_by_service[(capacity_by_service["BM"] == 0) & (capacity_by_service["Capacity"] != 0)].index.tolist()

        # gets list of assets with BM revenue but have a capacity listed from other services
        assets_to_amend_BM = list(set(assets_with_BM_revenue).intersection(assets_with_no_BM_capacity))
        assets_to_amend_BM_MW = capacity_by_service[capacity_by_service.index.isin(assets_to_amend_BM)].to_dict()["Capacity"]

        capacity_by_service["BM"] = capacity_by_service["BM"].where(~capacity_by_service.index.isin(assets_to_amend_BM), capacity_by_service.index.map(assets_to_amend_BM_MW))
        
        
        """This bit estimates capacities from units which have 0MW capacity across all services yet made money in 
        the BM, leading to infinite £/kW revenue
        
        Please note, this is a temporary fix due to time constraints. A better approach would be to look at
        FPNs over the time period or include the DC values
        
        Note: a better way of calculating the £/kW values would include plant outages and changing MW values from
        the BM register"""
        
        # only care about ones which have 0MW as a capacity as these cause the DIV0 errors
        zero_capacities = capacity_by_service[capacity_by_service["Capacity"] == 0]
        zero_capacities = zero_capacities.index.tolist()
        
        # gets units which have 0MW capacity but also make money in the BM
        BM_problem_childs = revenue_by_service[(revenue_by_service["BM"] != 0) & (revenue_by_service.index.isin(zero_capacities))]
        
        BM_problem_childs = BM_problem_childs.index.tolist()

        BM_problem_child_data = DSP_data[DSP_data["NGU ID"].isin(BM_problem_childs)]
        BM_problem_child_data = BM_problem_child_data.groupby("NGU ID").agg(Capacity = ("Volume ABS", "max"))
        

        BM_problem_child_data["Capacity"] = BM_problem_child_data["Capacity"]*2000 # gets MWh figure into estimated max capacity in kW
        # if anything is less than 1MW (which needs to be the case for the BM, sets it equal to 1)
        BM_problem_child_data["Capacity"] = BM_problem_child_data["Capacity"].astype(int).where(BM_problem_child_data["Capacity"] >= 1000, 1000) 
        BM_problem_child_approx_kW = BM_problem_child_data["Capacity"].to_dict()
        
        # updates capacity in the capacity-by-service-df with the approximated MW values, but only if the current capacity value is 0
        capacity_by_service["Capacity"] = capacity_by_service["Capacity"].map(BM_problem_child_approx_kW).where((capacity_by_service.index.isin(BM_problem_child_approx_kW)), capacity_by_service["Capacity"])
        capacity_by_service["BM"] =  capacity_by_service["BM"].map(BM_problem_child_approx_kW).where((capacity_by_service.index.isin(BM_problem_child_approx_kW)), capacity_by_service["BM"])
        
        
        revenue_by_service["Total £"] = revenue_by_service[revenue_by_service.columns.tolist()[:-2]].sum(axis = 1)
        
        """Company revenue analysis"""
        
        # total revenue during period
        company_revenue = revenue_by_service.groupby("Company")[revenue_by_service.columns.tolist()[:-3]].sum()
        company_revenue["Total £"] = company_revenue.sum(axis = 1)
        
        # total capacity by company during period
        company_capacity = capacity_by_service.groupby("Company")["Capacity"].sum()
        company_capacity_dict = company_capacity.to_dict()
        
        company_revenue["Capacity"] = company_revenue.index.map(company_capacity_dict)
        company_revenue["£/kW"] = company_revenue["Total £"].div(company_revenue["Capacity"])
        company_revenue = company_revenue.sort_values(by = "£/kW", ascending = False)
        
        # print(company_revenue)
        
        """Asset revenue"""
        revenue_by_service = revenue_by_service.reset_index().set_index(["NGU ID", "Company", "Fuel type"])
        capacity_by_service = capacity_by_service.reset_index().set_index(["NGU ID", "Company", "Fuel type"])
        asset_kW_revenue = revenue_by_service.div(capacity_by_service) # does all but the total column
        
        del asset_kW_revenue["Capacity"]
        
        # these are used to get the total £/kW revenue column in the asset £/kW table
        total_rev_map = revenue_by_service.to_dict()["Total £"]
        capacity_map = capacity_by_service.to_dict()["Capacity"] 
        
        asset_kW_revenue["Total £"] = asset_kW_revenue.index.map(total_rev_map)
        asset_kW_revenue["Max capacity during period"] = asset_kW_revenue.index.map(capacity_map)
        asset_kW_revenue["Total £/kW"] = asset_kW_revenue["Total £"].div(asset_kW_revenue["Max capacity during period"])
        
        asset_kW_revenue = asset_kW_revenue.sort_values(by = "Total £/kW", ascending = False)
        value_dict = asset_kW_revenue.to_dict()["Total £/kW"]
        """
        test = revenue_by_service.reset_index()
        test = test[test["NGU ID"] == "ZEN02A"]
        
        test_c = capacity_by_service.reset_index()
        test_c = test_c[test_c["NGU ID"] == "ZEN02A"]"""

        top_revenue_by_service = revenue_by_service[revenue_by_service.index.isin(asset_kW_revenue.head(20).index.tolist())]
        top_revenue_by_service["Total £/kW value"] = top_revenue_by_service.index.map(value_dict)
        top_revenue_by_service = top_revenue_by_service.sort_values("Total £/kW value", ascending = False)
        
        
        if Load == True:
            Excel_load("Company Revenue", company_revenue, "A1", name = "Revenue by company", clear_range = f"A:{num_to_col(len(company_revenue.columns.tolist()) + 3)}")
            Excel_load("Asset Revenue", revenue_by_service, "A1", name = "Revenue by flex asset", clear_range = "A:BA")
            col = len(revenue_by_service.columns.tolist()) + 5
            Excel_load("Asset Revenue", capacity_by_service, (0, col), name = "Entered capacity by service during date range")
            col = col + len(capacity_by_service.columns.tolist()) + 5
            Excel_load("Asset Revenue", asset_kW_revenue, (0, col), name = "£/kW revenue by flex asset")
            Excel_load("Top 20 Assets", asset_kW_revenue.head(20), "A1", name = "£/kW revenue by flex asset")
            Excel_load("Top 20 Assets NEW", top_revenue_by_service, "A1", name = "£/kW revenue by flex asset")

    print(f"Code finished in: {datetime.now() - start_time}")
else:
    pass
    
