# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 16:26:32 2024

@author: Tim.Sowinski
"""
import sys
import pyodbc
import pandas as pd
import xlwings as xw
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

"""This will be a script to pull all the data for the FMR and do all the analysis"""

"""==========================================================================================================
SETTINGS
============================================================================================================="""

Excel_workbook_name = "FMR Analysis test.xlsx" 

date_from: str = "2024-09-01" # date to begin analysis
date_to: str = "2024-09-30" # date to end analysis

# set these to true if you want the analysis to be done

BM: bool = True
EAC: bool = True
BR: bool = True
STOR: bool = True
SFFR: bool = True

# set Export to True if you want the SQL queries to be exported as a csv file (will make it quicker for rerunning the code in future)

Export = False
Load = False

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.reset_option('display.max_rows')

start_time = datetime.now()

class SQL_query: # creates SQL query class
    def __init__(self):
        server = "tcp:mssql-01.citg.one"
        database = "CI_DL1"
        connection_string = "DRIVER={SQL Server};SERVER="+server+";DATABASE="+database+";Trusted_Connection=yes"
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor 
    
    def BMU_data(self):
        query_string = f"""
        SELECT Elexon_BMUnitID, NGC_BMUnitID, PartyName, GSPGroup, ReportName, BMU.FuelTypeID

        FROM Meta.tblBMUnit_Managed as BMU

        INNER JOIN Meta.tblFuelType as ft ON ft.FuelTypeID = BMU.FuelTypeID
        """
        print("\nGathering asset information data from SQL server")
        df = pd.read_sql_query(query_string, self.connection)
        
        column_renames = {"Elexon_BMUnitID": "BMU ID", "NGC_BMUnitID": "NGU ID", "PartyName": "Company",
                          "GSPGroup": "GSP Group", "ReportName": "Fuel type", "FuelTypeID": "Fuel type ID"}
        df = df.rename(columns = column_renames)
        
        
        
        return df
    
    def BOD_data(self, date_from: str, date_to: str):
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
        df = pd.read_sql_query(query_string, self.connection)
        #print("hello")
        
        column_renames = {"SettlementDate": "Date", "HHPeriod": "SP","TimeFromUTC": "Time from",
                          "TimeToUTC": "Time to", "Elexon_BMUnitID": "BMU ID", "NGC_BMUnitID": "NGU ID",
                          "ReportName": "Fuel type", "PartyName": "Company","LevelFrom": "MW from", 
                          "LevelTo": "MW to", "PairId": "Pair ID", "Bid": "Bid price", "Offer": "Offer price"}
        df.rename(columns = column_renames, inplace = True)
        print(df)
        
        df = df[column_renames.values()]
        
        return df
    
    def BOA_data(self, date_from: str, date_to: str):
        pass
    
    def DSP_data(self, date_from: str, date_to: str):
        
        query_string_old = f"""SELECT *
        FROM PowerSystem.tblDetailedSystemPrices as DSP
        
        INNER JOIN Meta.tblBMUnit_Managed as BMU ON BMU.Elexon_BMUnitID = DSP.ID
        INNER JOIN Meta.tblFuelType as ft ON BMU.FuelTypeID = ft.FuelTypeID
        
        WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'
        
        """
        
        print("Gathering submitted Detailed System Prices data from SQL server")
        df = pd.read_sql_query(query_string, self.connection)
        column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "ID": "BMU ID", "NGC_BMUnitID": "NGU ID",
                          "ReportName": "Fuel type", "PartyName": "Company","BidOfferPairId": "Pair ID", "CadlFlag": "CADL Flag",
                          "SoFlag": "SO Flag", "StorFlag": "STOR Flag", "Price": "Price (£/MWh)", "Volume": "Volume (MWh)"}
        df.rename(columns = column_renames, inplace = True)
        df = df[column_renames.values()]
        df["Date"] == pd.to_datetime(df["Date"])
        df = df.sort_values(by = ["Date", "SP"])
        
        return df
    
    def EAC_data(self, date_from: str, date_to: str):
        pass
    
    def BR_data(self, date_from: str, date_to: str):
        pass
    
    def STOR_data(self, date_from: str, date_to: str):
        pass
    
    def SFFR_data(self, date_from: str, date_to: str):
        pass

"""==========================================================================================================
Balancing Mechanism
============================================================================================================="""

if __name__ == "__main__":
    
    def load(sheet_name, data, cell_ref, name = False, clear_range = False): #if using coordinates, needs to be entered as a list like [(row1, col1), (row2, col2)]
        """For cell_ref coordinates, must be in the format [(row, col)]. For clear range coordinates, must be in the format [(row1, col1), (row2, col2)]"""     
        current_sheets = []
        for i in range(len(wb.sheets)):
            current_sheets.append(wb.sheets[i].name) #this returns a new list of just the sheet names
        
        #adds in sheets if it doesn't already exist
        if sheet_name not in current_sheets:
            wb.sheets.add("{}".format(sheet_name))
            current_sheets.append("{}".format(sheet_name))
        else:
            pass
        
        sheet = wb.sheets["{}".format(sheet_name)]
        
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
            else:
                #print("D")
                sheet.range("{}".format(clear_range)).clear_contents()
                sheet["{}".format(cell_ref)].value = data
                sheet["{}".format(cell_ref)].value = name
            
        else:
            if type(cell_ref) == list:
                sheet[cell_ref[0]].value = data
                sheet[cell_ref[0]].value = name
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
    
    """=======================================================================================================
    Datetime dates for the code
    =========================================================================================================="""
    
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d") # datetime of date_from
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") # datetime of date_to
    
    date_from_prev_dt = date_from_dt + relativedelta(months = -1, day = 1) # previous month start
    date_to_prev_dt = date_from_dt + relativedelta(days = -1) # previous month end
    
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
    
    BMU_data = SQL_query().BMU_data()
    
    if BM == True:
        """===================================================================================================
        Loads Detailed system prices
        ======================================================================================================"""
        file_name_DSP = "DSP data " + file_name_suffix
        if file_name_DSP not in csv_files:
            print("\nLoading detailed system prices from SQL server...")
            DSP_data = SQL_query().DSP_data(date_from, date_to)
            print("Exporting detailed system prices to csv...")
            DSP_data.to_csv(file_name_DSP, index = False)
        else:
            print("\nLoading detailed system prices from csv file...")
            DSP_data = pd.read_csv(os.getcwd() + "//" + file_name_DSP)
            
        print(DSP_data)
        
        
    
    
    
    print(f"Code finished in: {datetime.now() - start_time}")
else:
    pass
    
