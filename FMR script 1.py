# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 16:26:32 2024

@author: Tim.Sowinski
"""
import sys
import pyodbc
import pandas as pd
import xlwings as xw

"""This will be a script to pull all the data for the FMR and do all the analysis"""

"""==========================================================================================================
SETTINGS
============================================================================================================="""

Excel_workbook_name = False # if False, will create a new workbook, if a string will place it into this file
"""No wait I want to get it to create a new workbook instead of already having one"""


date_from: str = "2024-09-01" # date to begin analysis
date_to: str = "2024-09-02" # date to end analysis

# set these to true if you want the analysis to be done

BM: bool = True
EAC: bool = True
BR: bool = True
STOR: bool = True
SFFR: bool = True

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.reset_option('display.max_rows')

class SQL_query: # creates SQL query class
    def __init__(self):
        server = "tcp:mssql-01.citg.one"
        database = "CI_DL1"
        connection_string = "DRIVER={SQL Server};SERVER="+server+";DATABASE="+database+";Trusted_Connection=yes"
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor 
    
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
        df = df[column_renames.values()]
        
        return df
    
    def BOA_data(self, date_from: str, date_to: str):
        pass
    
    def DSP_data(self, date_from: str, date_to: str):
        pass
    
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
    if BM == True:
        # Submitted bid and offer data
        BOD_data = SQL_query().BOD_data(date_from, date_to)
        print(BOD_data)
    
else:
    pass
    

