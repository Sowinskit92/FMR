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
date_to: str = "2024-10-01" # date to end analysis

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
        
        query_string = f"""SELECT *
        FROM PowerSystem.tblDetailedSystemPrices as DSP
        
        WHERE SettlementDate >= '{date_from}' AND SettlementDate <= '{date_to}'
        
        """
        
        print("Gathering submitted Detailed System Prices data from SQL server")
        df = pd.read_sql_query(query_string, self.connection)
        column_renames = {"SettlementDate": "Date", "HHPeriod": "SP", "ID": "BMU ID", "BidOfferPairId": "Pair ID",
                          "CadlFlag": "CADL Flag", "SoFlag": "SO Flag", "StorFlag": "STOR Flag", 
                          "Price": "Price (£/MWh)", "Volume": "Volume (MWh)"}
        df.rename(columns = column_renames, inplace = True)
        df = df[column_renames.values()]
        df["Date"] == pd.to_datetime(df["Date"])
        df = df.sort_values(by = ["Date", "SP"]).reset_index(drop = True)
        
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
        BMU_data.to_csv("BMU info.csv", index = False)
    else:
        BMU_data = pd.read_csv("BMU info.csv")
    
    BMUID_fuel_type_dict = BMU_data.set_index("BMU ID")["Fuel type"].to_dict()
    BMUID_NGUID_dict = BMU_data.set_index("BMU ID")["NGU ID"].to_dict()
    NGUID_BMUID_dict = BMU_data.set_index("NGU ID")["BMU ID"].to_dict()
    NGUID_fuel_type_dict = BMU_data.set_index("NGU ID")["Fuel type"].to_dict()
    
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
            DSP_data["Month"] = DSP_data["Date"].dt.strftime("%b-%y")
            DSP_data["Volume ABS"] = DSP_data["Volume (MWh)"].abs()
            DSP_data["Order type"] = "Offer"
            DSP_data["Order type"] = DSP_data["Order type"].where(DSP_data["Pair ID"] > 0, "Bid")
            DSP_data["Energy/System"] = "System"
            DSP_data["Energy/System"] = DSP_data["Energy/System"].where(DSP_data["SO Flag"] == "T", "Energy")
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
                DSP_data = pd.concat([DSP_data, DSP_new]).reset_index(drop = True)
                DSP_data["Month"] = DSP_data["Date"].dt.strftime("%b-%y")
                DSP_data["Volume ABS"] = DSP_data["Volume (MWh)"].abs()
                DSP_data["Order type"] = "Offer"
                DSP_data["Order type"] = DSP_data["Order type"].where(DSP_data["Pair ID"] > 0, "Bid")
                DSP_data["Energy/System"] = "System"
                DSP_data["Energy/System"] = DSP_data["Energy/System"].where(DSP_data["SO Flag"] == "T", "Energy")
                
                DSP_data.to_csv(file_name_DSP, index = False)
            else:
                pass

        """===================================================================================================
        Begins BM Analysis
        ======================================================================================================"""
        
        BM_techs = DSP_data["Fuel type"].unique().tolist()
        BM_techs = sorted([str(i) for i in BM_techs if str(i) != 'nan']) # sorted list of technologies
        
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
            load("BM Daily Dispatches", dispatch_graph, "A1", "Daily dispatches", "A:G")
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
            load("BM Volume share", BM_volume_share, "A1", "Volume share", "A:O")
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
            load("Battery BM spreads", battery_offers, (row, col), name = "Battery offers", clear_range = [(1, 1), (len(battery_offers.index) + 1, 2*len(battery_offers.columns.tolist()) + 1)])
            col = len(battery_offers.columns.tolist()) + 2
            load("Battery BM spreads", battery_bids, (row, col), name = "Battery bids")
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
            load("Monthly dispatches", total_dispatch, "A1", "Monthly dispatches", "A:G")
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
                
            return df, df_prev, change_df
        
        if Load == True:
            print("Calculating BOAs by tech type...")
            row = 0
            col = 0
            
            load("BOA Technology Breakdown", BM_tech_breakdown("Volume")[0], (row, col), name = "Total volume", clear_range = "A:Q")
            row = len(BM_tech_breakdown("Volume")[0].index) + 2
            load("BOA Technology Breakdown", BM_tech_breakdown("Volume")[1], (row, col), name = "Total volume")

            row = row + len(BM_tech_breakdown("Volume")[1].index) + 2
            load("BOA Technology Breakdown", BM_tech_breakdown("Volume")[2], (row, col), name = "Change in volume")
            
            col = len(BM_tech_breakdown("Volume")[0].columns.tolist()) + 2
            load("BOA Technology Breakdown", BM_tech_breakdown("Count")[0], (0, col), name = "Total count")
            load("BOA Technology Breakdown", BM_tech_breakdown("Count")[1], (row, col), name = "Total count")
            
            col = col + len(BM_tech_breakdown("Count")[0].columns.tolist()) + 2
            load("BOA Technology Breakdown", BM_tech_breakdown("Price")[0], (0, col), name = "Average price")
            load("BOA Technology Breakdown", BM_tech_breakdown("Price")[1], (row, col), name = "Average price")
            
            
        else:
            pass

        
        
        
        
        sys.exit()
        for a, i in enumerate(["Energy", "System"]):
            DSP_temp = monthly_dispatch[monthly_dispatch["Energy/System"] == i].reset_index(drop = True)
            #print(DSP_temp.columns.tolist())
            for b, j in enumerate(["Offer", "Bid"]):                  
                print(i, j)
                DSP_temp1 = DSP_temp[DSP_temp["Order type"] == j].reset_index(drop = True)
                DSP_temp1 = pd.pivot_table(DSP_temp1, values = "Volume ABS", columns = "Month", index = "Fuel type", aggfunc = "sum")
                
                DSP_temp1.rename(columns = {c: f"{i} {j} volume {c}" for c in DSP_temp1.columns.tolist()}, inplace = True)
                
                if (a == 0) and (b == 0): # if it's the first df, add it into the blank dataframes
                    print(DSP_temp1.columns.tolist())
                    df = DSP_temp1[f"{i} {j} volume {month_str}"]
                    df_prev = DSP_temp1[f"{i} {j} volume {month_str_prev}"]
                else:
                    df = pd.merge(df, DSP_temp1[f"{i} {j} volume {month_str}"], left_index = True, right_index = True, how = "inner")
                    
        print(df)
        
    
    print(f"Code finished in: {datetime.now() - start_time}")
else:
    pass
    
