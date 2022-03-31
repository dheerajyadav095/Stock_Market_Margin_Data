
import pandas as pd
import time
import datetime

class Margin_Data:

    def __init__(self,threadID, url,sheet_name):
       
       self.threadID = threadID
       self.url= url
       self.sheet_name = sheet_name
       self.df = ''
       self.tempdf = ''
    
    def export_file(self):
        
        print("Loading csv file")
        self.df=pd.read_csv(self.url)
            
    def add_newColumn_with_calculated(self):
        
        self.df['Date'] = pd.to_datetime(self.df['Date']).dt.date

        df1 = self.df.groupby(by=["Date","Exch","Open","Stock","Status","Buy/Sell"]).agg({'Price' : lambda x: x.mean(),'Qty' : lambda x: x.sum(),'Executed' : lambda x: x.sum(),'Cancelled' : lambda x: x.sum()}).sort_values(['Date','Stock'],ascending = [False,True])
      
        df1.reset_index(inplace=True)

        self.df =df1

    def calculate_profit_loss(self):
            
        Stockname = ''

        for i in self.df.index:
    
            temp_date = self.df.iloc[i]['Date']
    
            StockDate = datetime.datetime.strptime(str(temp_date),'%Y-%m-%d')
            currentDate = datetime.datetime.today()

            days = currentDate - StockDate
    
            self.df.at[i,'DiffDays'] = days.days
    
            strDate = datetime.datetime.strptime(str(temp_date), '%Y-%m-%d').date()
    
            self.df.at[i,'Day'] = pd.to_numeric(strDate.day)
            self.df.at[i,'Month'] = pd.to_numeric(strDate.month)
            self.df.at[i,'Year'] = pd.to_numeric(strDate.year)
    
            temp_Exch = self.df.iloc[i]['Exch']
            if temp_Exch == 'BSE':
                self.df.at[i,'ExchType'] = 1
            elif temp_Exch == 'NSE':
                self.df.at[i,'ExchType'] = 2
            elif temp_Exch == 'CSE':
                self.df.at[i,'ExchType'] = 3
            else:
                self.df.at[i,'ExchType'] = 4
        
            temp_Stock = self.df.iloc[i]['Stock']
            temp_Buy_Sell = self.df.iloc[i]['Buy/Sell']
    
            if temp_Stock == Stockname and temp_Buy_Sell =='Sell':

                temp_PriceBuy = self.df.iloc[i-1]['Price']
                temp_PriceSell = self.df.iloc[i]['Price']

            elif temp_Stock == self.df.iloc[i+1]['Stock']:

                temp_PriceBuy = self.df.iloc[i]['Price']
                temp_PriceSell = self.df.iloc[i+1]['Price']

            else:

                temp_PriceBuy = self.df.iloc[i]['Price']
                temp_PriceSell = self.df.iloc[i]['Price']
       
            temp_Cancelled = self.df.iloc[i]['Cancelled']
            temp_Profit_amt = 0
    
            temp_per_Profit_loss = 0
    
            if Stockname != temp_Stock:
                Stockname = temp_Stock
        
            if Stockname == temp_Stock:
                if temp_Cancelled > 0:
                    temp_Profit_amt = 0
                else:
                    temp_Profit_amt = temp_PriceSell - temp_PriceBuy
                    temp_per_Profit_loss = (temp_Profit_amt / temp_PriceSell )*100
            else:
                temp_Profit_amt = 0
    
            self.df.at[i,'Profit/Loss Amount'] = temp_Profit_amt
    
            self.df.at[i, 'Profit/Loss %']= round(temp_per_Profit_loss, 2) 
    
            if temp_Profit_amt >= 1:
                self.df.at[i,'Profit/Loss'] = 'Profit'
            elif temp_Profit_amt == 0:
                self.df.at[i,'Profit/Loss'] = ''
            else:
                self.df.at[i,'Profit/Loss'] = 'Loss'
    
        self.df['DiffDays'] = self.df['DiffDays'].astype(int)
        self.df['Day'] = self.df['Day'].astype(int)
        self.df['Month'] = self.df['Month'].astype(int)
        self.df['Year'] = self.df['Year'].astype(int)
        self.df['ExchType'] = self.df['ExchType'].astype(int)

    def calculate_date_wise_data(self):
        
        dfDate = self.df['Date'].drop_duplicates()
        self.df.columns = ["Date", "Exch","Open","Stock","Status","Buy_Sell","Price","Qty","Execute","Cancelled","DiffDays",'Day',"Month","Year","ExchType","Profit_Loss_AMT","Profit_Loss_Per","Profit_Loss"]

        df = self.df

        total_amt = 0
        total_per = 0   
        
        if dfDate.empty == False:

            for j in dfDate:
    
                total_amt = 0   
                total_per = 0   
    
                cnt = 0    
                tempDate=''
                tempExch=''
           
                tempOpen=0
                tempStock=''
            
                tempProfitLossAmt=0
                tempProfitLossPer=0
                tempProfitLoss=''
    
                tempPrice=0
                tempQty=0
                tempExecuted=0
                tempCancelled=0
    
                tempPriceSell=0
                tempQtySell=0
                tempExecutedSell=0
                tempCancelledSell=0
    
                tempPriceCancel=0
                tempQtyCancel=0
                tempExecutedCancel=0
                tempCancelledCancel=0
    
                tempPriceCancelSell=0
                tempQtyCancelSell=0
                tempExecutedCancelSell=0
                tempCancelledCancelSell=0
    
                tempDiffDays=0
                tempDay=0
                tempMonth=0
                tempYear=0
    
                for i in range(0,df['Date'].count()):
        
                    if df.iloc[i].Date == j:
            
                        if df.iloc[i].ExchType >=0:
                
                            if df.iloc[i].Buy_Sell == "Sell" and df.iloc[i].Status == "Executed":

                                    total_amt = total_amt + df.iloc[i].Profit_Loss_AMT
                                    total_per = total_per + df.iloc[i].Profit_Loss_Per
                                    tempPriceSell = tempPriceSell + df.iloc[i].Price
                                    tempQtySell = tempQtySell + df.iloc[i].Qty
                                    tempExecutedSell = tempExecutedSell + df.iloc[i].Execute
                                    tempCancelledSell = tempCancelledSell + df.iloc[i].Cancelled

                            elif df.iloc[i].Buy_Sell == "Buy" and df.iloc[i].Status == "Executed":

                                    tempPrice = tempPrice + df.iloc[i].Price
                                    tempQty = tempQty + df.iloc[i].Qty
                                    tempExecuted = tempExecuted + df.iloc[i].Execute
                                    tempCancelled = tempCancelled + df.iloc[i].Cancelled

                            elif df.iloc[i].Buy_Sell == "Sell" and df.iloc[i].Status == "Cancelled":

                                    tempPriceCancelSell = tempPriceCancelSell + df.iloc[i].Price
                                    tempQtyCancelSell = tempQtyCancelSell + df.iloc[i].Qty
                                    tempExecutedCancelSell = tempExecutedCancelSell + df.iloc[i].Execute
                                    tempCancelledCancelSell = tempCancelledCancelSell + df.iloc[i].Cancelled

                            elif df.iloc[i].Buy_Sell == "Buy" and df.iloc[i].Status == "Cancelled":

                                    tempPriceCancel = tempPriceCancel + df.iloc[i].Price
                                    tempQtyCancel = tempQtyCancel + df.iloc[i].Qty
                                    tempExecutedCancel = tempExecutedCancel + df.iloc[i].Execute
                                    tempCancelledCancel = tempCancelledCancel + df.iloc[i].Cancelled

                    cnt = cnt + 1
    
                tempDate= j
                tempExch= 'Date wise - Total'
                tempOpen= 1
                tempStock= ''
                tempProfitLossAmt = total_amt
                tempProfitLossPer = total_per
    
                if total_amt > 0: 
                    tempProfitLoss = 'Profit' 
                elif total_amt == 0: 
                    tempProfitLoss = '' 
                else:
                    tempProfitLoss = 'Loss' 
    
                tempDiffDays=df.iloc[i].DiffDays
                tempDay=df.iloc[i].Day
                tempMonth=df.iloc[i].Month
                tempYear=df.iloc[i].Year
    
                if tempPrice > 0:
        
                    New_Execute_Buy_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Executed', 'Profit_Loss_AMT':tempProfitLossAmt,'Profit_Loss_Per':tempProfitLossPer, 'Profit_Loss':tempProfitLoss, 'Buy_Sell': 'Buy', 'Price':tempPrice, 'Qty':tempQty, 'Execute':tempExecuted,'Cancelled':tempCancelled,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
        
                    df = df.append(New_Execute_Buy_row, ignore_index = True)
        
                if tempPriceSell > 0:
                    New_Execute_Sell_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Executed', 'Profit_Loss_AMT':tempProfitLossAmt,'Profit_Loss_Per':tempProfitLossPer, 'Profit_Loss':tempProfitLoss, 'Buy_Sell': 'Sell', 'Price':tempPriceSell, 'Qty':tempQtySell, 'Execute':tempExecutedSell,'Cancelled':tempCancelledSell,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                    df = df.append(New_Execute_Sell_row, ignore_index = True)
        
                if tempPriceCancel > 0:
        
                    New_Cancel_Buy_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Cancelled', 'Profit_Loss_AMT':0,'Profit_Loss_Per':0, 'Profit_Loss':'', 'Buy_Sell': 'Buy', 'Price':tempPriceCancel, 'Qty':tempQtyCancel, 'Execute':0,'Cancelled':tempCancelledCancel,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                    df = df.append(New_Cancel_Buy_row, ignore_index = True)
    
                if tempPriceCancelSell > 0 and tempQtyCancelSell > 0 and tempCancelledCancelSell > 0 :
        
                    New_Cancel_Sell_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Cancelled', 'Profit_Loss_AMT':0,'Profit_Loss_Per':0, 'Profit_Loss':'', 'Buy_Sell': 'Sell', 'Price':tempPriceCancelSell, 'Qty':tempQtyCancelSell, 'Execute':0,'Cancelled':tempCancelledCancelSell,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                    df = df.append(New_Cancel_Sell_row, ignore_index = True)
        
        self.df = df

    def calculate_Month_wise_data(self):
        
        dfLastYearandMonth = self.df[['Day','Month','Year']].drop_duplicates()
        dfLastYearandMonth = dfLastYearandMonth.groupby(by=['Year','Month']).max().sort_values(['Year'],ascending = [False])
        dfLastYearandMonth = dfLastYearandMonth.reset_index()
        dfLastYearandMonth
          
        total_amt = 0
        total_per = 0  
        
        if dfLastYearandMonth.empty == False:

            for j in dfLastYearandMonth.index:
    
                total_amt = 0   
                total_per = 0   
    
                cnt = 0    
                tempDate=''
                tempExch=''
                tempOpen=0
                tempStock=''
            
                tempProfitLossAmt=0
                tempProfitLossPer=0
                tempProfitLoss=''
    
                tempPrice=0
                tempQty=0
                tempExecuted=0
                tempCancelled=0
    
                tempPriceSell=0
                tempQtySell=0
                tempExecutedSell=0
                tempCancelledSell=0
    
                tempPriceCancel=0
                tempQtyCancel=0
                tempExecutedCancel=0
                tempCancelledCancel=0
    
                tempPriceCancelSell=0
                tempQtyCancelSell=0
                tempExecutedCancelSell=0
                tempCancelledCancelSell=0
    
                tempDiffDays=0
                tempDay=0
                tempMonth=0
                tempYear=0
    
                for i in range(0,self.df['Date'].count()):
        
                    if self.df.iloc[i].ExchType >=0:
            
                        if self.df.iloc[i].Year == dfLastYearandMonth.iloc[j].Year:

                            if self.df.iloc[i].Month == dfLastYearandMonth.iloc[j].Month:

                                if self.df.iloc[i].Buy_Sell == "Sell" and self.df.iloc[i].Status == "Executed":

                                        total_amt = total_amt + self.df.iloc[i].Profit_Loss_AMT
                                        total_per = total_per + self.df.iloc[i].Profit_Loss_Per
                                        tempPriceSell = tempPriceSell + self.df.iloc[i].Price
                                        tempQtySell = tempQtySell + self.df.iloc[i].Qty
                                        tempExecutedSell = tempExecutedSell + self.df.iloc[i].Execute
                                        tempCancelledSell = tempCancelledSell + self.df.iloc[i].Cancelled

                                elif self.df.iloc[i].Buy_Sell == "Buy" and self.df.iloc[i].Status == "Executed":

                                        tempPrice = tempPrice + self.df.iloc[i].Price
                                        tempQty = tempQty + self.df.iloc[i].Qty
                                        tempExecuted = tempExecuted + self.df.iloc[i].Execute
                                        tempCancelled = tempCancelled + self.df.iloc[i].Cancelled

                                elif self.df.iloc[i].Buy_Sell == "Sell" and self.df.iloc[i].Status == "Cancelled":

                                        tempPriceCancelSell = tempPriceCancelSell + self.df.iloc[i].Price
                                        tempQtyCancelSell = tempQtyCancelSell + self.df.iloc[i].Qty
                                        tempExecutedCancelSell = tempExecutedCancelSell + self.df.iloc[i].Execute
                                        tempCancelledCancelSell = tempCancelledCancelSell + self.df.iloc[i].Cancelled

                                elif self.df.iloc[i].Buy_Sell == "Buy" and self.df.iloc[i].Status == "Cancelled":

                                        tempPriceCancel = tempPriceCancel + self.df.iloc[i].Price
                                        tempQtyCancel = tempQtyCancel + self.df.iloc[i].Qty
                                        tempExecutedCancel = tempExecutedCancel + self.df.iloc[i].Execute
                                        tempCancelledCancel = tempCancelledCancel + self.df.iloc[i].Cancelled

                    cnt = cnt + 1
    
                tempDate= str(dfLastYearandMonth.iloc[j].Year) + '-' + str(dfLastYearandMonth.iloc[j].Month) + '-' + str(dfLastYearandMonth.iloc[j].Day)
                tempExch= 'Month wise - Total'
                tempOpen= 1
                tempStock= ''
                tempProfitLossAmt = total_amt
                tempProfitLossPer = total_per
    
                if total_amt > 0: 
                    tempProfitLoss = 'Profit' 
                elif total_amt == 0: 
                    tempProfitLoss = '' 
                else:
                    tempProfitLoss = 'Loss' 
    
                tempDiffDays=self.df.iloc[j].DiffDays
                tempDay=self.df.iloc[j].Day
                tempMonth=self.df.iloc[j].Month
                tempYear=self.df.iloc[j].Year
    
                if tempPrice > 0:
        
                    New_Execute_Buy_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Executed', 'Profit_Loss_AMT':tempProfitLossAmt,'Profit_Loss_Per':tempProfitLossPer, 'Profit_Loss':tempProfitLoss, 'Buy_Sell': 'Buy', 'Price':tempPrice, 'Qty':tempQty, 'Execute':tempExecuted,'Cancelled':tempCancelled,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                    self.df = self.df.append(New_Execute_Buy_row, ignore_index = True)
        
                if tempPriceSell > 0:
                    New_Execute_Sell_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Executed', 'Profit_Loss_AMT':tempProfitLossAmt,'Profit_Loss_Per':tempProfitLossPer, 'Profit_Loss':tempProfitLoss, 'Buy_Sell': 'Sell', 'Price':tempPriceSell, 'Qty':tempQtySell, 'Execute':tempExecutedSell,'Cancelled':tempCancelledSell,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                    self.df = self.df.append(New_Execute_Sell_row, ignore_index = True)
        
                if tempPriceCancel > 0:
        
                    New_Cancel_Buy_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Cancelled', 'Profit_Loss_AMT':0,'Profit_Loss_Per':0, 'Profit_Loss':'', 'Buy_Sell': 'Buy', 'Price':tempPriceCancel, 'Qty':tempQtyCancel, 'Execute':0,'Cancelled':tempCancelledCancel,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                    self.df = self.df.append(New_Cancel_Buy_row, ignore_index = True)
    
                if tempPriceCancelSell > 0 and tempQtyCancelSell > 0 and tempCancelledCancelSell > 0 :
        
                    New_Cancel_Sell_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Cancelled', 'Profit_Loss_AMT':0,'Profit_Loss_Per':0, 'Profit_Loss':'', 'Buy_Sell': 'Sell', 'Price':tempPriceCancelSell, 'Qty':tempQtyCancelSell, 'Execute':0,'Cancelled':tempCancelledCancelSell,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                    self.df = self.df.append(New_Cancel_Sell_row, ignore_index = True)
    
    def calculate_Year_wise_data(self):
        
        df223 = self.df
        
        dfYear_Month_Day = self.df[['Day','Month','Year']].drop_duplicates()
        dfYear_Month_Day = dfYear_Month_Day.loc[dfYear_Month_Day.groupby(by=['Year'])["Month"].idxmax()]    
        dfYear_Month_Day.reset_index(inplace=True)

        total_amt = 0
        total_per = 0   

        if dfYear_Month_Day.empty == False:

            for j in dfYear_Month_Day.index:
    
                    total_amt = 0   
                    total_per = 0   
    
                    cnt = 0    
                    tempDate=''
                    tempExch=''
                    tempOpen=0
                    tempStock=''
                    tempProfitLossAmt=0
                    tempProfitLossPer=0
                    tempProfitLoss=''
    
                    tempPrice=0
                    tempQty=0
                    tempExecuted=0
                    tempCancelled=0
    
                    tempPriceSell=0
                    tempQtySell=0
                    tempExecutedSell=0
                    tempCancelledSell=0
    
                    tempPriceCancel=0
                    tempQtyCancel=0
                    tempExecutedCancel=0
                    tempCancelledCancel=0
    
                    tempPriceCancelSell=0
                    tempQtyCancelSell=0
                    tempExecutedCancelSell=0
                    tempCancelledCancelSell=0
    
                    tempDiffDays=0
                    tempDay=0
                    tempMonth=0
                    tempYear=0
    
                    for i in range(0,df223['Date'].count()):
        
                        if df223.iloc[i].ExchType >=0:
            
                            if df223.iloc[i].Year == dfYear_Month_Day.iloc[j].Year:

                                if df223.iloc[i].Buy_Sell == "Sell" and df223.iloc[i].Status == "Executed":

                                        total_amt = total_amt + df223.iloc[i].Profit_Loss_AMT
                                        total_per = total_per + df223.iloc[i].Profit_Loss_Per
                                        tempPriceSell = tempPriceSell + df223.iloc[i].Price
                                        tempQtySell = tempQtySell + df223.iloc[i].Qty
                                        tempExecutedSell = tempExecutedSell + df223.iloc[i].Execute
                                        tempCancelledSell = tempCancelledSell + df223.iloc[i].Cancelled

                                elif df223.iloc[i].Buy_Sell == "Buy" and df223.iloc[i].Status == "Executed":

                                        tempPrice = tempPrice + df223.iloc[i].Price
                                        tempQty = tempQty + df223.iloc[i].Qty
                                        tempExecuted = tempExecuted + df223.iloc[i].Execute
                                        tempCancelled = tempCancelled + df223.iloc[i].Cancelled

                                elif df223.iloc[i].Buy_Sell == "Sell" and df223.iloc[i].Status == "Cancelled":

                                        tempPriceCancelSell = tempPriceCancelSell + df223.iloc[i].Price
                                        tempQtyCancelSell = tempQtyCancelSell + df223.iloc[i].Qty
                                        tempExecutedCancelSell = tempExecutedCancelSell + df223.iloc[i].Execute
                                        tempCancelledCancelSell = tempCancelledCancelSell + df223.iloc[i].Cancelled

                                elif df223.iloc[i].Buy_Sell == "Buy" and df223.iloc[i].Status == "Cancelled":

                                        tempPriceCancel = tempPriceCancel + df223.iloc[i].Price
                                        tempQtyCancel = tempQtyCancel + df223.iloc[i].Qty
                                        tempExecutedCancel = tempExecutedCancel + df223.iloc[i].Execute
                                        tempCancelledCancel = tempCancelledCancel + df223.iloc[i].Cancelled

                        cnt = cnt + 1
        
                    tempDate= str(dfYear_Month_Day.iloc[j].Year) + '-' + str(dfYear_Month_Day.iloc[j].Month) + '-' + str(dfYear_Month_Day.iloc[j].Day)

                    tempExch= 'Year wise - Total'
                    tempOpen= 1
                    tempStock= ''
                    tempProfitLossAmt = total_amt
                    tempProfitLossPer = total_per
    
                    if total_amt > 0: 
                        tempProfitLoss = 'Profit' 
                    elif total_amt == 0: 
                        tempProfitLoss = '' 
                    else:
                        tempProfitLoss = 'Loss' 
        
                    tempDiffDays=df223.iloc[j].DiffDays
                    tempDay=df223.iloc[j].Day
                    tempMonth=df223.iloc[j].Month
                    tempYear=df223.iloc[j].Year
    
                    if tempPrice > 0:
        
                        New_Execute_Buy_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Executed', 'Profit_Loss_AMT':tempProfitLossAmt,'Profit_Loss_Per':tempProfitLossPer, 'Profit_Loss':tempProfitLoss, 'Buy_Sell': 'Buy', 'Price':tempPrice, 'Qty':tempQty, 'Execute':tempExecuted,'Cancelled':tempCancelled,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                        df223 = df223.append(New_Execute_Buy_row, ignore_index = True)
        
                    if tempPriceSell > 0:
                        New_Execute_Sell_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Executed', 'Profit_Loss_AMT':tempProfitLossAmt,'Profit_Loss_Per':tempProfitLossPer, 'Profit_Loss':tempProfitLoss, 'Buy_Sell': 'Sell', 'Price':tempPriceSell, 'Qty':tempQtySell, 'Execute':tempExecutedSell,'Cancelled':tempCancelledSell,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                        df223 = df223.append(New_Execute_Sell_row, ignore_index = True)
        
                    if tempPriceCancel > 0:
        
                        New_Cancel_Buy_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Cancelled', 'Profit_Loss_AMT':0,'Profit_Loss_Per':0, 'Profit_Loss':'', 'Buy_Sell': 'Buy', 'Price':tempPriceCancel, 'Qty':tempQtyCancel, 'Execute':0,'Cancelled':tempCancelledCancel,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                        df223 = df223.append(New_Cancel_Buy_row, ignore_index = True)
    
                    if tempPriceCancelSell > 0 and tempQtyCancelSell > 0 and tempCancelledCancelSell > 0 :
        
                        New_Cancel_Sell_row = {'Date':tempDate, 'Exch':tempExch, 'ExchType': 0, 'Open':tempOpen, 'Stock':tempStock, 'Status':'Total Cancelled', 'Profit_Loss_AMT':0,'Profit_Loss_Per':0, 'Profit_Loss':'', 'Buy_Sell': 'Sell', 'Price':tempPriceCancelSell, 'Qty':tempQtyCancelSell, 'Execute':0,'Cancelled':tempCancelledCancelSell,'DiffDays' :tempDiffDays, 'Day' : tempDay,'Month': tempMonth,'Year': tempYear}
                        df223 = df223.append(New_Cancel_Sell_row, ignore_index = True)
        
        self.df = df223

    def unnamed_and_unused_column(self):
        
        delete_col=[]

        for i in range(0,len(self.df.columns)):
            if (self.df.columns[i].lower() == ('unnamed: '+ str(i))):
                delete_col.append(self.df.columns[i])

        if len(delete_col) > 0:
            pd.concat([self.df.pop(x) for x in delete_col], axis=1)
    
        self.df.columns = ["Date","Exch","Open","Stock","Status","Buy/Sell","Price","Qty","Executed","Cancelled","DiffDays","Day","Month","Year","ExchType","Profit/Loss Amount","Profit/Loss %","Profit/Loss"]
        self.df.drop(['DiffDays', 'Day','Month','Year'], axis=1, inplace=True)
    
    def save_data_in_excel(self):
        
        self.df['Date'] = pd.to_datetime(self.df['Date']).dt.date

        writer =''
        
        self.df = self.df.groupby(by=['Date','Exch','ExchType','Open','Stock','Status','Profit/Loss Amount','Profit/Loss %','Profit/Loss','Buy/Sell']).sum().sort_values(['Date','Open','Exch','ExchType','Status'],ascending = [True,True,True,True,True])
        
        writer=pd.ExcelWriter('Share_market_data\AllShareData.xlsx',mode='w',engine='openpyxl')

        self.df.to_excel(writer , sheet_name=self.sheet_name,index =True)

        writer.save()
                    
    def upload_file(self):
        
        Margin_Data.export_file(self)
        Margin_Data.add_newColumn_with_calculated(self)
        Margin_Data.calculate_profit_loss(self)
        Margin_Data.calculate_date_wise_data(self)
        Margin_Data.calculate_Month_wise_data(self)
        Margin_Data.calculate_Year_wise_data(self)
        Margin_Data.unnamed_and_unused_column(self)
        Margin_Data.save_data_in_excel(self)

class Margin_Calculated_Data:

    def __init__(self,threadID, url,sheet_name):

       Margin_Data.__init__(self,threadID, url,sheet_name)
       Margin_Data.upload_file(self)
   
