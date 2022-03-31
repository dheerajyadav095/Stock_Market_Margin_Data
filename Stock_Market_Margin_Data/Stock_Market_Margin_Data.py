
import threading

import Margin_Calculated_Data

class myThread (threading.Thread):

   def __init__(self, threadID, url,sheet_name):
     
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.url = url
      self.sheet_name = sheet_name
    
   def run(self):
      
      Margin_Calculated_Data.Margin_Calculated_Data(self.threadID, self.url,self.sheet_name)
  
def UploadMarginFile_CSV():
          
    url='Share_market_data\8505803380_OrderBook_DEFAULT.csv'
    
    sheet_name='Share Data'
    
    thread = myThread(1,url,sheet_name)
            
    thread.start()
   
    print('end')     

if __name__== "__main__":
    
    UploadMarginFile_CSV()
    
    print ("Exiting Main Thread")