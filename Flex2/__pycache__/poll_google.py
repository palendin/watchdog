import os
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import pandas as pd
import gspread as gs
import numpy as np
from insert_instrument_to_pgdb import auto_insert_flex_csv_to_pg

#from API.SQL_query.tools_and_functions.insert_instrument_to_pgdb import insert_flex_csv_to_pg

class Monkey(object):

    destinationPath = None
    obsvPath = None
    #event = None 
    worksheet = None
    masterDF = None
    dbKey = 0
    pathString = None
    startIndex = 0
    database = None
    cached_stamp = 0

    def __init__(self):
     
        # pathString: The file that the script will read to get pandas dataframe (the data file)
        self.pathString = '/Users/wayne/Documents/Programming/vscode/API/watchdog/Flex2/flex2/SampleResults2023-November.csv' # '/Users/sarmad/Documents/VSCode/Flex 2 Script/CDrive/SampleResults2023-November.csv'

        # destinationPath: Location of flex2 database (google drive for desktop path)
        self.destinationPath = '/Users/wayne/Library/CloudStorage/GoogleDrive-wayne@vitrolabsinc.com/Shared drives/R&PD Team/Vitrolab Experimental Data (Trained User Only)/Instrument/Flex2 Exports/Flex2 Database.gsheet' # '/Users/sarmad/Library/CloudStorage/GoogleDrive-sarmad@vitrolabsinc.com/Shared drives/R&PD Team/Vitrolab Experimental Data (Trained User Only)/Instrument/Flex2 Exports/Flex2 Database.gsheet'
        
        # folder to observe for changes
        self.obsvPath = '/Users/wayne/Documents/Programming/vscode/API/watchdog/Flex2/flex2' #'/Users/sarmad/Documents/VSCode/Flex 2 Script/CDrive'

        # start the observer
        self.myObserver = Observer()

    # UpdatedbKey: Everytime data is appended to database, this function will grab the value from the last populated row, column A.
    # This will be used to find what data from pandas dataframe has already been uploaded to database
    def updatedbKey(self):

        gc = gs.service_account(filename='/Users/wayne/Documents/Programming/vscode/API/Google_API/service_account.json')
        sh = gc.open_by_key('1CFYh78GX4T_xjn-nOZT0ysmWQdJCYz7yh3cy97OOe1g')
        self.worksheet = sh.worksheet('test')

        # databaseDF = pd.read_csv('https://docs.google.com/spreadsheets/d/1B5GIgo5jAgC163Cw9nt9fQj8fvh43KkgqirVemjOCiY/export?format=csv')

        # grab the last value of first col from google spreadsheet
        numRows = len(self.worksheet.col_values(1))      
        if numRows > 0:  
            self.dbKey = self.worksheet.cell(numRows,1).value
            #print(self.dbKey)
        else:
            self.dbKey = None

        
    # on_created: Whenever a new file is generated in the local flex2 data export folder directory, set the new file as the path
    # that pandas will grab data from (path for pd.read_csv())
    def on_created(self, event):
        if event.is_directory:
            return

        # Get the path of the new file
        self.pathString = event.src_path
        print(f"the path is now {self.pathString}")

    # push_to_DF: Push data from csv path to pandas dataframe, and then upload all new data from dataframe to database
    # Check for which data is old data by looking for the index that matches dbKey.
    def push_to_DF(self):
        #global worksheet, masterDF
        self.updatedbKey()
 
        # data for upload from csv file
        self.masterDF = pd.read_csv(self.pathString)
     
        # remove last empty column
        self.masterDF = self.masterDF.iloc[:,:-1]

        # convert empty cells to None
        self.masterDF = self.masterDF.replace(r'^\s*$', np.nan, regex=True)
        self.masterDF = self.masterDF.where(pd.notna(self.masterDF),None)

        # replace cells with "-" with None
        self.masterDF = self.masterDF.replace("-",np.nan)
        self.masterDF = self.masterDF.where(pd.notna(self.masterDF),None)
     
        # print(self.masterDF.loc[self.masterDF['Date & Time']==str(self.dbKey)].index[0])

        # if there is data match the last row value in database, then assume everything after is new data
        try:
            self.startIndex = self.masterDF.loc[self.masterDF['Date & Time']==str(self.dbKey)].index[0] + 1
        except:
            # if nothing matches, assume the whole file is new data, so start at 0
            self.startIndex = 0
        print('start index and lastrowkey is is',self.startIndex,self.dbKey)

        # push to google spreadsheet
        self.worksheet.append_rows(self.masterDF[self.startIndex:].values.tolist())
        print('Values Uploaded to google spreadsheet!')
     

    def push_to_postgres(self):
        
        # differential dataframe for uploading into postgresql
        df = self.masterDF.loc[self.startIndex:]
        print('dataframe is', df)
        if len(df) > 0:
            print('insert to postgres')
            auto_insert_flex_csv_to_pg(df, 'flex2')
            
        else:
            print('no need to upload to postgres')
            pass

    def watch(self):
        
        my_event_handler = PatternMatchingEventHandler(['*'], None, False, True)
        my_event_handler.on_created = self.on_created
        self.myObserver.schedule(my_event_handler,self.obsvPath,recursive=True)
        
        self.myObserver.start()

        while True:
            try:
                time.sleep(1)
                # Get the time stamp when the csv file linked by self.filepath was last saved
                stamp = os.stat(self.pathString).st_mtime
                # print(self.pathString)

                # Check if the current time stamp matches the last known save. If not, assume the file has been saved again and there is new
                # data that needs to be uploaded. Push new data to database and update cached time stamp with new stamp
                if stamp != self.cached_stamp:
                    
                    self.cached_stamp = stamp
                    print(f"MONKEY: {self.pathString} has been changed")
                    # masterDF = pd.read_csv(pathString)
                    self.push_to_DF()
                    self.push_to_postgres()
                
                # Infinite loop. Every 1 second, pub (class monkey) will check if there is either a new file or new save on existing file linked by
                # pathString. If a new file has been created, set the path to that new file such that this new file gets monitored for new saves
            except KeyboardInterrupt:
                print('\nDone')
                self.myObserver.stop()
                self.myObserver.join()
                exit()
        
if __name__ == "__main__":
    pub = Monkey()
    pub.watch()

    