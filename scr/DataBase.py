import sqlite3
import pandas as pd

class DataBase:

    DataFrame = pd.DataFrame()

    def __init__(self, DataFrame):
        self.DataFrame = DataFrame

    def CreateAndConnect(self, dbPath: str):

        con = sqlite3.connect(dbPath) #conectar a la base de datos
        
        self.DataFrame.to_sql('performance', con, if_exists= 'replace', index=False) #guardar información desde el dataframe a la base de datos
        con.commit() #commit a los cambios
    
    
    