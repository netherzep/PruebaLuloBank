import pandas as pd
from pandas_profiling import ProfileReport
import json, requests, os, glob
from DataBase import DataBase


def SaveFile(data: json, Name: str, FilePath: str):
    """Method that saves json files in to the directory"""
    
    OutFile = open(FilePath+Name+'.json','w')
    json.dump(data, OutFile, indent = 4) #saving json file.

def MergeJson(FilePath: str) -> pd.DataFrame():

    """Mthod that merges all json files into one big data frame and returns the dataframe"""

    json_pattern = os.path.join(FilePath + '/json/', '*.json')
    file_list = glob.glob(json_pattern) #list of json files

    DataFrames = [] #dataframes array

    for file in file_list:
        with open(file) as f:
            json_data = pd.json_normalize(json.loads(f.read()))
            json_data['site'] = file.rsplit("/", 1)[-1]
        DataFrames.append(json_data)
    BigDataFrame = pd.concat(DataFrames) #concat all dataframes from the dataframes array

    BigDataFrame.index = pd.RangeIndex(1,len(BigDataFrame) + 1)
    BigDataFrame['index'] = BigDataFrame.index
    print("\nMerge de archivos json ejecutado con éxito!\n")
    
        
    return BigDataFrame

def dfCreation(BigDataFrame: pd.DataFrame(), FilePath: str) -> pd.DataFrame():
    
    """Dataframe creation from all json files and send to profiling"""
    myDataframe = pd.DataFrame(BigDataFrame[['index','_embedded.show.id','_embedded.show.name', '_embedded.show.type','_embedded.show.language', 
                        '_embedded.show.averageRuntime', '_embedded.show.webChannel.country.name']])
    #myDataframe = BigDataFrame[['index','_embedded.show.id','_embedded.show.name', '_embedded.show.type','_embedded.show.language', 
                        #'_embedded.show.averageRuntime', '_embedded.show.webChannel.country.name']]

    myDataframe.columns = ['indx','serie_id', 'Name', 'Type', 'Language', 'AvgRunTime', 'Country']

    #cleaning Data from dataframe name column
    myDataframe['Name'] = myDataframe['Name'].replace('#','').replace('@','').replace(')','').replace(
                            '₴','').replace('?','').replace('$','') 

    print("\nDataframe Creado con éxito!\n")
    myDataframe.to_csv('myDataFrame.csv')                        
    print(myDataframe)
    
    #Creando profiling del dataframe
    profile = ProfileReport(myDataframe, title = 'Profiling', html = {'style': {'full_width': True}})
    profile.to_file(FilePath + '/profiling/profile.html')

    print("\Profiling del dataframe creado con éxito.\n")
    
    return myDataframe


def run():

    FileNames = []

    """pulling data from API and sending files to save method"""

    FilePath = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) #'/mnt/d/Desktop/PruebaLuloBank/json/'
    
    print("Iniciando request al API y guardando archivos json...\n")

    for i in range(0, 31): #Creación de los archivos json 

        if i <9:   
            
            FileNames.append("2020-12-0"+str(i+1))
            myRequest = requests.get("http://api.tvmaze.com/schedule/web?date=2020-12-0"+str(i+1)) #Request al API
            data = myRequest.json()
            SaveFile(data, "2020-12-0"+str(i+1), FilePath + '/json/') #Metodo para guardar el archivo en carpeta json
            
            
        if i >= 9:        
            FileNames.append("2020-12-"+str(i+1))
            myRequest = requests.get("http://api.tvmaze.com/schedule/web?date=2020-12-"+str(i+1)) #Request al API
            data = myRequest.json()
            SaveFile(data, "2020-12-"+str(i+1), FilePath + '/json/') #Metodo para guardar el archivo en carpeta json
            
    print("\nRequest al API ejecutado con éxito! Archivos json guardados.\n")
   
    db = DataBase(dfCreation(MergeJson(FilePath), FilePath)) #instancia de objeto de la clase DataBase
    db.CreateAndConnect(FilePath + '/db/performance.db') #Creates database with the final dataframe.
    
    

if __name__ == '__main__':
    run()






 