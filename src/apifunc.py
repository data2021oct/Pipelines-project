import requests 
import json
import os
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize
import time
load_dotenv()
import requests
from bs4 import BeautifulSoup
import src.scrappingfunc as scf




def urls_llamadas(df, colartista, colalbum): #pasar nombre tabla por parametros
    """
    esta función recibe 3 parámetros:
        df = un dataframe
        colartista = la columna que se refiere al artista y compatible con url
        colalbum = la columna que se reifere al album y compatible con url
    llama a la api de last fm del album.
    hace un return de una lista de diccionarios con los datos que tiene lastfm de todos los álbumes del dataframe    
    """
    api_urls = []
    apikey = os.getenv("apikey")
    for s in range(len(df)): #cambiarnombre tabla
        artist = df.loc[s,colartista] #tambien pasar nombre columnas por parametros
        album = df.loc[s,colalbum]
        api_urls.append(f"http://ws.audioscrobbler.com/2.0/?method=album.getinfo&api_key={apikey}&artist={artist}&album={album}&format=json")
    request_dic = [] 
    i = 0
    print("take a seat, 500 requests ahead")
    for a in api_urls:
        res = requests.get(a).json()
        request_dic.append(res["album"])
        if i%20==0:
            print(f"{i} done")
        elif i == (len(df) -1) :
            print(f"index {i} reached, finished")
        i+=1
    return request_dic
