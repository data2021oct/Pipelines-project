import requests 
import json
import os
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize
import tweepy
import time
load_dotenv()
import re


#como hay nombres de álbumes que son números, los convertimos a string.
def num_string(dato):
    """
    if the function recives an integer, returns the number into a string.
    """
    if type(dato)== int:
        return str(dato)
    else:
        return dato




#función que cambia los caracteres especiales para las urls
def url_prep(df, colum):
    """
    recives the name of a dataframe anda the name of a one of it's columns
    applays regex in each cell changing substrings withe special characters into HTML URL Encode
    """
    url_replace = {"'":"%27",",":"%2c", "&":"%26",".":"%2e","/":"%2f","#":"%23","(":"%28",")":"%29","-":"%2d",'"':"%22",r"(\s+)":"%20" }
    for key,value in url_replace.items():
        df[colum] = df[colum].str.replace(key,value,regex=True)
        

#comprobamos respuestas de las urls
def urls_llamadas (df,art,alb):
    api_urls = []
    apikey = os.getenv("apikey")
    for s in range(len(df)):
        artist = df.loc[s,art]
        album = df.loc[s,alb]
        api_urls.append(f"http://ws.audioscrobbler.com/2.0/?method=album.getinfo&api_key={apikey}&artist={artist}&album={album}&format=json")
    request_dic = [] 
    i = 0
    for a in api_urls:
        res = requests.get(a).json()
        request_dic.append(res["album"])
        #if i%50 == 0:
        n = len(api_urls)/10
        if i%n == 0: 
            print(f"{i} done")
        elif i == (len(api_urls)-1):
            print(f"{i} done,finished")
        i+=1
    return request_dic