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
import src.apifunc as apif

#LIMPIEZA

#cargar dataset The 500 Greatest Albums of All Time
stone = pd.read_csv("../data/albumlist.csv", encoding='latin-1')

#limpieza de datos
stone.Album = stone.Album.str.replace("_","-")
stone.Album = stone.Album.str.replace("Ê"," ")
stone.Artist = stone.Artist.str.replace("Ê"," ")
stone.Genre = stone.Genre.str.replace("Ê"," ")
stone.Subgenre = stone.Subgenre.str.replace("Ê"," ")
stone.Artist = stone.Artist.str.replace("Wu Tang Clan","Wu-Tang Clan")
stone.Album = stone.Album.str.replace("Chteau","Chateau")
stone.Album = stone.Album.str.replace(r'(^Blues Breakers With Eric \w+.+)',"Blues Breakers",regex=True)
stone.Album = stone.Album.str.replace(r'(^The Band...The Brown.*)',"The Band",regex=True)
stone.Album = stone.Album.str.replace("The B 52's / Play Loud","The B-52's")
stone.Album = stone.Album.str.replace("Proud Mary: The Best of Ike and Tina Turner","Best Of / Proud Mary")

#creamos una columna igual que album porque algunos títulos tienen caracteres incompatibles con las url
#cambiamos sus datos
stone["Alb_url"] = stone["Album"]

stone.Alb_url = stone.Alb_url.str.replace(r'(^Sign .Peace..the Times)',"Sign 'O' The Times",regex=True)
stone.Alb_url = stone.Alb_url.str.replace("#1 Record","%231 Record",regex=True)


#creamos nuevas columnas de gen y subgen y nos quedamos solo con el primer elemento de cada que hay en columnas genre y subgenre

stone["Gen"] = stone["Genre"].str.extract(r"(^([^,])+)")[0]
stone["Subgen"] = stone["Subgenre"].str.extract(r"(^([^,])+)")[0]


#exportamos tabla resultante
stone.to_csv("../data/stone.csv")


#TRATAMIENTO DE DATOS // wrangling

#importamos api de LastFM 
apikey = os.getenv("apikey") #apikey de lastfm

#soptify most streamed (scrapping)
url_spotify = "https://chartmasters.org/spotify-most-streamed-albums/?y=alltime"
html_spotify = requests.get(url_spotify)
soup_spotify = BeautifulSoup(html_spotify.content,"html.parser")
tablas_spotify = soup_spotify.findAll("table")
spotify = tablas_spotify[1]
#llamamos a la función que crea la función necesaria para crear un diccionario base de un dataset con los datos de spotify
element = spotify
Top_alb_spotify = scf.data_spotify(element)
#cramos el dataset
Spotify_500 = pd.DataFrame(Top_alb_spotify)
#exportamos datos a csv
Spotify_500.to_csv("../data/Spotify_500.csv")


#LastFM 50 Top Artists

#usamos la api de Lastfm para extraer la información que ofrece sobre los 50 artistas más scrobbleados en su web
url_last50 = f"http://ws.audioscrobbler.com/2.0/?method=chart.gettopartists&api_key={apikey}&format=json"
req_last50 = requests.get(url_last50).json()
last_50 = req_last50["artists"]["artist"]

#creamos el dataframe la información de lastfm
last_top50 = pd.DataFrame.from_dict(last_50)

top50_lastfm = last_top50[['name', 'playcount', 'listeners']]

#exportamos el dataframe
top50_lastfm.to_csv("../data/top50_lastfm.csv")


#TOP SELLS

#scrapeamos la web de insiderbussines que tiene información actualizada de RIAA sobre los 500 álbumes más vendidos en USA

url_insider = "https://www.businessinsider.com/50-best-selling-albums-all-time-2016-9"
html_insider = requests.get(url_insider)
soup_insider = BeautifulSoup(html_insider.content,"html.parser")

tags_insider = soup_insider.find_all("div", {"class": "slide-layout"})

#utilizamos una función que hemos creado para extraer y normalizar los datos de esta web

insider_data = scf.data_sells(tags_insider)
#creamos el dataframe necesario par normalizar los datos
insider = pd.DataFrame(insider_data)

#limpiamos los datos del dataframe obtenido

insider["Rank"] = insider["album"].str.extract(r"(^\d+)")
insider["Millions"] = insider["certified units"].str.extract(r"(\d+)")
insider["album"] = insider["album"].str.replace(" – "," — ",regex = True)
insider["Artist"] = insider["album"].str.extract(r"(^\d+.\s(.+?)—)")[1]
insider["Title"] = insider["album"].str.extract(r"(—\s(.+?)$)")[1]
insider["Title"] = insider["Title"].str.replace('"',"")

insider[["Millions","Rank"]] = insider[["Millions","Rank"]].astype("int64")

#resumimos los datos en un dataframe más conciso y exportamos a csv
top_sells = insider[["Rank","Artist","Title","Millions"]]
top_sells.to_csv("../data/top_sells.csv")

#enriquecimiento de top sells
#defimos las variables que vamos a pasar como argumentos a la función que llama a la api de albums de lastfm
df = top_sells
colartista = "Artist"
colalbum = "Title"
req_top_sales = apif.urls_llamadas(df, colartista, colalbum)
#creamos un dataframe con el resultado de la función
top_sales_df = pd.DataFrame(req_top_sales)
#hacemos un dataframe más conciso de sales y enriquecemos con los datos de lastfm

top_50_sales = top_sales_df[["artist","name","playcount","listeners"]]
top_50_sales_rich = top_sells.merge(top_50_sales,left_index=True, right_index=True)
top_sales_rich = top_50_sales_rich[["Rank","Artist","Title","playcount","listeners"]]

#exportamos el dataframe enriquecido a csv
top_sales_rich.to_csv("../data/top_sells_rich.csv")


# Top 500 albums Rolling Stones enrinquecimiento con LastFM
    ### carga de archivo limpio

stone = pd.read_csv("../data/stone.csv", encoding='latin-1')
stone = stone.drop("Unnamed: 0",axis=1)

#función llamada a api de lastfm que información de todos los albums en un dataframe
df = stone
colartista = "Artist"
colalbum = "Alb_url"
Top500_last = apif.urls_llamadas(df, colartista, colalbum)

#creamos un dataframe con el resultado de la función
Top500_last_df = pd.DataFrame(Top500_last)

#concretamos el dataframe
Lastfm_500 = Top500_last_df[["artist","playcount","name","listeners"]]

#concretamos el dataframe original de rolling stone
stone_500 = stone[["Number","Year","Album","Artist","Gen","Subgen"]]

#enriquecemos los datos de rolling stone con los de lastfm
stone_500_richment = stone_500.merge(Lastfm_500,left_index=True, right_index=True)
stone_500_rich =stone_500_richment [["Number","Year","Album","Artist","Gen","Subgen","playcount","listeners"]]

#exportamos el csv
stone_500_rich.to_csv("../data/stone_500_rich.csv")






