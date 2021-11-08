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
stone = pd.read_excel("data/musicbrainz.xlsx")

#cambiamos nombre columna title a album
stone.rename(columns={"Title": "Album"}, inplace = True)

#reemplazamos datos de album que tienen número a string
stone.Album = stone.Album.replace(1999,"1999",regex=True)
stone.Album = stone.Album.replace(21,"21",regex=True)
stone.Album = stone.Album.replace(1989,"1989",regex=True)

#quitamos espacios delate y detrás de strings
stone.Artist = stone.Artist.str.strip()
stone.Album = stone.Album.str.strip()
stone.Type = stone.Type.str.strip()
stone.Genre = stone.Genre.str.strip()

#creamos neuvas columnas a partir de Genre cogiendo solo un género y un subgénero
stone["Gen"] = stone["Genre"].str.extract(r"(^([^,])+)")[0]
stone["Subgenre"] = stone["Genre"].str.extract(r",\s*(.*$)")[0]


#limpieza de datos
stone.Album = stone.Album.str.replace(r'(Metallica...The Black Album..)',"Metallica",regex=True)
stone.Album = stone.Album.str.replace(r'(Eagles...st album.)',"Eagles",regex=True)
stone.Artist = stone.Artist.str.replace("Neil Young with Crazy Horse","Neil Young & Crazy Horse",regex=True)
stone.Artist = stone.Artist.str.replace(r"(Bob Dylan...The Band)","Bob Dylan And The Band",regex=True)
stone.Artist = stone.Artist.str.replace("Prince and The Revolution","Prince & The Revolution",regex=True)
stone.Artist = stone.Artist.str.replace(r"(Rufus...Chaka Khan)","Rufus",regex=True)
stone.Album = stone.Album.str.replace(r'(Proud Mary.*)',"Proud Mary",regex=True)

#creamos nuevas columnas de artista y album para crear urls
stone["Alb_url"] = stone["Album"]
stone["Art_url"] = stone["Artist"]

stone.Alb_url = stone.Alb_url.str.replace(r'(^The Beatles\s..The White Album..)',"The Beatles (Remastered)",regex=True)
stone.Alb_url = stone.Alb_url.str.replace(r'(^The Band...The Brown Album..)',"The Band",regex=True)

stone["Alb_url"] = stone["Alb_url"].str.replace("'","%27",regex=True)
stone["Alb_url"] = stone["Alb_url"].str.replace("&","%26",regex=True)
stone["Alb_url"] = stone["Alb_url"].str.replace(".","%2e",regex=True)
stone["Alb_url"] = stone["Alb_url"].str.replace("/","%2f",regex=True)
stone["Alb_url"] = stone["Alb_url"].str.replace("#","%23",regex=True)
stone["Alb_url"] = stone["Alb_url"].str.replace(r"(\s+)","%20",regex=True)

stone["Art_url"] = stone["Art_url"].str.replace("'","%27",regex=True)
stone["Art_url"] = stone["Art_url"].str.replace("&","%26",regex=True)
stone["Art_url"] = stone["Art_url"].str.replace(".","%2e",regex=True)
stone["Art_url"] = stone["Art_url"].str.replace("/","%2f",regex=True)
stone["Art_url"] = stone["Art_url"].str.replace("#","%23",regex=True)
stone["Art_url"] = stone["Art_url"].str.replace(r"(\s+)","%20",regex=True)


#exportamos tabla resultante
stone.to_csv("output/stone.csv")


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
#creamos el dataset
Spotify_500 = pd.DataFrame(Top_alb_spotify)
#exportamos datos a csv
Spotify_500.to_csv("output/Spotify_500.csv")


#LastFM 50 Top Artists

#usamos la api de Lastfm para extraer la información que ofrece sobre los 50 artistas más scrobbleados en su web
url_last50 = f"http://ws.audioscrobbler.com/2.0/?method=chart.gettopartists&api_key={apikey}&format=json"
req_last50 = requests.get(url_last50).json()
last_50 = req_last50["artists"]["artist"]

#creamos el dataframe la información de lastfm
last_top50 = pd.DataFrame.from_dict(last_50)

top50_lastfm = last_top50[['name', 'playcount', 'listeners']]

#exportamos el dataframe
top50_lastfm.to_csv("output/top50_lastfm.csv")


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
top_sells.to_csv("output/top_sells.csv")


# Top 500 albums Rolling Stones enrinquecimiento con LastFM
    ### carga de archivo limpio

stone = pd.read_csv("output/stone.csv")
stone = stone.drop("Unnamed: 0",axis=1)

#función llamada a api de lastfm que información de todos los albums en un dataframe
df = stone
colartista = "Art_url"
colalbum = "Alb_url"
Top500_last = apif.urls_llamadas(df, colartista, colalbum)

#creamos un dataframe con el resultado de la función
Top500_last_df = pd.DataFrame(Top500_last)

#concretamos el dataframe
Lastfm_500 = Top500_last_df[["artist","playcount","name","listeners"]]

#concretamos el dataframe original de rolling stone
stone_500 = stone[["Number","Year","Album","Artist","Gen"]]

#enriquecemos los datos de rolling stone con los de lastfm
stone_500_richment = stone_500.merge(Lastfm_500,left_index=True, right_index=True)
stone_500_rich =stone_500_richment [["Number","Year","Album","Artist","Gen","playcount","listeners"]]


#exportamos el csv
stone_500_rich.to_csv("output/stone_500_rich.csv")
