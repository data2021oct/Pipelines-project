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
import tweepy
import numpy as np
import src.cleaningfun as cln
import src.scrapapi as scap


## LIMPIEZA
print("cargamos y limpiampos el dataset elegido")
stone = pd.read_excel("data/musicbrainzset.xlsx")
stone["Album"] = stone["Album"].apply(cln.num_string)

stone.Artist = stone.Artist.str.strip()
stone.Album = stone.Album.str.strip()
stone.Type = stone.Type.str.strip()
stone.Genre = stone.Genre.str.strip()

stone["Gen"] = stone["Genre"].str.extract(r"(^([^,])+)")[0]
stone["Subgenre"] = stone["Genre"].str.extract(r",\s*(.*$)")[0]

stone.Gen = stone.Gen.str.strip()
stone.Subgenre = stone.Subgenre.str.strip()
stone.Rating.fillna(0,inplace = True)
stone.Subgenre.fillna("None",inplace = True)

#hay algunos títulos de album que hay que cambiar de nombre
stone.Album = stone.Album.str.replace(r'(Metallica...The Black Album..)',"Metallica",regex=True)
stone.Album = stone.Album.str.replace(r'(Eagles...st album.)',"Eagles",regex=True)
stone.Artist = stone.Artist.str.replace("Neil Young with Crazy Horse","Neil Young & Crazy Horse",regex=True)
stone.Artist = stone.Artist.str.replace(r"(Bob Dylan...The Band)","Bob Dylan And The Band",regex=True)
stone.Artist = stone.Artist.str.replace(r"(Prince\s+.*)","Prince & The Revolution",regex=True)
stone.Artist = stone.Artist.str.replace(r"(Rufus...Chaka Khan)","Rufus",regex=True)
stone.Album = stone.Album.str.replace(r'(Proud Mary.*)',"Best Of / Proud Mary",regex=True)
stone.Artist = stone.Artist.str.replace(r'(.*The Velvet Underground.*)',"The Velvet Underground",regex= True)
stone.Artist = stone.Artist.str.replace("‐","-",regex = True)
stone.Album = stone.Album.str.replace("‐","-",regex = True)
stone.Gen = stone.Gen.str.replace("piunk","punk",regex = True)
stone.Gen = stone.Gen.str.replace("punk rock","punk",regex = True)
stone.Gen = stone.Gen.str.replace("folk rock","folk",regex = True)

#creamos nuevas columnas que haremos compatibles con url
stone["Alb_url"] = stone["Album"]
stone["Art_url"] = stone["Artist"]

#algunos nombres de discos hay que cambiarlos
stone.Alb_url = stone.Alb_url.str.replace(r'(^The Beatles\s..The White Album..)',"The Beatles (Remastered)",regex=True)
stone.Alb_url = stone.Alb_url.str.replace(r'(^The Band...The Brown Album..)',"The Band",regex=True)
stone.Alb_url = stone.Alb_url.str.replace(r'(^Sign.*the Times$)',"Sign 'O' The Times",regex=True)

df = stone
colum = "Alb_url"
cln.url_prep(df,colum)
colum = "Art_url"
cln.url_prep(df,colum)

#Principal Enrichment
df = stone
art = "Art_url"
alb = "Alb_url"
print("take your time, 500 request to lastfm api ahead")
Top500_last = cln.urls_llamadas (df,art,alb)

stone.to_csv("output/stone.csv",index=False)

Top500_last_df = pd.DataFrame(Top500_last)
Top500_last_df[["playcount","listeners"]] = Top500_last_df[["playcount","listeners"]].astype("int64")
Lastfm_500 = Top500_last_df[["artist","playcount","name","listeners"]]
stone_500 = stone[["Number","Year","Album","Artist","Type","Rating","Gen"]]
stone_500_richment = stone_500.merge(Lastfm_500,left_index=True, right_index=True)
stone_500_rich =stone_500_richment [["Number","Year","Album","Artist","Type","Rating","Gen","playcount","listeners"]]

stone_500_rich.to_csv("output/stone_500_rich.csv",index=False)

print("we start scraping and conecting with apis")
## SCRAPING // API
apikey = os.getenv("apikey") #apikey de lastfm
stone_500_rich = pd.read_csv("data/stone_500_rich.csv") 

#spotify 
print("conecting to chartmasters")
url_spotify = "https://chartmasters.org/spotify-most-streamed-albums/?y=alltime"
html_spotify = requests.get(url_spotify)
soup_spotify = BeautifulSoup(html_spotify.content,"html.parser")

tablas_spotify = soup_spotify.findAll("table")
spotify = tablas_spotify[1]

tab = spotify
Top_alb_spotify = scap.data_tab(tab)

Spotify_500 = pd.DataFrame(Top_alb_spotify)
Spotify_500.to_csv("output/Spotify_500.csv",index = False) 

stone_last_spot= stone_500_rich.merge(Spotify_500, how='left', on = ["Album","Artist"])

stone_last_spot.Rank.fillna(0, inplace=True)
stone_last_spot.Total.fillna(0, inplace=True)
stone_last_spot.EAS.fillna(0, inplace=True)

stone_last_spot[["Rank","Total","EAS"]] = stone_last_spot[["Rank","Total","EAS"]].astype("int64")

stone_last_spot.to_csv("output/Spotify_500.csv",index = False) 

#lastfm 50 artists
print("conecting to lastfm")
url_last50 = f"http://ws.audioscrobbler.com/2.0/?method=chart.gettopartists&api_key={apikey}&format=json"
req_last50 = requests.get(url_last50).json()
last_50 = req_last50["artists"]["artist"]
last_top50 = pd.DataFrame.from_dict(last_50)

last_top50.rename(columns={"name": "Artist"}, inplace = True)
last_top50[["playcount","listeners"]] = last_top50[["playcount","listeners"]].astype("int64")
top_50_last = last_top50[["Artist","playcount","listeners"]]
top_50_last.to_csv("output/top_50_last.csv",index = False) 

stone = pd.read_csv("output/stone.csv")
stone_red = stone[["Number","Artist","Album","Rating","Genre"]]
stone_Artist = stone_red.groupby(["Artist"]).agg({"Number":"min","Album":"count","Rating":"mean"}).sort_values("Number", ascending = True).reset_index()
stone_Artist.Rating = round(stone_Artist.Rating, 2)
stone_Artist_last = stone_Artist.merge(top_50_last, how = "left", on= "Artist")
stone_Artist_last.playcount.fillna(0,inplace = True)
stone_Artist_last.listeners.fillna(0,inplace = True)
stone_Artist_last[["playcount","listeners"]] = stone_Artist_last[["playcount","listeners"]].astype("int64")

stone_Artist_last.to_csv("output/stone_Artist_last.csv",index = False)

#topsells
print("conecting to bussinesinsider")
url_insider = "https://www.businessinsider.com/50-best-selling-albums-all-time-2016-9"
html_insider = requests.get(url_insider)
soup_insider = BeautifulSoup(html_insider.content,"html.parser")
tags_insider = soup_insider.find_all("div", {"class": "slide-layout"})

insider_data = scap.data_scrap(tags_insider)
insider = pd.DataFrame(insider_data)

#limpieza nuevo dataFrame
insider["Rank"] = insider["art_alb"].str.extract(r"(^\d+)")
insider["Millions"] = insider["certified units"].str.extract(r"(\d+)")
insider["art_alb"] = insider["art_alb"].str.replace(" – "," — ",regex = True)
insider["Artist"] = insider["art_alb"].str.extract(r"(^\d+.\s(.+?)—)")[1]
insider["Album"] = insider["art_alb"].str.extract(r"(—\s(.+?)$)")[1]
insider["Album"] = insider["Album"].str.replace('"',"")
insider["Artist"] = insider["Artist"].str.strip()
insider["Album"] = insider["Album"].str.strip()

insider[["Millions","Rank"]] = insider[["Millions","Rank"]].astype("int64")
top_sells = insider[["Rank","Artist","Album","Millions"]]
top_sells= top_sells.sort_values("Millions",ascending = False)

top_sells["Album"] = top_sells["Album"].str.replace(r"(^The Beatles..The White\s.*)","The Beatles",regex = True)

top_sells.to_csv("output/top_sells.csv",index=False)

top_sell_rs = top_sells.merge(stone_500_rich, how = "left", on=["Artist","Album"])

top_sell_rs.Number.fillna(0,inplace = True)
top_sell_rs.Year.fillna(0,inplace = True)
top_sell_rs.Type.fillna("unknown",inplace = True)
top_sell_rs.Rating.fillna(0,inplace = True)
top_sell_rs.Gen.fillna("none",inplace = True)
top_sell_rs.playcount.fillna(0,inplace = True)
top_sell_rs.listeners.fillna(0,inplace = True)
top_sell_rs[["Number","Year","playcount","listeners"]] = top_sell_rs[["Number","Year","playcount","listeners"]].astype("int64")

top_sell_rs.to_csv("output/top_sell_rs.csv",index=False)

stone_last_spot_sells = stone_last_spot.merge(top_sells,how = "left", on=["Artist","Album"])
stone_last_spot_sells = stone_last_spot_sells.rename(columns = {"Rank_x":"Spotify","Rank_y":"Sells"})
stone_last_spot_sells.Sells.fillna(0,inplace = True)
stone_last_spot_sells.Millions.fillna(0,inplace = True)

stone_last_spot_sells[["Sells","Millions"]] = stone_last_spot_sells[["Sells","Millions"]].astype("int64")

stone_last_spot_sells["In_Spot"] = np.where(stone_last_spot_sells["Spotify"],"Yes","No")
stone_last_spot_sells["In_Sells"] = np.where(stone_last_spot_sells["Sells"],"Yes","No")

stone_last_spot_sells.to_csv("output/stone_last_spot_sells.csv",index=False)

RS_Artists = stone_500_rich.groupby("Artist").agg({"Album":"count","playcount":"sum","listeners":"sum"}).reset_index()
RS_Artists.to_csv("output/RS_Artists.csv",index = False) 

