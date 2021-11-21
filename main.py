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
import src.cleaningfun as cln
#import src.apifunc as apif


## LIMPIEZA
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

