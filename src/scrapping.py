
import requests
from bs4 import BeautifulSoup
import requests 
import json
import os
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize


def data_spotify(element):
    """
    recibe un b64.element  que contiene tags de la web chartmasters
    scrapea por web chartmasters para sacar información sobre las reproducciones de spotify
    devuelve una lista de diccionarios
    cada diccionario contiene información de uno de los discos más escuchados en spotify
    """
    Top_alb = []
    for s in element.find_all("tr"):
        fila = [elemento for elemento in s.find_all("td")]
        if len(fila)>1:
            spot_dicc = {"Rank" : int(fila[0].text),
                        "Artist": fila[2].text.strip(),
                         "Album": fila[3].text.strip(),
                         "Total" :int(fila[4].text.replace(",","")),
                         "Daily" : int(fila[5].text.replace(",",""))
                        }
            Top_alb.append(spot_dicc)
    return Top_alb


def data_sells(element):
    """
    recibe un b64.element  que contiene tags de la web chartmasters
    scrapea por web insider 
    obtiene una lista de diccionarios
    cada diccionario tiene la información de "album" y "unidades vendidas"
    """
    lista = []
    for t in element:
        dicc_t = {"album": t.find("h2").getText(),
                    "certified units" : t.find("p").getText()}
        lista.append(dicc_t)
    return lista