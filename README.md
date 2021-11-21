<img src= "images/RSlogo.png"> <img src= "images/lastfm.png">


# Crítica vs. Público

## Objetivo:

Crear un pipeline que ejecute la limpieza de un archivo de datos y enriquecimiento con datos consguidos a través de scraping y llamando a apis.

## Hipótesis

El público nunca está de acuerdo con la crítica.

Estudiamos la lista de los 500 mejores discos de la historia según la publicación Rolling Stone. [Artículo](https://www.rollingstone.com/music/music-lists/best-albums-of-all-time-1062063/)
Cargamos los datos de esta lista con un archivo excel conseguido a través de [muscicbrainz](https://musicbrainz.org/series/6a4b53b9-2756-4afe-93f2-306039d41910)

Enriqueceremos estos datos con las siguentes webs:

- Lista de los discos más reproducidos en Spotify, conseguida a través de la web de [Chartmasters](https://chartmasters.org/spotify-most-streamed-albums/?y=alltime). La última actualización data del 11/07/21. (Scrapping)

- De la web Insider hemos conseguido los datos de los 50 álbumes más vendidos de todos los tiempos. [Artículo](https://www.businessinsider.com/50-best-selling-albums-all-time-2016-9). (Scrapping)

- Hemos conectado con las apis de la red social musical de [Lastfm](https://www.last.fm/api/scrobbling) y hemos conseguido los siguientes datos:
    - El top 50 de los álbumes más escuchados por sus usuarios
    - El número total de usuarios y reproducciones que se han registrado de los 500 álbumes del artículo de Rolling Stone





## Contenido:

- Carpeta Data: varios csv de carga y exportación de datos. 
- Carpeta Images: logos para el readme + exportación de los gráficos creados con jupyter Notebooks
- Carpeta Notebook con los archivos de prueba de manipulación de datos:
    - limpieza: carga de datos, limpieza y primer enriquecimiento
    - scra_api: con la extración de datos a través de apis y scrapping
    - visualización: estudio de los datos a través de gráficos. (ver con nbviewer: [visualización](https://nbviewer.org/github/data2021oct/Pipelines-project/blob/main/notebooks/visualization.ipynb))
    
- Carpeta Output: archivos creados al ejectuar el archivo main.py
- Carpeta src:
    - archivo apifunc: funciones que se ejecutan para llamar a las apis
    - archivo scrappingfunc.py: funciones que se ejecutan para scrapear por las webs
- Archivo main.py (ejecutables del pipeline)