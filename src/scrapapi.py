#función que extrae la información de una tabla.
def data_tab(tab):
    """
    hace scrapping por una tabla de la web de charset para extraer información
    devuelve una lista de diccionarios
    """
    Top_alb = []
    for s in tab.find_all("tr"):
        fila = [elemento for elemento in s.find_all("td")]
        if len(fila)>1:
            spot_dicc = {"Rank" : int(fila[0].text),
                        "Artist": fila[2].text.strip(),
                         "Album": fila[3].text.strip(),
                         "Total" :int(fila[4].text.replace(",","")),
                         "EAS" : int(fila[6].text.replace(",","")) #Equivalent Album Sales
                        }
            Top_alb.append(spot_dicc)
    return Top_alb


def data_scrap(tags):
    """
    crea una lista de diccionarios basada en un web escrapeada
    """
    lista = []
    for t in tags:
        dicc_t = {"art_alb": t.find("h2").getText(),
                 "certified units" : t.find("p").getText()}
        lista.append(dicc_t)
    return lista