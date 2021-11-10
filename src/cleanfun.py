def num_string(dato):
    """[summary]

    Args:
        dato (int): [recib el dato de una columna de panas]

    Returns:
        [str]: [si el dato es un entero, lo convierte en string.]
    """
    if type(dato)== int:
        return str(dato)
    



def url_prep(df, colum):
    """
    This function changes elements of a string to make them compatible with urls

    Args:
        df = DataFrame
        colum = Column of the DataFrame
    """
    url_replace = {"'":"%27", "&":"%26",".":"%2e","/":"%2f","#":"%23",r"(\s+)":"%20" }
    for key,value in url_replace.items():
        df[colum] = df[colum].str.replace(key,value,regex=True)