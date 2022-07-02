'''
Módulo de transformación de datos.
Mediante este módulo se transforman los archivos en formato XLSX a formato CSV
@author: Juan Camilo Cuartas
'''
import doctest
import pandas as pd

def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo
    data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.


    """
    precio_bolsa_total =  pd.DataFrame()

    for num in range(1995,2022):
        archivo_precio_bolsa = pd.read_csv('data_lake/raw/' + str(num) + '.csv', sep = ',')
        precio_bolsa_total = pd.concat([precio_bolsa_total, archivo_precio_bolsa])


    precio_bolsa_total = precio_bolsa_total.reset_index(drop=True)
    precio_bolsa_total = precio_bolsa_total.fillna(0)
    precio_bolsa_total = precio_bolsa_total[precio_bolsa_total['Fecha'] != 0]
    precios_horarios = pd.melt(precio_bolsa_total, id_vars=['Fecha'])
    precios_horarios = precios_horarios.rename(columns={'Fecha':'fecha',\
        'variable':'hora','value':'precio'})

    precios_horarios.to_csv('data_lake/cleansed/precios-horarios.csv',\
        index = False,  encoding='utf-8')
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    clean_data()
    doctest.testmod()
