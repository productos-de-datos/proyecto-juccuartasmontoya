'''
Módulo de creación de archivos insumos para el modelo.
Mediante este módulo se genera el archivo requerido con los insumos para el pronóstico
@author: Juan Camilo Cuartas
'''
import doctest
import pandas as pd

def make_features():
    """Prepara datos para pronóstico.

    Cree el archivo data_lake/business/features/precios-diarios.csv. Este
    archivo contiene la información para pronosticar los precios diarios de la
    electricidad con base en los precios de los días pasados. Las columnas
    correspoden a las variables explicativas del modelo, y debe incluir,
    adicionalmente, la fecha del precio que se desea pronosticar y el precio
    que se desea pronosticar (variable dependiente).

    En la carpeta notebooks/ cree los notebooks de jupyter necesarios para
    analizar y determinar las variables explicativas del modelo.

    """

    precios_diarios = pd.read_csv('data_lake/business/precios-diarios.csv',\
         sep = ',')
    precios_diarios['Variable_dependiente'] = 0
    for i in range(9393,9417):
        precios_diarios.iloc[i,2] = precios_diarios.iloc[i,1]
        precios_diarios.iloc[i,1] = 0

    precios_diarios.to_csv('data_lake/business/features/precios_diarios.csv',\
        index = False,  encoding='utf-8')

    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":

    make_features()
    doctest.testmod()
