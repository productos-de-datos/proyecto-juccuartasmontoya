'''
Módulo de transformación de datos.
Mediante este genera el archivo precios-mensuales.csv
@author: Juan Camilo Cuartas
'''
import doctest
import pandas as pd

def compute_monthly_prices():
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
    precios_horarios = pd.read_csv('data_lake/cleansed/precios-horarios.csv', sep = ',')
    precios_horarios[['ano', 'mes', 'dia']] = precios_horarios['fecha'].str.split('-', expand=True)
    precios_horarios['fecha2'] = precios_horarios['ano'] + '-' + precios_horarios['mes'] + '-01'
    precios_horarios = precios_horarios.drop(['fecha','ano','mes','dia','hora'], axis=1)
    precios_mensuales = precios_horarios.groupby(['fecha2'])['precio'].mean()
    precios_mensuales = precios_mensuales.reset_index()
    precios_mensuales = precios_mensuales.rename(columns={'fecha2':'fecha'})
    precios_mensuales.to_csv('data_lake/business/precios-mensuales.csv',\
        index = False,  encoding='utf-8')

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    compute_monthly_prices()
    doctest.testmod()
