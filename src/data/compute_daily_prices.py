'''
Módulo de transformación de datos.
Mediante este genera el archivo precios-diarios.csv
@author: Juan Camilo Cuartas
'''
import doctest
import pandas as pd

def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



    """
    precios_horarios = pd.read_csv('data_lake/cleansed/precios-horarios.csv',\
         sep = ',')
    precios_diarios = precios_horarios.groupby(['fecha'])['precio'].mean()
    precios_diarios = precios_diarios.reset_index()
    precios_diarios.to_csv('data_lake/business/precios-diarios.csv',\
        index = False,  encoding='utf-8')

    #raise NotImplementedError("Implementar esta función")

def test_values_prices():
    precios = pd.read_csv('data_lake/business/precios-diarios.csv').head()

    assert precios['precio'].head().to_list() == [
        1.3507083333333334,
        4.924333333333333,
        1.2695,
        0.9530833333333332,
        4.305916666666667
    ]

if __name__ == "__main__":
    compute_daily_prices()
    test_values_prices()
    doctest.testmod()
