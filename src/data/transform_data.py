'''
Módulo de transformación de datos.
Mediante este módulo se transforman los archivos en formato XLSX a formato CSV
@author: Juan Camilo Cuartas
'''
import doctest
import pandas as pd

def charge_file(anio, formato, fila):
    """Cargar archivo xlsx y xls.

    Esta función carga los archivos en formato xlsx y xls y
    lo asigna al dataframe data_xls
    """
    data_xls = pd.read_excel('data_lake/landing/' + str(anio) + formato, \
                index_col=0, header = fila)
    return data_xls

def transform_data():
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """
    for num in range(1995,2022):
        if num in range(1995,2000):
            data_xls = charge_file(num, '.xlsx', 3)
            data_xls.to_csv('data_lake/raw/' + str(num) + '.csv', encoding='utf-8')
        elif num in range(2000,2016):
            data_xls = charge_file(num, '.xlsx', 2)
            if num in [2000,2005,2010,2015]:
                data_xls = data_xls.drop('Version', axis=1)
            if num == 2011:
                data_xls = data_xls.drop(data_xls.columns[[25]], axis=1)
                data_xls = data_xls.drop(['Version'], axis=1)
            if num in [2012,2013,2014]:
                data_xls = data_xls.drop(['Version','Unnamed: 26'], axis=1)
            data_xls.to_csv('data_lake/raw/' + str(num) + '.csv', encoding='utf-8')
        elif num in range(2016,2018):
            data_xls = charge_file(num, '.xls', 2)
            if num in [2016,2017]:
                data_xls = data_xls.drop('Version', axis=1)
            data_xls.to_csv('data_lake/raw/' + str(num) + '.csv', encoding='utf-8')
        else:
            data_xls = charge_file(num, '.xlsx', 0)
            data_xls.to_csv('data_lake/raw/' + str(num) + '.csv', encoding='utf-8')

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    transform_data()
    doctest.testmod()
