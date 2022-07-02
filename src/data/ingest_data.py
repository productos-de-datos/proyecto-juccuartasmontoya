'''
Módulo de ingestión de datos.
@author: Juan Camilo Cuartas
'''

import urllib.request
import doctest

def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.

    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.

    """
    for num in range(1995,2022):
        if num in range(2016,2018):
            url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/ \
                precio_bolsa_nacional/xls/' + str(num) + '.xls?raw=true'
            urllib.request.urlretrieve(url, filename='data_lake/landing/' + str(num) + '.xls')

        else:
            url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/ \
                precio_bolsa_nacional/xls/' + str(num) + '.xlsx?raw=true'
            urllib.request.urlretrieve(url, filename='data_lake/landing/' + str(num) + '.xlsx')


   # raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    ingest_data()
    doctest.testmod()
    