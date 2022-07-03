'''
En este modeulo se realiza la creación de los directorios
@author: Juan Camilo Cuartas
'''

import doctest
import os
import shutil

def create_data_lake():
    """Cree el data lake con sus capas.

    Esta función debe crear la carpeta `data_lake` en la raiz del proyecto. El data lake contiene
    las siguientes subcarpetas:

    ```
    .
    |
    ___ data_lake/
         |___ landing/
         |___ raw/
         |___ cleansed/
         ___ business/
              |___ reports/
              |    |___ figures/
              |___ features/
              |___ forecasts/

    ```
    """
    if os.path.isdir('data_lake'):
        shutil.rmtree('data_lake')

    rutas_directorios = ['data_lake/landing','data_lake/raw','data_lake/cleansed',\
        'data_lake/business/reports/figures','data_lake/business/features',\
        'data_lake/business/forecasts']

    for ruta in rutas_directorios:
        os.makedirs(ruta)

    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":

    doctest.testmod()
    create_data_lake()
