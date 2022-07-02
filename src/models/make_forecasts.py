'''
Módulo de pronóstico del modelo.
Mediante este módulo se entrena el modelo red neuronal
@author: Juan Camilo Cuartas
'''
import doctest
import pickle
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

def make_forecasts():
    """Construya los pronosticos con el modelo entrenado final.

    Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:

    * La fecha.

    * El precio promedio real de la electricidad.

    * El pronóstico del precio promedio real.


    """
    precios_diarios = pd.read_csv('data_lake/business/precios-diarios.csv', sep = ',')

    data_d1 = [precios_diarios['precio'][t] - precios_diarios['precio'][t - 1]\
         for t in range(1, len(precios_diarios['precio']))]
    data_d1d12 = [data_d1[t] - data_d1[t - 12] for t in range(12, len(data_d1))]
    scaler = MinMaxScaler()
    data_d1d12_scaled = scaler.fit_transform(np.array(data_d1d12).reshape(-1, 1))
    data_d1d12_scaled = [u[0] for u in data_d1d12_scaled]
    valor_p = 13

    lista_x = []
    for valor_t in range(valor_p - 1, len(data_d1d12_scaled) - 1):
        lista_x.append([data_d1d12_scaled[valor_t - n] for n in range(valor_p)])

    filename = 'modeles/precios-diarios.pkl'
    with open(filename, 'rb') as modelo:
        loaded_model = pickle.load(modelo)

    y_d1d12_scaled_m2 = loaded_model.predict(lista_x)
    y_d1d12_scaled_m2 = data_d1d12_scaled[0:valor_p] + y_d1d12_scaled_m2.tolist()

    y_d1d12_m2 = scaler.inverse_transform([[u] for u in y_d1d12_scaled_m2])
    y_d1d12_m2 = [u[0] for u in y_d1d12_m2.tolist()]

    y_d1_m2 = [y_d1d12_m2[t] + data_d1[t] for t in range(len(y_d1d12_m2))]
    y_d1_m2 = data_d1[0:12] + y_d1_m2

    y_m2 = [y_d1_m2[t] + precios_diarios['precio'][t] for t in range(len(y_d1_m2))]
    y_m2 = [precios_diarios['precio'][0]] + y_m2
    precios_diarios['pronostico'] = y_m2

    precios_diarios.to_csv('data_lake/business/forecasts/precios-diarios.csv'\
        ,index = False,  encoding='utf-8')

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":

    make_forecasts()
    doctest.testmod()
