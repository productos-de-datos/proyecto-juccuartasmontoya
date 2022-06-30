import pickle
from sklearn.neural_network import MLPRegressor
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import numpy as np


def train_daily_model():
    """Entrena el modelo de pronóstico de precios diarios.

    Con las features entrene el modelo de proóstico de precios diarios y
    salvelo en models/precios-diarios.pkl


    """

    precios_diarios = pd.read_csv('data_lake/business/precios-diarios.csv', sep = ',')

    scaler = MinMaxScaler()

    # escala la serie
    data_scaled = scaler.fit_transform(np.array(precios_diarios['precio']).reshape(-1, 1))

    # z es un array de listas como efecto
    # del escalamiento
    data_scaled = [u[0] for u in data_scaled]

    data_d1 = [precios_diarios['precio'][t] - precios_diarios['precio'][t - 1] for t in range(1, len(precios_diarios['precio']))]

    data_d1d12 = [data_d1[t] - data_d1[t - 12] for t in range(12, len(data_d1))]

    data_d1d12_scaled = scaler.fit_transform(np.array(data_d1d12).reshape(-1, 1))
    data_d1d12_scaled = [u[0] for u in data_d1d12_scaled]


    P = 13

    X = []
    for t in range(P - 1, len(data_d1d12_scaled) - 1):
        X.append([data_d1d12_scaled[t - n] for n in range(P)])

    d = data_d1d12_scaled[P:]

    H = 4  # Se escoge arbitrariamente

    np.random.seed(123456)

    mlp = MLPRegressor(
        hidden_layer_sizes=(H,),
        activation="logistic",
        learning_rate="adaptive",
        momentum=0.0,
        learning_rate_init=0.002,
        max_iter=100000,
    )

    # Entrenamiento
    mlp.fit(X[0:9391], data_d1d12_scaled[0:9391]) 

    filename = 'modeles/precios-diarios.pkl'
    pickle.dump(mlp, open(filename, 'wb'))


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    train_daily_model()

    doctest.testmod()
