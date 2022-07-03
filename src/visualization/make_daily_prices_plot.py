'''
Módulo de creación de gráfica de precio diario.
Mediante este módulo se genera la grafica de lineas con el precio promedio diario
@author: Juan Camilo Cuartas
'''
import doctest
import pandas as pd
import matplotlib.pyplot as plt

def make_daily_prices_plot():
    """Crea un grafico de lines que representa los precios promedios diarios.

    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.

    El archivo se debe salvar en formato PNG en
    data_lake/business/reports/figures/daily_prices.png.

    """

    precios_diarios = pd.read_csv('data_lake/business/precios-diarios.csv', \
        sep = ',')
    precios_diarios['fecha'] = pd.to_datetime(precios_diarios['fecha'], format="%Y-%m-%d %H:%M:%S")
    plt.plot(precios_diarios['fecha'], precios_diarios['precio'])
    plt.xlabel("fecha")
    plt.ylabel("Precio COP/kWh")
    plt.title("Evolución precio promedio diario")
    plt.savefig('data_lake/business/reports/figures/daily_prices.png')

    #raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":

    make_daily_prices_plot()
    doctest.testmod()
