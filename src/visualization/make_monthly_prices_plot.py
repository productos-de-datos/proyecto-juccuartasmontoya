'''
Módulo de creación de gráfica de precio mensual.
Mediante este módulo se genera la grafica de lineas con el precio promedio mensual
@author: Juan Camilo Cuartas
'''
import doctest
import pandas as pd
import matplotlib.pyplot as plt

def make_monthly_prices_plot():
    """Crea un grafico de lines que representa los precios promedios mensuales.

    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.

    El archivo se debe salvar en formato PNG en \
        data_lake/business/reports/figures/daily_prices.png.

    """

    precios_mensuales = pd.read_csv('data_lake/business/precios-mensuales.csv', sep = ',')
    precios_mensuales['fecha'] = pd.to_datetime(precios_mensuales['fecha'],\
         format="%Y-%m-%d %H:%M:%S")
    plt.plot(precios_mensuales['fecha'], precios_mensuales['precio'])
    plt.xlabel("fecha")
    plt.ylabel("Precio COP/kWh")
    plt.title("Evolución precio promedio mensual")
    plt.savefig('data_lake/business/reports/figures/monthly_prices.png')

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":

    make_monthly_prices_plot()
    doctest.testmod()
