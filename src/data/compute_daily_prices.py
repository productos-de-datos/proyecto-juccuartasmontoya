import pandas as pd

def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



    """
    
    precios_horarios = pd.read_csv('data_lake/cleansed/precios-horarios.csv', sep = ',')
    precios_diarios = precios_horarios.groupby(['fecha'])['precio'].mean() 
    precios_diarios = precios_diarios.reset_index()
    precios_diarios.to_csv('data_lake/business/precios-diarios.csv',index = False,  encoding='utf-8')
    
    
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    
    fechas = compute_daily_prices()


    import doctest
    doctest.testmod()
