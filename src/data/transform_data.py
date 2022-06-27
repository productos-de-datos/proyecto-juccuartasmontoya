


def transform_data():
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """

    #data_xls = pd.read_excel('data_lake/landing/1995.xlsx', index_col=0, header = 3)
    #data_xls.to_csv('data_lake/raw/1995.csv', encoding='utf-8')

    for num in range(1995,2022):

        if num in range(1995,2000):
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=0, header = 3)
            data_xls.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8')
        elif num in range(2000,2016):
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=0, header = 2)
            if num in [2000,2005,2010,2015]:
                data_xls = data_xls.drop('Version', axis=1)
            if num == 2011:
                data_xls = data_xls.drop(data_xls.columns[[25]], axis=1)
                data_xls = data_xls.drop(['Version'], axis=1)
            if num in [2012,2013,2014]:
                data_xls = data_xls.drop(['Version','Unnamed: 26'], axis=1)
            data_xls.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8')
        elif num in range(2016,2018):
            data_xls = pd.read_excel('data_lake/landing/{}.xls'.format(num), index_col=0, header = 2)
            if num in [2016,2017]:
                data_xls = data_xls.drop('Version', axis=1)
            data_xls.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8')
        else:
            data_xls = pd.read_excel('data_lake/landing/{}.xlsx'.format(num), index_col=0)
            data_xls.to_csv('data_lake/raw/{}.csv'.format(num), encoding='utf-8')



    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    
    
    import doctest
    import pandas as pd

    transform_data()
    
    doctest.testmod()
