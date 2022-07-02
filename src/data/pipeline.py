'''
Módulo de pipeline.
Mediante este módulo se generan los proceso desarrollados anteriormente
@author: Juan Camilo Cuartas

Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.


'''
import doctest
import urllib.request
import luigi
import pandas as pd

class GetListFiles(luigi.Task):
    """
    Download files from github repository and save in local directory
    """

    def output(self):
        return luigi.LocalTarget("data_lake/file_list.txt")

    def run(self):

        repositorio = 'https://github.com/jdvelasq/datalabs/'
        ruta = repositorio + 'blob/master/datasets/precio_bolsa_nacional/xls/'
        with self.output().open("w") as file:

            for num in range(1995,2022):
                if num in range(2016,2018):
                    url = ruta + str(num) + '.xls?raw=true\n'
                    urllib.request.urlretrieve(url, filename='data_lake/landing/' \
                        + str(num) + '.xls')
                    file.write(url)

                else:
                    url = ruta + str(num) + '.xlsx?raw=true\n'
                    urllib.request.urlretrieve(url, filename='data_lake/landing/' \
                        + str(num) + '.xlsx')
                    file.write(url)

class TransforData(luigi.Task):
    """
    Transform the files from xlsx to csv
    """

    def requires(self):
        return GetListFiles()

    def output(self):
        return luigi.LocalTarget("data_lake/file_list2.txt")

    def run(self):

        with self.output().open("w") as file:

            file.write('Inicio')
            for num in range(1995,2022):

                if num in range(1995,2000):
                    data_xls = pd.read_excel('data_lake/landing/' + str(num) \
                        + '.xlsx', index_col=0, header = 3)
                    data_xls.to_csv('data_lake/raw/' + str(num) + '.csv', encoding='utf-8')
                elif num in range(2000,2016):
                    data_xls = pd.read_excel('data_lake/landing/' + str(num) \
                        + '.xlsx', index_col=0, header = 2)
                    if num in [2000,2005,2010,2015]:
                        data_xls = data_xls.drop('Version', axis=1)
                    if num == 2011:
                        data_xls = data_xls.drop(data_xls.columns[[25]], axis=1)
                        data_xls = data_xls.drop(['Version'], axis=1)
                    if num in [2012,2013,2014]:
                        data_xls = data_xls.drop(['Version','Unnamed: 26'], axis=1)
                    data_xls.to_csv('data_lake/raw/' + str(num) + '.csv', encoding='utf-8')
                elif num in range(2016,2018):
                    data_xls = pd.read_excel('data_lake/landing/' + str(num) \
                        + '.xls', index_col=0, header = 2)
                    if num in [2016,2017]:
                        data_xls = data_xls.drop('Version', axis=1)
                    data_xls.to_csv('data_lake/raw/' + str(num) + '.csv', encoding='utf-8')
                else:
                    data_xls = pd.read_excel('data_lake/landing/' \
                        + str(num) + '.xlsx', index_col=0)
                    data_xls.to_csv('data_lake/raw/' + str(num) \
                        + '.csv', encoding='utf-8')


class CleanData(luigi.Task):
    """
    Create the files precios-horarios.csv, precios-diarios.csv y precios-mensuales.csv
    """

    def requires(self):
        return TransforData()

    def output(self):
        return luigi.LocalTarget("data_lake/file_list3.txt")

    def run(self):

        with self.output().open("w") as file:

            file.write('Inicio')
            precio_bolsa_total =  pd.DataFrame()

            for num in range(1995,2022):
                archivo_precio_bolsa = pd.read_csv('data_lake/raw/' \
                    + str(num) + '.csv', sep = ',')
                precio_bolsa_total = pd.concat([precio_bolsa_total, archivo_precio_bolsa])

            precio_bolsa_total = precio_bolsa_total.reset_index(drop=True)
            precio_bolsa_total = precio_bolsa_total.fillna(0)
            precio_bolsa_total = precio_bolsa_total[precio_bolsa_total['Fecha'] != 0]
            precios_horarios = pd.melt(precio_bolsa_total, id_vars=['Fecha'])
            precios_horarios = precios_horarios.rename(columns={'Fecha':'fecha',\
                'variable':'hora','value':'precio'})

            precios_horarios.to_csv('data_lake/cleansed/precios-horarios.csv',\
                index = False,  encoding='utf-8')

            precios_horarios = pd.read_csv('data_lake/cleansed/precios-horarios.csv', sep = ',')
            precios_diarios = precios_horarios.groupby(['fecha'])['precio'].mean()
            precios_diarios = precios_diarios.reset_index()
            precios_diarios.to_csv('data_lake/business/precios-diarios.csv',\
                index = False,  encoding='utf-8')

            precios_horarios = pd.read_csv('data_lake/cleansed/precios-horarios.csv', sep = ',')
            precios_horarios[['ano', 'mes', 'dia']] = \
                precios_horarios['fecha'].str.split('-', expand=True)
            precios_horarios['fecha2'] = precios_horarios['ano'] + '-' +\
                 precios_horarios['mes'] + '-01'
            precios_horarios = precios_horarios.drop(['fecha','ano','mes','dia','hora'], axis=1)
            precios_mensuales = precios_horarios.groupby(['fecha2'])['precio'].mean()
            precios_mensuales = precios_mensuales.reset_index()
            precios_mensuales = precios_mensuales.rename(columns={'fecha2':'fecha'})
            precios_mensuales.to_csv('data_lake/business/precios-mensuales.csv',\
                index = False,  encoding='utf-8')


if __name__ == "__main__":

    luigi.run(["CleanData", "--local-scheduler"])
    doctest.testmod()
