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
import luigi
import ingest_data
import transform_data
import clean_data
import compute_daily_prices
import compute_monthly_prices

class GetListFiles(luigi.Task):
    """
    Download files from github repository and save in local directory
    """

    def output(self):
        return luigi.LocalTarget("data_lake/file_list.txt")

    def run(self):

        with self.output().open("w") as file:
            ingest_data.ingest_data()
            file.write('Finaliza Carga')


class TransforData(luigi.Task):
    """
    Transform the files from xlsx to csv
    """

    def requires(self):
        return GetListFiles()

    def output(self):
        return luigi.LocalTarget("data_lake/file_list.txt")

    def run(self):

        with self.output().open("w") as file:
            transform_data.transform_data()
            file.write('Inicio')

class CleanData(luigi.Task):
    """
    Create the files precios-horarios.csv, precios-diarios.csv y precios-mensuales.csv
    """

    def requires(self):
        return TransforData()

    def output(self):
        return luigi.LocalTarget("data_lake/file_list.txt")

    def run(self):

        with self.output().open("w") as file:
            clean_data.clean_data()
            compute_daily_prices.compute_daily_prices()
            compute_monthly_prices.compute_monthly_prices()
            file.write('Inicio')


if __name__ == "__main__":

    luigi.run(["CleanData", "--local-scheduler"])
    doctest.testmod()
