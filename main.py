from pyspark.sql import SparkSession

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.engine.Transformer import Transformer

import argparse

if __name__ == '__main__':
    spark: SparkSession = SparkSession \
        .builder \
        .master(SPARK_MODE) \
        .getOrCreate()
    transformer = Transformer(spark)

    pathfile = "/resources/data/players_21.csv"

    # lectura del csv
    df = transformer.read_input(pathfile)

    # seleccion de columnas especificas
    df_1 = transformer.column_selection(df)

    # agregar columna "player_cat" con regla señalada 
    df_2 = transformer.example_window_function(df_1)

    # agregar columna "PotentialVsOverall" con regla señalada
    df_3 = transformer.add_PotentialVsOverall_Column(df_2)

    # filtrar por columnas "player_cat" y "potential_vs_overall" con condiciones señaladas
    df_4 = transformer.clean_data(df_3)

    # agregar parametro 
    parser = argparse.ArgumentParser()
    parser.add_argument("--ngrams", help="de ser 1, los pasos unicamente sera para los jugadores menores de 23 años y en caso de ser 0, se aplicara para todos los jugadores del dataset")
    args = parser.parse_args()
    if args.ngrams:
        ngrams = args.ngrams

    if args.ngrams == 1:
        df_5 = transformer.filter_less_23(df_4)
    if args.ngrams == 0:
        df_5 = df_4

    # clean memory
    df.unpersist()
    df_1.unpersist()
    df_2.unpersist()
    df_3.unpersist()
    df_4.unpersist()

    # write output to parquet file
    df_5.coalesce(1).write.partitionBy("nationality").format("parquet").save("dataPartByNationality.parquet")
