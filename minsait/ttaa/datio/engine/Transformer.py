import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer


class Transformer(Writer):
    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df.printSchema()
        df = self.clean_data(df)
        df = self.example_window_function(df)
        df = self.column_selection(df)

        # for show 100 records after your transformations and show the DataFrame schema
        df.show(n=100, truncate=False)
        df.printSchema()

        # Uncomment when you want write your final output
        self.write(df)

    def read_input(self, pathfile) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option("csv") \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(pathfile)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column player_cat == "A" | player_cat == "B"
        column player_cat == "C" & potential_vs_overall > 1.15
        column player_cat == "D" & potential_vs_overall > 1.25       
        """
        df = df.filter(('player_cat == "A"' | 'player_cat == "b"' )  \
                 .filter('player_cat == "C"' & "potential_vs_overall > 1.15" ) \
                     .filter('player_cat == "D"' & "potential_vs_overall > 1.25")
        )
        return df

    def column_selection(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            long_name.column(),
            age.column(),
            height_cm.column(),
            weight_kg.column(),
            nationality.column(),
            club_name.column(),
            overall.column(),
            potential.column(),
            team_position.column()
        )
        return df

    def example_window_function(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have nationality and team_position columns)
        :return: add to the DataFrame the column "player_cat"
             by each position value
             cat A for if is in the best 3 players of its nationality
             cat B for if is in the best 5 players of its nationality
             cat C for if is in the best 10 players of its nationality
             cat D for the rest
        """
        w: WindowSpec = Window \
            .partitionBy(nationality.column(), team_position.column()) \
            .orderBy(overall.column())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank >= 3, "A") \
            .when(rank >= 5, "B") \
            .when(rank >= 10, "C") \
            .otherwise("D")

        df = df.withColumn("player_cat", rule)
        return df

    def add_PotentialVsOverall_Column(self, df: DataFrame) -> DataFrame:
        df = df.witchColumn("potential_vs_overall", potential.column() / overall.column())

    def filter_less_23(self, df: DataFrame) -> DataFrame:
        df = df.filter(age.colun()<23)