from chispa.dataframe_comparer import *
from collections import namedtuple
from pyspark.sql import types as T

from ..jobs.player_game_edge_job import do_player_game_edge_transformation

# Define namedtuples for input and output schemas
PlayerGame = namedtuple("PlayerGame", [
    "game_id", "team_id", "team_abbreviation", "team_city", "player_id",
    "player_name", "nickname", "start_position", "comment", "min",
    "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta",
    "ft_pct", "oreb", "dreb", "reb", "ast", "stl", "blk", "TO", "pf",
    "pts", "plus_minus"
])
PlayerGameEdge = namedtuple("PlayerGameEdge", [
    "subject_identifier", "subject_type", "object_identifier", "object_type",
    "edge_type", "properties"
])

def test_player_edge_transformation(spark):
    # Input data mimicking game_details with a duplicate row
    input_data = [
        PlayerGame(
            22000690,
            1610612742,
            "DAL",
            "Dallas",
            1630182,
            "Josh Green",
            None,
            "G",
            None,
            "8:32",
            3.0,5.0,0.6,0.0,1.0,0.0,
            1.0, 2.0, 0.5, 1.0, 0.0, 1.0,
            0.0, 0.0, 0.0, 1.0, 0.0, 7.0, -8.0
        )
    ]
    input_schema = T.StructType([
        T.StructField("game_id", T.IntegerType(), False),
        T.StructField("team_id", T.IntegerType(), False),
        T.StructField("team_abbreviation", T.StringType(), True),
        T.StructField("team_city", T.StringType(), True),
        T.StructField("player_id", T.IntegerType(), False),
        T.StructField("player_name", T.StringType(), True),
        T.StructField("nickname", T.StringType(), True),
        T.StructField("start_position", T.StringType(), True),
        T.StructField("comment", T.StringType(), True),
        T.StructField("min", T.StringType(), True),
        T.StructField("fgm", T.FloatType(), True),
        T.StructField("fga", T.FloatType(), True),
        T.StructField("fg_pct", T.FloatType(), True),
        T.StructField("fg3m", T.FloatType(), True),
        T.StructField("fg3a", T.FloatType(), True),
        T.StructField("fg3_pct", T.FloatType(), True),
        T.StructField("ftm", T.FloatType(), True),
        T.StructField("fta", T.FloatType(), True),
        T.StructField("ft_pct", T.FloatType(), True),
        T.StructField("oreb", T.FloatType(), True),
        T.StructField("dreb", T.FloatType(), True),
        T.StructField("reb", T.FloatType(), True),
        T.StructField("ast", T.FloatType(), True),
        T.StructField("stl", T.FloatType(), True),
        T.StructField("blk", T.FloatType(), True),
        T.StructField("TO", T.FloatType(), True),
        T.StructField("pf", T.FloatType(), True),
        T.StructField("pts", T.FloatType(), True),
        T.StructField("plus_minus", T.FloatType(), True)
    ])

    input_dataframe = spark.createDataFrame(input_data, input_schema)
    actual_df = do_player_game_edge_transformation(spark, input_dataframe)

    # Expected output with one edge, selecting higher pts row
    expected_output = [
        PlayerGameEdge(
            subject_identifier=1630182,
            subject_type='player',
            object_identifier=22000690,
            object_type='game',
            edge_type='plays_in',
            properties={
                'start_position': 'G',
                'pts': '7.0',
                'team_id': '1610612742',
                'team_abbreviation': 'DAL'
            }
        )
    ]

    expected_schema = T.StructType([
        T.StructField("subject_identifier", T.IntegerType(), False),
        T.StructField("subject_type", T.StringType(), False),
        T.StructField("object_identifier", T.IntegerType(), False),
        T.StructField("object_type", T.StringType(), False),
        T.StructField("edge_type", T.StringType(), False),
        T.StructField("properties", T.MapType(T.StringType(), T.StringType()), False)
    ])

    expected_df = spark.createDataFrame(expected_output,expected_schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)