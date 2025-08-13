from chispa.dataframe_comparer import *

from ..jobs.player_edge_job import do_player_edge_transformation
from collections import namedtuple

TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")

def test_player_edge_generation(spark):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Test").getOrCreate()
    input_data = [
        (22000690, 1610612742, "DAL", "Dallas", 1630182, "Josh Green", None, None, None, "8:32", 3, 5, 0.6, 0, 1, 0, 1, 2, 0.5, 1, 0, 1, 0, 0, 0, 1, 0, 7, -8, "Mavericks", "American Airlines Center", 1980),  # First row
        (22000690, 1610612742, "DAL", "Dallas", 1630183, "Another Player", None, None, None, "10:00", 5, 7, 0.7, 1, 2, 0.5, 2, 3, 0.67, 2, 1, 3, 2, 1, 1, 0, 1, 13, 5, "Bad Mavericks", "American Airlines Center", 1980)  # Duplicate team_id, different nickname
    ]
    input_schema = [
        "game_id", "team_id", "team_abbreviation", "team_city", "player_id", "player_name",
        "nickname", "start_position", "comment", "min", "fgm", "fga", "fg_pct", "fg3m",
        "fg3a", "fg3_pct", "ftm", "fta", "ft_pct", "oreb", "dreb", "reb", "ast",
        "stl", "blk", "TO", "pf", "pts", "plus_minus", "team_nickname", "arena", "year_founded"
    ]

    input_dataframe = spark.createDataFrame(input_data)
    input_dataframe.printSchema()

    expected_output = [
    TeamVertex(
        identifier=1,
        type='team',
        properties={
            'abbreviation': 'GSW',
            'nickname': 'Warriors',
            'city': 'San Francisco',
            'arena': 'Chase Center',
            'year_founded': '1900'
        }
    )
]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(input_df, expected_df, ignore_nullable=True)