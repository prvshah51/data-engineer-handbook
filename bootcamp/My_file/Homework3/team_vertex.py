from pyspark.sql import SparkSession


query =  """
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id ) as row_num
    FROM teams
)
SELECT
       team_id AS identifier,
    'team' AS `type`,
    map(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', CAST(yearfounded AS STRING)
        ) as properties
FROM teams_deduped
WHERE row_num = 1 
"""

def do_team_vertex_transformation1(spark, dataframe):
    dataframe.createOrReplaceTempView("teams")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("team_vertex") \
      .getOrCreate()
    output_df = do_team_vertex_transformation1(spark, spark.table("teams"))
    output_df.write.mode("overwrite").insertInto("team_vertex")
