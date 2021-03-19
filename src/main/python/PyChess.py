
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



spark = SparkSession\
    .builder\
    .appName("PySpark Chess data")\
    .master("local")\
    .getOrCreate()

chessDF = spark.read.json("jeroen_chess.json")

chessDF = chessDF.select(explode("Games").alias("game"))

chessDF = chessDF.select("game.black.result","game.black.username").filter("username=='JeroenWolfe'").groupBy("result","username").count().collect()

chessDF = spark.createDataFrame(chessDF)

chessDF.printSchema()
chessDF.show()

