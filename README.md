from pyspark.sql import *
from pyspark.sql import functions as f
from lib.logger import Log4j





if __name__ == '__main__':

    spark = SparkSession.builder.appName('aggdemo').master("local[2]").getOrCreate()
    logger = Log4j(spark)

    df = spark.read.csv("data/invoices.csv",inferSchema=True,header=True)
sdf = df.withColumn("Date", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm"))\
    .where(f.year("Date") == 2010).withColumn("Week", f.weekofyear(f.col("Date")))

sdf.groupby("Country","Week").agg(f.sum("Quantity").alias("TotalValue"),f.countDistinct("InvoiceNo").alias("InvoiceCount")).show()

