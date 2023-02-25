import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os


# Step 1 Create Dictionary with isoAlpha2Code
def qa_enforce():
    alpha2FileName = 'resources/isoAlpha2Code.csv'
    alpha2Data = open(alpha2FileName, 'r')
    alpha2Reader = csv.DictReader(alpha2Data)
    alpha2Dict = {}

    for row in alpha2Reader:
        alpha2Dict[row['Code']] = row['Name']

    # Step 2 Quality Check wether the following rules comply:
    #  * HS Code needs to start with `870423` (Trucks & Lorries) and have 8 characters
    #  * Port code starts with the country's ISO Alpha-2 code
    #  * Quantity is a numeric, where we can assume the number is 1 if the value
    #    is less than 80,000 USD when there's no information

    spark = SparkSession.builder.getOrCreate()

    tradesFileName = 'resources/trades.csv'
    tradesQaFileName = 'csv/trades_qa'

    df = spark. \
        read. \
        option("header", True). \
        option("sep", ';'). \
        csv(tradesFileName)

    df_qa = df.withColumn("value_fob_usd", regexp_replace(df.value_fob_usd, ',', '.')). \
        withColumn("std_quantity", col("std_quantity").cast("int")). \
        withColumn("std_quantity", when(col('value_fob_usd').__lt__(80000), 1).otherwise(col("std_quantity"))). \
        withColumn("QA", when(col('source_port').substr(1, 2).isin([*alpha2Dict.keys()]) &
                              col('destination_port').substr(1, 2).isin([*alpha2Dict.keys()]) &
                              (df.hs_code.substr(1, 6) == '870423') &
                              (length(df.hs_code) == 8), 1).otherwise(0)). \
        filter(col('QA') == 1). \
        drop(col("QA"))

    df_qa.show(10)

    df_qa.write. \
        mode("overwrite"). \
        option("header", True). \
        option("sep", ';'). \
        csv(tradesQaFileName)

    # old_file = os.path.join("./trades_qa.csv/", "*.csv")
    # new_file = os.path.join(".", "trades_qa.csv")
    # os.rename(re.compile(r"trades_qa.csv/.*.csv"), "trades_qa.csv")

    import re

    dir_src = "csv/trades_qa/"
    delete_dir = "csv/trades_qa"
    dir_dst = ""
    new_name = "csv/trades_qa.csv"
    for file in os.listdir(dir_src):
        if re.search(r".*.csv$", file):
            src_file = os.path.join(dir_src, file)
            dst_file = os.path.join(dir_dst, new_name)
            print(dir_dst + new_name)
            print(file)

            os.rename(src_file, dst_file)
            os.chmod(delete_dir, 0o777)
            try:
                os.remove(delete_dir)
            except OSError as e:
                print("Error: %s : %s" % (delete_dir, e.strerror))
