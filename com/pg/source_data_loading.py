#read data from mysql and add a column and write data to s3
from pyspark.sql import SparkSession
from utils import aws_utils as ut
import os
import yaml
from pyspark.sql.functions import *
if __name__ == "__main__":
    os.environ["PYSPARK_SUBMIT_ARGS"]=(
        '--package "org.apache.hadoop:hadoop-aws:2.7.4" pyspark shell'
    )
    # setting application and secrets config file
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_dir = os.path.abspath(current_dir + "/../../" + "application.yml")
    secret_dir = os.path.abspath(current_dir + "/../../" + ".secret")

    conf = open(app_dir)
    conf_app_dir = yaml.load(conf, Loader=yaml.FullLoader)
    conf1 = open(secret_dir)
    conf_secret_dir = yaml.load(conf1, Loader=yaml.FullLoader)
    
    spark = SparkSession \
        .builder \
        .appName('SQL_reading') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('error')
    # creating connection with s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", conf_secret_dir["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", conf_secret_dir["s3_conf"]["secret_key"])

    source = conf_app_dir["source_list"]

    for src in source:
        src_config = conf_app_dir[src]
        if src == "SB":
            # reading data from mysql
            df_sql = ut.read_from_mysql(spark, src_config, conf_secret_dir) \
                .withColumn("ind_dt", current_date().alias("current_date"))
            df_sql.printSchema()
            df_sql.show()
        
            df_sql.write \
                .partitionBy('ind_dt') \
                .mode("overwrite") \
                .parquet("s3a://" + conf_app_dir["s3_conf"]["s3_bucket"] + "/" + conf_app_dir["staging_saving"]+"/" +src)

        elif src == "OL":
            # reading data from sftp server
            pem_file_path = os.path.abspath(current_dir + "/../../../../" + conf_secret_dir["sftp_conf"]["pem"])
            # file_name = ""
            df_sftp = ut.read_from_sftp(spark, conf_secret_dir, src_config, pem_file_path) \
                .withColumn("ind_dt", current_date().alias("current_date"))
            df_sftp.printSchema()
            df_sftp.show()
            df_sftp.write \
                   .partitionBy('ind_dt') \
                   .mode("overwrite") \
                   .parquet("s3a://" + conf_app_dir["s3_conf"]["s3_bucket"] + "/" + conf_app_dir["staging_saving"]+"/" +src)
        elif src == "CP":
            # read data from csv file
            # file_name1 = ""
            df_csv = ut.read_from_s3(spark, src_config) \
               .withColumn("ind_dt", current_date().alias("current_date"))

            df_csv.printSchema()
            df_csv.show()

            df_csv.write \
                  .partitionBy('ind_dt') \
                  .mode("overwrite") \
                  .parquet("s3a://" + conf_app_dir["s3_conf"]["s3_bucket"] + "/" + conf_app_dir["staging_saving"]+"/" +src)
        
    # read data from mongo db
        elif src == "ADDR":
            df_mongo = ut.read_from_mongodb(spark, src_config)\
                        .withColumn("ind_dt", current_date().alias("current_date"))
            df_mongo.printSchema()
            df_mongo.show(5, False)

            df_mongo.write\
                   .partitionBy('ind_dt') \
                   .mode("overwrite")\
                   .parquet("s3a://" + conf_app_dir["s3_conf"]["s3_bucket"] + "/" + conf_app_dir["staging_saving"]+"/" +src)








