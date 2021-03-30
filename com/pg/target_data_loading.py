import os
import yaml
from pyspark.sql import SparkSession
from utils import aws_utils as ut
if __name__ == "__main__":
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_dir = os.path.abspath(current_dir + '/../' + 'application.yml')
    secret_dir = os.path.abspath(current_dir + '/../' + '.secret')
    
    conf = open(app_dir)
    conf_app_dir = yaml.load(conf, Loader=yaml.FullLoader)
    conf1 = open(secret_dir)
    conf_secret_dir = yaml.load(conf1, Loader=yaml.FullLoader)
    
    spark = SparkSession \
        .builder \
        .appName("data loading to target tables")\
        .getOrCreate()
    
    spark.SparkContext.setLogLevel('Error')
    
    # connection to s3
    hadoop_conf = spark.SparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', conf_secret_dir["s3_conf"]["access_key"])
    hadoop_conf.set('fs.s3a.secret.key', conf_secret_dir["s3_conf"]["secret_key"])
    
    s3_temp = "s3a://" + conf_app_dir["s3_conf"]["s3_bucket"] + "/temp"
    app_target = conf_app_dir["target_list"]
    for list1 in app_target:
        if list1 == "REGIS_DIM":
            source_target = list1["source_data"]
            for src in source_target:
                ut.read_data_from_s3(src, conf_app_dir, spark)
            df_sql = spark.sql(list1['loading_query'])
            df_sql.show(5, False)
            ut.write_datato_redhsift(df_sql.coalesce(1), conf_secret_dir, list1, s3_temp)
            
        elif list1 == "CHILD_DIM":
            ut.read_data_from_s3(list1["source_data"], conf_app_dir, spark)
            df_sql = spark.sql(list1["loading_query"])
            df_sql.show(5, False)
            ut.write_datato_redhsift(df_sql.coalesce(1), conf_secret_dir, list1, s3_temp)
            
        elif list1 == "RTL_TXN_FACT":
            source_target = list1["source_data"]
            for src in source_target:
                ut.read_data_from_s3(src, conf_app_dir, spark)
            source_table = list1["source_table"]
            df = ut.read_data_from_redshift(spark, conf_secret_dir, source_table, s3_temp)
            df.printSchema()
            df.show(5, False)
            df.createOrReplaceTempView(src[source_table].split('.')[1])
            fact_sql = spark.sql(list1["loading_query"])
            fact_sql.show(5, False)
            
            ut.write_data_to_redshift(fact_sql, conf_secret_dir, list1, s3_temp)
            

            
            
            
    
    
    
    
    
    
    