# function to read all necessary info from mysql and return a dataframe
def read_from_mysql(spark, conf_app_dir, conf_secret_dir):
    jdbcparams = {"url": get_mysql_jdbc_url(conf_secret_dir),
                  "lowerBound": "1",
                  "upperBound": "100",
                  "db_table": conf_app_dir["mysql_config"]["db_table"],
                  "numPartition": 2,
                  "partitionColumn": conf_app_dir["mysql_config"]["partition_column"],
                  "user": conf_secret_dir["mysql_config"]["user"],
                  "password": conf_secret_dir["mysql_config"]["password"]
                  }
    df_sql = spark.read \
        .format("jdbc") \
        .option("driver", 'com.mysql.cj.jdbc.Driver') \
        .options(**jdbcparams) \
        .load()
    return df_sql


# function to read data from SFTP server and return dataframe
def read_from_sftp(spark, conf_secret_dir, conf_app_dir, pem_file_path):
    df_sftp = spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", conf_secret_dir["sftp_conf"]["host"]) \
        .option("port", conf_secret_dir["sftp_conf"]["port"]) \
        .option("user", conf_secret_dir["sftp_conf"]["username"]) \
        .option("password", conf_secret_dir["sftp_conf"]["password"]) \
        .option("pem", pem_file_path) \
        .option('filetype', conf_app_dir["sftp_conf"]["filetype"]) \
        .option('delimiter', conf_app_dir["sftp_conf"]["delimiter"]) \
        .load(conf_app_dir["sftp_conf"]["directory"]+ "/" + conf_app_dir["file_name"])
    return df_sftp


# read data from s3
def read_from_s3(spark, conf_app_dir):
    df_csv = spark.read.format("csv") \
        .option('header', 'false') \
        .option('delimiter', ',') \
        .load("s3a://" + conf_app_dir["s3_conf"]["s3_bucket"] + "/" + conf_app_dir["file_name"])
    return df_csv


# read data from mongo db
def read_from_mongodb(spark, conf_app_dir):
    df_mongo = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", conf_app_dir["mongo_conf"]["database"]) \
        .option("collection", conf_app_dir["mongo_conf"]["collection"]) \
        .load()
    return df_mongo


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_config"]["host"]
    port = mysql_config["mysql_config"]["port"]
    database = mysql_config["mysql_config"]["Dbname"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)



def get_redshift_jdbc_url(redshift_config:dict):
    host = redshift_config["redshift_config"]["host"]
    port = redshift_config["redshift_config"]["port"]
    database = redshift_config["redshift_config"]["host"]
    username = redshift_config["redshift_config"]["host"]
    password = redshift_config["redshift_config"]["host"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host,port,database,username,password)


def write_data_to_redshift(df, conf_secret_dir, app_conf_list, s3_temp):
    df.write.format("io.github.spark_redshift_community.spark.redshift")\
                    .option("uri", get_redshift_jdbc_url(conf_secret_dir)) \
                    .option("temp_folder", s3_temp) \
                    .option("forward_spark_s3_credentials", "true") \
                    .option("table_name", app_conf_list['target_table']) \
                    .mode("overwrite") \
                    .save()
    

def read_data_from_s3(src, conf_app_dir, spark):
    file_path = "s3a:/" + conf_app_dir["s3_conf"]["s3_bucket"] + "/" + conf_app_dir["s3_conf"][
        "staging_saving"] + "/" + src
    df = spark.sql("select * from parquet{}".format(file_path))
    df.printSchema()
    df.show(5, False)
    df.createOrReplaceTempView(src)
    
    
def read_data_from_redshift(spark, conf_secret_dir, source_table, s3_temp_dir ):
    df = spark.read.format("io.github.spark_redshift_community.spark.redshift") \
                    .option("uri", get_redshift_jdbc_url(conf_secret_dir)) \
                    .option("table", source_table) \
                    .option("forward_spark_s3_credentials", "true") \
                    .option("tempdir", s3_temp_dir) \
                    .load()
    return df
 
    