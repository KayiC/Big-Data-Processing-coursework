import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when, concat, array, explode
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("graphframes")\
        .config("spark.jars", "/path/to/graphframes-0.8.2-spark3.0-s_2.12.jar")\
        .getOrCreate()

    green_taxi_schema = StructType([
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("passenger_count", FloatType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("trip_type", IntegerType(), True),
        StructField("congestion_surcharge", FloatType(), True),
        StructField("taxi_type", StringType(), True)
                                  ])

    vertexSchema = StructType([StructField("id", IntegerType(), False),
                               StructField("Borough", StringType(), True),
                               StructField("Zone", StringType(), True),
                               StructField("service_zone", StringType(), True)
                              ])

    edgeSchema = StructType([StructField("src", IntegerType(), False),
                               StructField("dst", IntegerType(), False)])
    
    try:
        sqlContext = SQLContext(spark)
        # shared read-only object bucket containing datasets
        s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
        s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
        s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        s3_bucket = os.environ['BUCKET_NAME']
    
        hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
        hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
        hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
        hadoopConf.set("fs.s3a.path.style.access", "true")
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

        taxi_zone_lookup_path = f"s3a://{s3_bucket}/taxi_zone_lookup.csv"
        green_sampledata_path = f"s3a://{s3_bucket}/task_3_mini_green_tripdata_dataset.csv"
        green_sampledata_2023_path = f"s3a://{s3_bucket}/green_tripdata_2023-01.csv"
        green_tripdata_path = f"s3a://{os.environ['DATA_REPOSITORY_BUCKET']}/ECS765/nyc_taxi/green_tripdata/2023/"

        vertex_df = spark.read.csv(taxi_zone_lookup_path, header=True, schema=vertexSchema)
        green_sampledata_df = spark.read.csv(green_sampledata_path, header=True, schema=green_taxi_schema)
        
        green_sampledata_2023_df = spark.read.csv(green_sampledata_2023_path, header=True, schema=green_taxi_schema)
        green_datasets_files = [f"{green_tripdata_path}green_tripdata_2023-{month:02d}.csv" for month in range(1, 8)] 
        green_tripdata_df = spark.read.csv(green_datasets_files, header=True, schema=green_taxi_schema)

        
        # Q1:
        # Sample dataset:
        sample_count = green_sampledata_df.count()
        print(f"The number of entries is: {sample_count}")
        sys.stdout.flush()

        # Large dataset:
        total_entries = green_tripdata_df.count() 
        print(f"Total number of entries in the NYC Green Taxi dataset: {total_entries}")
        sys.stdout.flush()

        
        # Q2:
        '''
        # Sample dataset:
        edge_sample_df = green_sampledata_df.select(green_sampledata_df.PULocationID.alias("src"), green_sampledata_df.DOLocationID.alias("dst"))
        edge_sample_df.select("src", "dst").show(5, truncate=False)
        '''
        # Large dataset:
        vertex_df.show(5, truncate=False) 

        # Debug: Verify data loading in 'green_tripdata_2023-01.csv'
        green_sampledata_2023_df.select(green_sampledata_2023_df.PULocationID, green_sampledata_2023_df.DOLocationID).show(5, truncate=False)

        # Large dataset:
        edge_df = green_tripdata_df.select(green_tripdata_df.PULocationID.alias("src"), green_tripdata_df.DOLocationID.alias("dst"))
        edge_df.select("src", "dst").show(5, truncate=False)
        

        # Q3:
        '''
        # Sample dataset:
        sample_graph = GraphFrame(vertex_df, edge_sample_df)
        
        edges_sample_with_column_df = edge_sample_df.withColumn("edge", array(col("src"), col("dst")))
        vertex_sample_with_lists_df = vertex_df.withColumn("vertex_info", array("id", "Borough", "Zone", "service_zone"))
        
        src_sample_df = edges_sample_with_column_df.join(vertex_sample_with_lists_df, edges_sample_with_column_df.src == vertex_sample_with_lists_df.id, "left") \
            .select(vertex_sample_with_lists_df.vertex_info.alias("src"), "edge", col("dst"))
        graph_sample_df = src_sample_df.join(vertex_sample_with_lists_df, src_sample_df["edge"][1] == vertex_sample_with_lists_df.id, "left") \
            .select("src", "edge", vertex_sample_with_lists_df.vertex_info.alias("dst"))
        
        graph_sample_df.show(10, truncate=False)
        '''
        
        # Large dataset:
        graph = GraphFrame(vertex_df, edge_df)
        
        edges_with_column_df = edge_df.withColumn("edge", array(col("src"), col("dst")))
        vertex_with_lists_df = vertex_df.withColumn("vertex_info", array("id", "Borough", "Zone", "service_zone"))
        
        src_df = edges_with_column_df.join(vertex_with_lists_df, edges_with_column_df.src == vertex_with_lists_df.id, "left") \
            .select(vertex_with_lists_df.vertex_info.alias("src"), "edge", col("dst"))
        graph_df = src_df.join(vertex_with_lists_df, src_df["edge"][1] == vertex_with_lists_df.id, "left") \
            .select("src", "edge", vertex_with_lists_df.vertex_info.alias("dst"))
        
        graph_df.show(10, truncate=False)
        

        # Q4
        # First join
        new_src_df = edge_df.join(vertex_df, edge_df.src == vertex_df.id, "left") \
            .select(col("src"), col("dst"), vertex_df.Borough.alias("src_Borough"), vertex_df.service_zone.alias("src_service_zone"))
        
        # Second join
        edges_with_vertex_df = new_src_df.join(vertex_df, new_src_df.dst == vertex_df.id, "left") \
            .select(col("src"), col("dst"), col("src_Borough"), col("src_service_zone"), 
                    vertex_df.Borough.alias("dst_Borough"), vertex_df.service_zone.alias("dst_service_zone"))
        
        # Filter
        filtered_edges_df = edges_with_vertex_df.filter((col("src_Borough") == col("dst_Borough")) & 
                                               (col("src_service_zone") == col("dst_service_zone")))

        #Print count statement
        count = filtered_edges_df.count()
        print(f"count: {count}")
        sys.stdout.flush()

        # Group it
        connected_vertices_count_df = filtered_edges_df.groupBy("src", "dst", "src_Borough", "src_service_zone") \
            .count() \
            .select(col("src").alias("id"), col("dst").alias("id"), 
                    col("src_Borough").alias("Borough"), col("src_service_zone").alias("service_zone"))

        connected_vertices_count_df.show(10, truncate=False)
        

        # Q5
        shortest_paths_df = graph.shortestPaths(landmarks=[1])
        
        # Explode the distances dictionary
        exploded_df = shortest_paths_df.select(col("id"), explode("distances").alias("id_to", "shortest_distance"))       
        
        filtered_df = exploded_df.filter(col("id_to") == 1)
        
        # Combine the value in different column into a single column
        combined_df = filtered_df.withColumn("id_to_1", concat(col("id").cast("string"), lit(" -> "), col("id_to").cast("string")))
        
        result_df = combined_df.select(col("id_to_1"), col("shortest_distance"))
        result_df.show(10, truncate=False)
        

        # Q6:
        # Calculate the page ranking for each vertex in the graph
        pagerank_results = graph.pageRank(resetProbability=0.17, tol=0.01)
        
        sorted_pagerank_df = pagerank_results.vertices.orderBy(col("pagerank"), ascending=False)
        
        result_df = sorted_pagerank_df.select(col("id"), col("pagerank"))
        result_df.show(5, truncate=False)

        
    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        spark.stop()
