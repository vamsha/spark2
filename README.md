# spark2
References:
https://databricks.com/blog/2016/07/26/introducing-apache-spark-2-0.html
http://blog.madhukaraphatak.com/

	1. Use scala 2.11.8
	2. Use built-in csv connector instead of spark-csv ( 3rd party library)
		i.  Schema Inference
		ii. Broadcast join will happen automatically in spark2.0
	3. Dataframe to Dataset
			Dataframe : Used to perform the operations on structured data as it have Schema.
			Rdd: Functional API's are easy to perform ( eg: map).
			Dataset: combination of Dataframe and Rdd. which will support structured dsl and sql along with functional API's.
			
      i.  Dataframe in spark 1.x:
				Dataframe: to process the data using domain specific language (dsl) and sql.
				RDD: will provide the functional API'scala
					eg:
						val loadedDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("../test_data/sales.csv")
						
						val amountRDD = loadedDF.map(row ⇒ row.getDouble(3))
						
						Here, DF will be automatically converted into rdd when we apply .map
		ii. Dataframe in spark 2.x:
				Dataframe = Dataset[row]
				Dataset supports: structured query dsl and sql along with functional API's.
				Code has to be converted like follows:
					eg: 
						val amountRDD = loadedDF.rdd.map(row ⇒ row.getDouble(3))
						println(amountRDD.collect.toList)
		iii. RDD to Dataset Abstractions:
				Converting the dataframe to rdd wont give good performance. as RDD's dont have schema , we have give manually to proceed. and its a costly operation. its is bit easy in a vice-versa way.	
				Using Dataset functional API's instead of RDD api's will give good performance.
				we need to take care for the encoder which map returns, by default spark will provide encoders for all the primitive types case classes.
					eg: 
						import sparkSession.implicits._   // will import all the default encoders.
						val dataset = df.map(row => row.getDouble(3))  // encoder needed for double datatype, which is available in spark
						
						Switch back to RDD API, when we dataset SPI doesnt support rdd API.
	
	4. Cross joins in Spark-sql:
		cross joins are costly as we have to perform m*n number of rows, where m,n are size of datasets.
		spark 1.x:
			val loadedDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../test_data/sales.csv")
				
				 val crossJoinDf = loadedDf.join(loadedDf)     // by default it will perform cross join 
				
				 println(crossJoinDf.explain()) 		// it will mention CartesianProduct in the paln.
				 println(crossJoinDf.count)
				 
		Spark 2.x:
			above query will throw an error in spark 2.x as all the joins reject cross product.
				Error: org.apache.spark.sql.AnalysisException: Detected cartesian product for INNER join between logical plans
				
			You need to explicit while working on cross joins.
				val crossJoinDf = loadedDf.crossJoin(loadedDf)
				
	5. Catalog API:
		spark 1.x is heavily depend on hive for all metastore related operations. Even though sqlContext supported few of the DDL operations, most of them were very basic and not complete. So spark documentation often recommended using HiveContext over SQLContext. Also whenever user uses HiveContext, spark support for interacting with hive metastore was limited. So most of the metastore operation’s often done as embedded hive queries.
		
		Creating a table:
		spark 1.x:
			val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("/path")
			df.registerTempTable("tableName")
		spark 2.x:
			val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("/path")
			df.createOrReplaceTempView("tableName")
		Catalog Operations:
			List tables:
				spark 1.x:
					sqlContext.tables.show()
				spark 2.x:
					sparkSession.catalog.listTables.show()
			List table names:
				spark 1.x:
					sqlContext.tableNames()
				spark 2.x:
					sparkSession.catalog.listTables.select("tableName").collect
			Caching:
				spark 1.x:
					sqlContext.isCached("tableName")
				spark 2.x:
					sparkSession.catalog.isCached("tableName")
			External Tables:
				spark 1.x:
					creating a table directly from a file without using dataframe API's.
						val hiveContext = new HiveContext(sparkContext)
						hiveContext.setConf("hive.metastore.warehouse.dir", "/tmp")
						hiveContext.createExternalTable("sales_external", "com.databricks.spark.csv", Map(
								"path" -> "../test_data/sales.csv",
								"header" -> "true"))
						hiveContext.table("sales_external").show()
						
						In above code, first we create HiveContext. Then we need to set the warehouse directory so hive context knows where to keep the data. Then we use createExternalTable API to load the data to sales_external table.
				spark 2.x: 
					creating external table is part of catalog API itself. We don’t need to enable hive for this functionality.
						sparkSession.catalog.createExternalTable("sales_external", "com.databricks.spark.csv", Map(
								"path" -> "../test_data/sales.csv",
								"header" -> "true"))
						sparkSession.table("sales_external").show()
			List functions:
				sparkSession.catalog.listFunctions.show()
			List Columns:
				sparkSession.catalog.listColumns("tableName").show()
	
	6. Hive integration in spark:
		Enable hive support:
			val sparkSession = SparkSession.builder.master("local").appName("mapexample").
					enableHiveSupport().getOrCreate()
			Spark session looks for hive-site.xml for connecting to hive metastore.
			
		Loading Table:
			val df = sparkSession.table("tableName")  // Table API is used to load the table/data from hive.
			df.show()
		Save dataframe as hive table:
			df.write.saveAsTable("tableName")   //it will create table metadata in hive metastore and save the data in parquet format.
			
	
