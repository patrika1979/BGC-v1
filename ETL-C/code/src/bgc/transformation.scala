package bgc
import org.apache.spark.sql.functions.{col, udf, explode, row_number, desc, asc, avg, count, sum, when, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.expressions.Window
import org.apache.spark._
//import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SQLContext

/** Data Transformation for movie Data*/
object transformation {
  
  val titleSchema = StructType(Array(
      StructField("tconst", StringType, nullable = true),
      StructField("titleType", StringType, nullable = true),
      StructField("primaryTitle", StringType, nullable = true),
      StructField("originalTitle", StringType, nullable = true),
      StructField("isAdult", StringType, nullable = true),
      StructField("startYear", StringType, nullable = true),
      StructField("endYear", StringType, nullable = true),
      StructField("runtimeMinutes", StringType, nullable = true),
      StructField("genres", StringType, nullable = true)
))
  
    /** Main program */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    //Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    
    // Create a SparkContext using every core of the local machine
    val conf = new SparkConf().setAppName("ETL BGC").setMaster("local") 
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    println("Starting...")
    
    println("Title_basics dataset...")
    
    var df_title_basics = sqlContext.read.format("csv").schema(titleSchema).option("sep", "\t").option("header", "true").option("inferSchema", "false").load("movies_db/title_basics")
    df_title_basics = df_title_basics.select(df_title_basics("tconst").alias("title_const"), df_title_basics("primaryTitle"))
    
    println("Title_principals dataset...")
    var df_title_principals = sqlContext.read.format("csv").option("sep", "\t").option("header", "true").option("inferSchema", "true").load("movies_db/title_principals")
    
    df_title_principals = df_title_principals.select(df_title_principals("tconst").alias("ttconst"), 
                                                  df_title_principals("nconst").alias("name_const"), 
                                                  df_title_principals("category"))
	
  	
    println("Names dataset...")                                              
    var df_names = sqlContext.read.format("csv").option("sep", "\t").option("header", "true").option("inferSchema", "true").load("movies_db/names")
  	df_names = df_names.select(df_names("nconst"), df_names("primaryName"))
  	
  	println("Joining...")
  	var df_join1 = df_title_principals.join(df_names, df_names("nconst")===df_title_principals("name_const"))
  	
  	var df_join2 = df_join1.join(df_title_basics, df_join1("ttconst")=== df_title_basics("title_const"))
  	
  	var df_ratings = sqlContext.read.format("csv").option("sep", "\t").option("header", "true").option("inferSchema", "true").load("movies_db/title_ratings")
  	
  	df_ratings = df_ratings.where("numVotes>=50")
  	
  	var sumVotes=df_ratings.select(avg(df_ratings("numVotes")).alias("averageNumVotes"))
  	
  	val avgvotes=(sumVotes.select("averageNumVotes").collect().map(_.getDouble(0)).mkString(" ")).toDouble
  	
  	var df_averageNumberOfVotes1 = df_ratings.withColumn("averageNumberOfVotes", (df_ratings("numVotes")/lit(avgvotes))*df_ratings("averageRating"))
  	
  	val df_rank=df_averageNumberOfVotes1.select(df_averageNumberOfVotes1("tconst"), 
                                             df_averageNumberOfVotes1("averageRating"), 
                                             df_averageNumberOfVotes1("numVotes"),
                                             df_averageNumberOfVotes1("averageNumberOfVotes"),
  										   row_number().over(Window.orderBy(df_averageNumberOfVotes1("averageNumberOfVotes") desc)).alias("rowNum"))
      										
  	val df_filter = df_rank.where("rowNum between 1 and 20")		
  
  	var df_join_final = df_filter.join(df_join2, df_join2("ttconst")===df_filter("tconst"))
  	  	 	
  	df_join_final = df_join_final.select(df_join_final("rowNum").alias("Rank_id"), 
                                      df_join_final("primaryTitle").alias("Movie_name"), 
                                      df_join_final("averageRating"), 
                                      df_join_final("numVotes"), 
                                      df_join_final("averageNumberofVotes"), 
                                      df_join_final("primaryName"), 
                                      df_join_final("category"))
                                      
    df_join_final = df_join_final.sort("Rank_id")
  	
    println("Final Report....")
    //for (df_join_final <- df_join_final.collect()) {
    //     println(df_join_final) 
    //  }
    
    //var result = df_join_final.collect()
    
    // Print the results.
    //result.foreach(println)
    
    val tsvWithHeaderOptions: Map[String, String] = Map(
    ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
    ("header", "true"))  // Writes a header record with column names
    
    println("Saving output...")

    df_join_final.coalesce(1).write.mode("overwrite").options(tsvWithHeaderOptions).csv("output")
    //df_join_final.write.options(tsvWithHeaderOptions).csv("output")
    sc.stop()
    }
}