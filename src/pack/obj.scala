package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj {

  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir","D:\\hadoop")
    
    val conf = new SparkConf().setAppName("first")
                              .setMaster("local[*]")
                              .set("spark.driver.allowMultipleContexts", "true")
                              
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate( )
    import spark.implicits._
    
    val df = spark.read
                 .format("org.apache.spark.sql.cassandra")
                 .option("spark.cassandra.connection.host","localhost") //running on local host
                 .option("spark.cassandra.connection.port","9042") //9042 is default port on which cassandra runs
                 .option("keyspace","b36") //keyspace name
                 .option("table","ztab") //table name
                 .load()
                 
    df.show()    
    
    
    //processing cassandra table
    val finaldf = df.filter(col("id")>1)
    finaldf.show()
    
    
    //writing to a different cassandra table (imagine this cassandra is running in a different host in real time)
    finaldf.write
         .format("org.apache.spark.sql.cassandra")
         .option("spark.cassandra.connection.host","localhost") //running on local host
         .option("spark.cassandra.connection.port","9042") //9042 is default port on which cassandra runs
         .option("keyspace","b361") //This keyspace should already exist in the destination cassandra
         .option("table","zout") // this table should already exist in the destination cassandra keyspace
         .save()
    

    
  }
}