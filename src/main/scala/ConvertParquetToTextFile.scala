

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hive.ql.exec.UDF


/**
 * 
 * This example convert parquet file to textFile 
 * 
 */
object ConvertParquetToTextFile {
  
 
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("parquet")
    
    val sc=new SparkContext(conf)
    
    val sqlContext=new SQLContext(sc);
    
    val parquetFile=sqlContext.parquetFile("file:///home/cloudera/parquet_file/customers/000000_0")
    
    parquetFile.registerTempTable("customer")
    
    val sqlCustomer=sqlContext.sql("select * from customer limit 10")
    
    sqlCustomer.collect().foreach { x => println(x) }
    
    sqlCustomer.rdd.saveAsTextFile("hdfs:///user/cloudera/parquet_file/customer_output")
//    
    
  }
}
