import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.{toRDDFunctions, _}
import org.apache.spark.{SparkConf, SparkContext}

object QuickStart {
  def main(args:Array[String]) = {

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("scala-spark-couchbase")
      .setMaster("local[*]")
      .set("com.couchbase.bucket.travel-sample", "")

    // Generate The Context
    val sc = new SparkContext(cfg)
    // Query by ID
    sc.couchbaseGet[JsonDocument](Seq("airline_10")).collect().foreach(println)

    // Calling a View
    println("Query by View")
    sc.couchbaseView(ViewQuery.from("airline","airline_view").limit(10))
      .map(_.id)
      .couchbaseGet[JsonDocument]()
      .collect()
      .foreach(println)

  }
}
