package groupId

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
import scala.collection.mutable.ArrayBuffer


case class Edge[T, U](src: T, dest: T, weight: U) {
  def toCCO: (T, T, U) = new Tuple3[T, T, U](src, dest, weight)
}

object PageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PageRank")
      .getOrCreate()

    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, 2)

    val pagesDS = getPagesDS(spark)
    //      .sort(asc("src"), asc("dest"))

    pagesDS.show()

    pagesDS.rdd.take(20).map(_.toCCO).foreach(println)

    val transitionDS = getTransitionDS(spark, pagesDS)

    transitionDS.show()

    transitionDS.rdd.take(20).map(_.toCCO).foreach(println)

    val pagesNumDS = pagesDS.agg(max(pagesDS.col("src").as("max_from")), max(pagesDS.col("dest").as("max_to")))

    // How many pages we have here
    val pagesNum = math.max(pagesNumDS.first().getAs[Int](0), pagesNumDS.first().getAs[Int](1))

    println("Pages num: " + pagesNum)

    computePageRank(spark, transitionDS, pagesNum)
  }

  def getPagesDS(spark: SparkSession): Dataset[Edge[Int, Int]] = {

    import org.apache.spark.sql.Encoders
    import spark.implicits._
    val schema = Encoders.product[Edge[Int, Int]].schema

    val pagesDS: Dataset[Edge[Int, Int]] = spark
      .read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
//      .load("hdfs://localhost:50071/user/graph_mock.tsv")
      .load("hdfs://localhost:50071/user/graph1.tsv")
      .toDF("src", "dest", "weight")
      .as[Edge[Int, Int]]
    pagesDS
  }

  /**
    * Computes transition probability for each edge.
    *
    * @param spark: SparkSession
    * @param dataset: pages dataset
    * @return transitions dataset ordered by dest, src for convenience of PageRank computing
    */
  def getTransitionDS(spark: SparkSession, dataset: Dataset[Edge[Int, Int]]): Dataset[Edge[Int, Double]] = {
    val windowSpec = Window
      .partitionBy(dataset.col("src"))
      .rowsBetween(Long.MinValue, Long.MaxValue)  // To sum all rows
      .orderBy(asc("src"), asc("dest"))

    val proba = dataset.col("weight") / sum(dataset.col("weight")).over(windowSpec)

    // To get DataFrame as Dataset[MatrixEntry]
    import spark.implicits._

    val transitionDS: Dataset[Edge[Int, Double]] = dataset
      .withColumn("weight", proba)
      .select("src", "dest", "weight")
      .orderBy(asc("dest"), asc("src"))
      .as[Edge[Int, Double]]

    transitionDS
  }

  def computePageRank(spark: SparkSession, dataset: Dataset[Edge[Int, Double]], pagesNum: Int): Unit = {
    import spark.implicits._

    val v = Array.fill[Double](pagesNum)(1d / pagesNum)

    var p: Array[Double] = Array.fill[Double](pagesNum)(1d / pagesNum)

    val deltas: ArrayBuffer[Double] = ArrayBuffer()

    var delta = 0d

    val c = 0.85

    for (i <- 1 to 50) {
//      println("Iteration " + i)

      var pNextMap = dataset
        .map(edge => Edge(edge.src, edge.dest, edge.weight * p(edge.src - 1)))
        .groupBy("dest")
        .agg(sum(col("weight")).as("p_elems"))
        .orderBy(asc("dest"))
        .select("dest", "p_elems")
        .as[(Int, Double)]
        .rdd
        .map[(Int, Double)](tpl => (tpl._1, tpl._2 * c))
        .collectAsMap

      var gamma = p.map(math.abs).sum - pNextMap.values.map(math.abs).sum

      println(pNextMap.values.map(math.abs).sum)

//      println("[" + pNext.mkString(", ") + "]")
//      println(gamma)

      var pNext = for (i <- 1 to pagesNum ) yield pNextMap.getOrElse(i, 0d)
      pNext = pNext.zip(v).map { case (x, y) => x + y * gamma }
//
////      println("[" + pNext.deep.mkString(", ") + "]")
//
      delta = pNext.zip(p).map { case (x, y) => x - y }.map(math.abs).sum
      deltas.append(delta)
//
////      println(delta)
////      println("====== Iteration end ======")
      println(pNext.sum)
      p = pNext.toArray.clone()
    }

    println("delta = " + delta)

    println("Sum of PageRank vector: " + p.sum)

    val mappedP: Map[Int, Double] = (for (i <- 1 until pagesNum) yield (i, p(i))).toMap[Int, Double]


    import java.io._
    val pw = new PrintWriter(new File("pageRank.txt"))
    mappedP.toSeq.sortWith(_._2 > _._2).take(50).foreach(pair => pw.write(s"${pair._1} \t ${pair._2} \n"))
    pw.close()

    val pw2 = new PrintWriter(new File("deltas.txt"))
    pw2.write(s"[${deltas.toList.mkString(", ")}]")
    pw2.close()
  }
}
