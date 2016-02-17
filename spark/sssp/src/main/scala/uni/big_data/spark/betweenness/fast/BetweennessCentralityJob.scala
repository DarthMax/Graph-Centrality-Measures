package uni.big_data.spark.betweenness.fast

import java.util.Calendar

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BetweennessCentralityJob   {

  var sc:SparkContext = null

  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sssp")
      .setMaster("local[1]")

    sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)

    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, "data/sample_graph.txt")
    val weightedGraph = graph.mapEdges((e:Edge[Int]) => e.attr.toDouble)

    val t1 = Calendar.getInstance().getTime
    val betweennessCentralityValues = BetweennessFast.run(weightedGraph)
    val t2 = Calendar.getInstance().getTime

    betweennessCentralityValues.vertices.collect.foreach( (data) => println(s"${data._1}: ${data._2}"))
    println(s"Took: ${(t2.getTime - t1.getTime) / 1000.0}")
  }

  def load_sample_data(): Graph[Int,Double] = {
    // Set Source
    val vertices: RDD[(VertexId,Int)] = sc.parallelize(
      Array(
        (1L,0),
        (2L,0),
        (3L,0),
        (4L,0),
        (5L,0),
        (6L,0),
        (7L,0)
      )
    )

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] = sc.parallelize(
      Array(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(3L, 4L, 1.0),
        Edge(3L, 5L, 1.0),
        Edge(4L, 6L, 1.0),
        Edge(4L, 7L, 1.0),
        Edge(5L, 7L, 1.0)
      )
    )


    Graph(vertices, relationships)
  }

}
