package uni.big_data.spark.sssp

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object SSSPJob   {

  var sc:SparkContext = null

  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sssp")
      .setMaster("local[1]")

    sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)
    val sourceId:VertexId = 1L

    val example_graph = load_sample_data()
    val shortest_paths = SingleSourceShortestPath.run(example_graph,sourceId)

    shortest_paths.vertices.collect.foreach( (data) => {
      println(s"${data._1}: -----")
      println(s"Distance: ${data._2._1}")
      println(s"Predecessors: ${data._2._2.mkString(",")}")
    })
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
