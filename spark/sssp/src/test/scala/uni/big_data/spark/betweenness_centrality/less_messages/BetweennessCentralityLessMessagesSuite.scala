package uni.big_data.spark.betweenness_centrality.less_messages

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by max on 26.11.15.
  */
class BetweennessCentralityLessMessagesSuite extends FunSuite with SharedSparkContext {
  test("test BetweennessBase") {
    val vertices: RDD[(VertexId, Int)] = sc.parallelize(
      Array(
        (1L, 0),
        (2L, 0),
        (3L, 0),
        (4L, 0),
        (5L, 0)
      )
    )

    val edges: RDD[Edge[Double]] = sc.parallelize(
      Array(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(2L, 4L, 1.0),
        Edge(3L, 4L, 1.0),
        Edge(4L, 5L, 1.0)
      )
    )

    val expected_betweenness = Array(
      (4l, 3.0),
      (1l, 0.0),
      (5l, 0.0),
      (2l, 1.0),
      (3l, 1.0)
    )

    val betweennessGraph = BetweennessCentralityLessMessages.run(Graph(vertices, edges))
    println("BetweennessBase of vertices")
    betweennessGraph.vertices.collect
      .foreach((data) => {
        println(s"\tVertex ${data._1}: ${data._2}")
      })
    assert(betweennessGraph.vertices.collect().deep == expected_betweenness.deep)
  }
}
