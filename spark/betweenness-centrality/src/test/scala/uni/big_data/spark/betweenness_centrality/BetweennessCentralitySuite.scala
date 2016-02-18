package uni.big_data.spark.betweenness_centrality

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite


class BetweennessCentralitySuite extends FunSuite with SharedSparkContext {
  test("test BetweennessCentralityCalculation") {
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

    val betweennessGraph = BetweennessCentrality.run(Graph(vertices, edges))
    assert(betweennessGraph.vertices.collect().deep == expected_betweenness.deep)
  }
}
