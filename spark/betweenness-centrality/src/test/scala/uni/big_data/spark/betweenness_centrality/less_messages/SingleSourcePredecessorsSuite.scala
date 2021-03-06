package uni.big_data.spark.betweenness_centrality.less_messages

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by max on 26.11.15.
  */
class SingleSourcePredecessorsSuite extends FunSuite with SharedSparkContext {
  test("test BetweennessFindPaths") {
    val vertices: RDD[(VertexId, Int)] = sc.parallelize(
      Array(
        (1L, 0),
        (2L, 0),
        (3L, 0),
        (4L, 0)
      )
    )

    val edges: RDD[Edge[Double]] = sc.parallelize(
      Array(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(2L, 4L, 1.0),
        Edge(3L, 4L, 1.0)
      )
    )

    val expected_distances = Array(
      (4l, (0.0, 2.0, Array[VertexId](2l, 3l).deep, 0L)),
      (1l, (0.0, 0.0, Array[VertexId]().deep, 0L)),
      (2l, (0.0, 1.0, Array[VertexId](1l).deep, 0L)),
      (3l, (0.0, 1.0, Array[VertexId](1l).deep, 0L))
    )


    val testGraph = Graph(vertices, edges).mapVertices((id, _) =>
      (0.0, 0.0, Array[VertexId](), 0L)
    ).cache()

    val shortest_paths = SingleSourcePredecessors.run(testGraph, 1L)

    //Predecessor array must be deep compared
    val res = shortest_paths.vertices.collect().map((vertex: (VertexId, (Double, Double, Array[VertexId], Long))) =>
      (vertex._1, (vertex._2._1, vertex._2._2, vertex._2._3.deep, vertex._2._4))
    )

    println("shortest paths")
    shortest_paths.vertices.collect.foreach((data) => {
      println(s"${data._1}: -----")
      println(s"BetweennessBase: ${data._2._1}")
      println(s"Distance: ${data._2._2}")
      println(s"Predecessors: ${data._2._3.mkString(",")}")
      println(s"Number of Succcessors: ${data._2._4}")
    })

    assert(res.deep == expected_distances.deep)
  }
  test("test BetweennessPredecessors with no shortest path (inifite loop problem)") {
    //same graph start at vertex 4
    val vertices: RDD[(VertexId, Int)] = sc.parallelize(
      Array(
        (1L, 0),
        (2L, 0),
        (3L, 0),
        (4L, 0)
      )
    )

    val edges: RDD[Edge[Double]] = sc.parallelize(
      Array(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(2L, 4L, 1.0),
        Edge(3L, 4L, 1.0)
      )
    )

    val expected_distances = Array(
      (4l, (0.0, 0.0, Array[VertexId]().deep, 0L)),
      (1l, (0.0, Double.PositiveInfinity, Array[VertexId]().deep, 0L)),
      (2l, (0.0, Double.PositiveInfinity, Array[VertexId]().deep, 0L)),
      (3l, (0.0, Double.PositiveInfinity, Array[VertexId]().deep, 0L))
    )


    val testGraph = Graph(vertices, edges).mapVertices((id, _) =>
      (0.0, 0.0, Array[VertexId](), 0L)
    ).cache()

    val shortest_paths = SingleSourcePredecessors.run(testGraph, 4L)

    //Predecessor array must be deep compared
    val res = shortest_paths.vertices.collect().map((vertex: (VertexId, (Double, Double, Array[VertexId], Long))) =>
      (vertex._1, (vertex._2._1, vertex._2._2, vertex._2._3.deep, vertex._2._4))
    )

    println("shortest paths")
    shortest_paths.vertices.collect.foreach((data) => {
      println(s"${data._1}: -----")
      println(s"BetweennessBase: ${data._2._1}")
      println(s"Distance: ${data._2._2}")
      println(s"Predecessors: ${data._2._3.mkString(",")}")
      println(s"Number of Succcessors: ${data._2._4}")
    })

    assert(res.deep == expected_distances.deep)
  }
}