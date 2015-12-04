package uni.big_data.spark.sssp

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by max on 26.11.15.
  */
class BetweennessSuite extends FunSuite with SharedSparkContext {
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
      (4l, (0.0, 2.0, Array[VertexId](2l, 3l).deep)),
      (1l, (0.0, 0.0, Array[VertexId]().deep)),
      (2l, (0.0, 1.0, Array[VertexId](1l).deep)),
      (3l, (0.0, 1.0, Array[VertexId](1l).deep))
    )


    val testGraph = Graph(vertices, edges).mapVertices((id, _) =>
      (0.0, 0.0, Array[VertexId]())
    ).cache()

    val shortest_paths = SingleSourceBetweennessFindPaths.run(testGraph, 1L)

    //Predecessor array must be deep compared
    val res = shortest_paths.vertices.collect().map((vertex: (VertexId, (Double, Double, Array[VertexId]))) =>
      (vertex._1, (vertex._2._1, vertex._2._2, vertex._2._3.deep))
    )

    println("shortest paths")
    shortest_paths.vertices.collect.foreach((data) => {
      println(s"${data._1}: -----")
      println(s"Betweenness: ${data._2._1}")
      println(s"Distance: ${data._2._2}")
      println(s"Predecessors: ${data._2._3.mkString(",")}")
    })

    assert(res.deep == expected_distances.deep)
  }
  test("test Betweenness with no shortest path (inifite loop problem)") {
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
      (4l, (0.0, 0.0, Array[VertexId]().deep)),
      (1l, (0.0, Double.PositiveInfinity, Array[VertexId]().deep)),
      (2l, (0.0, Double.PositiveInfinity, Array[VertexId]().deep)),
      (3l, (0.0, Double.PositiveInfinity, Array[VertexId]().deep))
    )


    val testGraph = Graph(vertices, edges).mapVertices((id, _) =>
      (0.0, 0.0, Array[VertexId]())
    ).cache()

    val shortest_paths = SingleSourceBetweennessFindPaths.run(testGraph, 4L)

    //Predecessor array must be deep compared
    val res = shortest_paths.vertices.collect().map((vertex: (VertexId, (Double, Double, Array[VertexId]))) =>
      (vertex._1, (vertex._2._1, vertex._2._2, vertex._2._3.deep))
    )

    println("shortest paths")
    shortest_paths.vertices.collect.foreach((data) => {
      println(s"${data._1}: -----")
      println(s"Betweenness: ${data._2._1}")
      println(s"Distance: ${data._2._2}")
      println(s"Predecessors: ${data._2._3.mkString(",")}")
    })

    assert(res.deep == expected_distances.deep)
  }

  test("test Betweenness") {
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

    val betweennessGraph = Betweenness.run(Graph(vertices, edges))
    println("Betweenness of vertices")
    betweennessGraph.vertices.collect
      .foreach((data) => {
        println(s"\tVertex ${data._1}: ${data._2}")
      })
    assert(betweennessGraph.vertices.collect().deep == expected_betweenness.deep)
  }
}
