package uni.big_data.spark.sssp

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by max on 26.11.15.
  */
class SingleSourceShortestPathSuite extends FunSuite with SharedSparkContext{
  test("Calculate Shortest Path between 2 Nodes") {
    val vertices: RDD[(VertexId,Int)] = sc.parallelize(
      Array(
        (1L,0),
        (2L,0),
        (3L,0)
      )
    )

    val edges: RDD[Edge[Double]] = sc.parallelize(
      Array(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 5.0),
        Edge(2L, 3L, 2.0)
      )
    )

    val expected_distances = Array(
      (1l, (0.0, Array[VertexId]().deep)),
      (2l, (1.0, Array[VertexId](1L).deep)),
      (3l, (3.0, Array[VertexId](2L).deep))
    )

    val shortest_paths = SingleSourceShortestPath.run(Graph(vertices,edges),1L)


    //Predecessor array must be deep compared
    val res = shortest_paths.vertices.collect().map( (vertex:(VertexId,(Double,Array[VertexId]))) =>
      (vertex._1,(vertex._2._1,vertex._2._2.deep))
    )


    assert(res.deep == expected_distances.deep )

    shortest_paths.vertices.collect.foreach( (data) => {
      println(s"${data._1}: -----")
      println(s"Distance: ${data._2._1}")
      println(s"Predecessors: ${data._2._2.mkString(",")}")
    })

  }

  test("store all predecessors if distances are equal") {
    val vertices: RDD[(VertexId,Int)] = sc.parallelize(
      Array(
        (1L,0),
        (2L,0),
        (3L,0),
        (4L,0)
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

    val with_double_predecessors = (4l, (2.0, Array[VertexId](2L,3L).deep))


    val shortest_paths = SingleSourceShortestPath.run(Graph(vertices,edges),1L)

    //Predecessor array must be deep compared
    val res = shortest_paths.vertices.collect().map( (vertex:(VertexId,(Double,Array[VertexId]))) =>
      (vertex._1,(vertex._2._1,vertex._2._2.deep))
    )


    assert(res.contains(with_double_predecessors))

    shortest_paths.vertices.collect.foreach( (data) => {
      println(s"${data._1}: -----")
      println(s"Distance: ${data._2._1}")
      println(s"Predecessors: ${data._2._2.mkString(",")}")
    })
  }
}
