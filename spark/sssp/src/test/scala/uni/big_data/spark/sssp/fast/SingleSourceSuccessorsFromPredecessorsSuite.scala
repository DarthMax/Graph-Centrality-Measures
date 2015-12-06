package uni.big_data.spark.sssp.fast

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by max on 26.11.15.
  */
class SingleSourceSuccessorsFromPredecessorsSuite extends FunSuite with SharedSparkContext {


  //  graph: Graph[(Double, Double, Array[VertexId]), Double], sourceId: VertexId):
  //  Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double] = {
  test("Find Last") {

    val vertices: RDD[(VertexId, (Double, Double, Array[VertexId], Long))] = sc.parallelize(
      Array(
        (1L, (0.0, 34.9, Array[VertexId](), 0L)),
        (2L, (4.0, 14.9, Array[VertexId](1L), 0L)),
        (3L, (0.8, 74.9, Array[VertexId](1L), 344L)),
        (4L, (2.0, 38.9, Array[VertexId](2L, 3L), 343L)),
        (5L, (2.0, 38.9, Array[VertexId](4L), 0L))
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

    val testGraph = Graph(vertices, edges)


    val successors = SingleSourceSuccessorsFromPredecessors.run(testGraph, 1L).vertices.collect()

    println("Successor lists")
    successors.foreach((data) => {
      println(s"id${data._1}: -----")
      println(s"unused betweenness: ${data._2._1}")
      println(s"unused Double: ${data._2._2}")
      println(s"Predecessors: ${data._2._3.mkString(",")}")
      println(s"Number of Succcessors: ${data._2._4}")
    })
    // predeccessors have to be the same

    //successsors have to be those
    val expectedSuccessors = Array(
      (4l, Array[VertexId](5L).deep),
      (1l, Array[VertexId](2l, 3l).deep),
      (5l, Array[VertexId]().deep),
      (2l, Array[VertexId](4L).deep),
      (3l, Array[VertexId](4L).deep)
    )
    val resultSuccessors = successors.map(vertex => (vertex._1, vertex._2._4))

    assert(resultSuccessors.deep == expectedSuccessors.deep, "\n\nThe calculated sucessors are not the right ones\n")
  }
}