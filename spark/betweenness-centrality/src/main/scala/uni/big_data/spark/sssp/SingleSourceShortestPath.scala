package uni.big_data.spark.sssp

import org.apache.spark.graphx._


/**
  * Implements an solution for den Single Source Shortest Path problem using the Pregel API
  *
  * Given a start vertex and a weighted directed graph this calculates the shortest paths to
  * every other vertex in the graph that can be reached. In the resulting graph every vertex
  * stores the distance to the start node and a list of all direct predecessors on the shortest path.
  *
  * @example Given a simple graph we can calculate the SSSP problem:
  * {{{
  *
  * //Define Vertices
  * val vertices: RDD[(VertexId,Int)] = sc.parallelize(
  *    Array(
  *      (1L,0),
  *      (2L,0),
  *      (3L,0),
  *      (4L,0),
  *      (5L,0),
  *    )
  * )
  *
  * //Define edges and edge weights
  * val relationships: RDD[Edge[Double]] = sc.parallelize(
  *   Array(
  *     Edge(1L, 2L, 1.0),
  *     Edge(1L, 3L, 1.0),
  *     Edge(3L, 4L, 1.0),
  *     Edge(2L, 4L, 1.0),
  *     Edge(3L, 5L, 1.0),
  *     Edge(4L, 5L, 2.0),
  *   )
  * )
  *
  * // Generate a GraphX graph object
  * val graph = Graph(vertices, relationships)
  *
  * // Run calculations
  * val shortestPaths = SingeSourceShortestPath.run(graph,1L)
  *
  * println("ID \t Distance  \t Predecessors")
  * shortestPaths.vertices.collect.foreach( (data) => {
  *   println(s"${data._1} \t ${} \t ${data._2._1 \t ${data._2._2.mkString(",")}")
  * })
  * }}}
  *
  */
object SingleSourceShortestPath {

  /**
    *
    * Given a start vertex and a weighted directed graph this calculates the shortest paths to
    * every other vertex in the graph that can be reached. In the resulting graph every vertex
    * stores the distance to the start node and a list of all direct predecessors on the shortest path.
    *
    * @tparam T the vertex data type
    *
    * @param graph the input graph, edge type must be Double (weight).
    *
    * @param sourceId the start vertex for which shortest paths are calculated
    * iteration
    *
    * @return the resulting graph with distance an direct predecessors attached to each vertex
    *
    */
  def run[T]
      (graph: Graph[T,Double],
       sourceId:VertexId)
    : Graph[(Double,Array[VertexId]), Double] =
  {
    // Create the initial output graph with every distance set to infinity except for the start node
    val ssspGraph = graph.mapVertices( (id, _) =>
      if (id == sourceId)
        (0.0,Array[VertexId]())
      else
        (Double.PositiveInfinity,Array[VertexId]())
    ).cache()

    // Define how to handle incomming messages
    def vertexProgramm(id:VertexId, nodeData:(Double,Array[VertexId]), newData:(Double,Array[VertexId])): (Double,Array[VertexId]) = {
      //if the new path is shorter than the current one replace the current data
      if (nodeData._1 > newData._1)
        newData

      //if both paths are equally long merge the predecessors
      else if (nodeData._1 == newData._1)
        (nodeData._1, nodeData._2 ++ newData._2)

      //the new path is longer, so ignore it
      else
        nodeData
    }

    //define when and to whom we send a message
    def sendMsg(triplet: EdgeTriplet[(Double,Array[VertexId]),Double]): Iterator[(VertexId,(Double,Array[VertexId]))] = {

      //calculate the distance from the start node using my own distance + edge weight
      val tripletDistance = triplet.srcAttr._1 + triplet.attr

      //Send a messaage to the successor if the new path is shorter or if they are equally long and the path is new
      if (
        tripletDistance < triplet.dstAttr._1 ||
        (tripletDistance == triplet.dstAttr._1 &&
        !triplet.dstAttr._2.contains(triplet.srcId) &&
        !(tripletDistance==Double.PositiveInfinity) )
      ){
        Iterator((triplet.dstId, (tripletDistance, Array(triplet.srcId))))
      }

      else {
        Iterator.empty
      }
    }

    def msgCombiner(a:(Double,Array[VertexId]), b:(Double,Array[VertexId])): (Double,Array[VertexId]) = {
      if (a._1 < b._1)
        a
      else if (a._1 == b._1)
        (a._1, a._2 ++ b._2)
      else
        b
    }

    // Start pregel calculation
    Pregel(ssspGraph,(Double.PositiveInfinity,Array[VertexId]()))(
      vertexProgramm,sendMsg,msgCombiner
    )
  }

}
