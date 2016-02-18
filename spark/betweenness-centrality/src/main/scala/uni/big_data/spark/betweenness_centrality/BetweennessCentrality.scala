package uni.big_data.spark.betweenness_centrality

import org.apache.spark.graphx._
import uni.big_data.spark.sssp.SingleSourceShortestPath

/**
  * Implements an solution to calculate betweeness centrality values for a given graph.
  * [https://en.wikipedia.org/wiki/Betweenness_centrality]
  *
  * @example Given a simple graph we can calculate the betwenness centrality values:
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
  * val centralityValues = BetweenessCentrality.run(graph)
  *
  * println("ID \t Betweeness Centrality")
  * shortestPaths.vertices.collect.foreach( (data) => {
  *   println(s"${data._1} \t ${} \t ${data._2")
  * })
  * }}}
  *
  */
object BetweennessCentrality {

  /**
    *
    * Given a weighted directed graph this calculates the betweenness centrality for every vertex in the graph.
    * This value states the number of shortest paths for all vertices to all vertices that pass through that node
    * and can be used as a measurement for vertex importance.
    * The values are calculated using the following algorithm:
    *
    * (1) For every vertex v in the graph
    *   (2) calculate the shortest path to all other vertices
    *   (3) traverse these shortest paths in opposite direction and increase the counter for every vertex on that path

    *
    * @tparam T the vertex data type
    *
    * @param graph the input graph, edge type must be Double (weight).
    *
    * @return the resulting graph with betweenness centrality values attached to each vertex
    *
    */
  def run[T](graph: Graph[T,Double]): Graph[Double, Double] ={

    //Initialize the betweenness graph with value 0 for every vertex
    var centralityValues = graph.mapVertices( (id, _) => 0.0)
    var i = 1

    // For every vertex in the graph
    graph.vertices.collect().foreach { (vertex) =>

      //calculate the shortest paths
      val sssp = SingleSourceShortestPath.run(graph, vertex._1)

      //calculate the centrality values for these paths
      val singleCentralityValues = calculateBetwennessCentrality(sssp, vertex._1)

      //merge local centrality values with the global ones
      centralityValues = centralityValues.joinVertices(singleCentralityValues.vertices)(
        (id, oldValue, newValue) => oldValue + newValue
      )
    }


    centralityValues
  }

  /**
    * Given a graph with shortest paths for one vertex calculate Betweenness Centrality values for each vertex
    *
    * @param graph the input graph, vertex type is (Double, Array[VertexId]) edge type must be Double (weight).
    *
    * @return the resulting graph with betweenness centrality values attached to each vertex
    *
    */
  private def calculateBetwennessCentrality(graph: Graph[(Double,Array[VertexId]),Double], sourceId: VertexId)
    : Graph[Double, Double] =
  {

    //initialize calculation graph (centrality value, value of last message, predecessors)
    val centralityValues = graph.mapVertices( (_, value) => (0.0,0.0,value._2))

    def vertexProgramm(id:VertexId, nodeData:(Double, Double,Array[VertexId]), newData: Double): (Double, Double, Array[VertexId]) = {
      //if its the initial message initialize the last message value with 1/#predecessors
      if(newData == -1.0)
        (0.0, 1.0 / nodeData._3.length, nodeData._3)

      // for every other message add the message value to centrality value and set the to be send value
      else
        (nodeData._1 + newData, newData  / nodeData._3.length, nodeData._3)
    }

    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId]),Double]): Iterator[(VertexId,Double)] = {
      //Send the to be send value to every predecessor
      if(triplet.dstAttr._2 > 0 && triplet.dstAttr._3.contains(triplet.srcId) && !(triplet.srcId==sourceId)) {
        Iterator( (triplet.srcId, triplet.dstAttr._2), (triplet.dstId,0.0) )
      } else {
        Iterator.empty
      }
    }

    def msgCombiner(a:Double, b:Double): Double = {
      a+b
    }

    Pregel(centralityValues,-1.0)(
      vertexProgramm,sendMsg,msgCombiner
    ).mapVertices((_,values) => values._1)

  }
}
