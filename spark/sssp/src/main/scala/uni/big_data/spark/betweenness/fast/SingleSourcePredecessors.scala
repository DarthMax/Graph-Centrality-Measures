package uni.big_data.spark.betweenness.fast

import org.apache.spark.graphx._

/**
  * SSSP
  * Calculate single source shortest path
  * and store the predecessors of each vertex
  * Created by wolf on 01.12.2015
  **/
object SingleSourcePredecessors {

  // BetweennessBase value (1), Double Variable for calulations (2), Predeccessors (3), Nr of Successors (4)
  def run(graph: Graph[(Double, Double, Array[VertexId], Long), Double], sourceId: VertexId):
  Graph[(Double, Double, Array[VertexId], Long), Double] = {
    def vertexProgramm(id: VertexId,
                       nodeData: (Double, Double, Array[VertexId], Long),
                       newData: (Double, Array[VertexId])):
    ((Double, Double, Array[VertexId], Long)) = {
      if (newData._2.length == 0) {
        // First message
        if (id == sourceId) // Vertex is the source
          (nodeData._1, 0.0, Array[VertexId](), nodeData._4) // Initilize with zero (else infinity)
        else
          (nodeData._1, Double.PositiveInfinity, Array[VertexId](), nodeData._4)
      }
      else if (nodeData._2 > newData._1) // There is a shorter path
        (nodeData._1, newData._1, newData._2, nodeData._4) // remember length and predecessor
      //todo next line is untested
      else if (nodeData._2 == newData._1) // There is a path with equal length
        (nodeData._1, nodeData._2, nodeData._3 ++ newData._2, nodeData._4) // ad vertex to predecessor list
      else
        nodeData
    }

    //todo control messages
    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId], Long), Double]):
    Iterator[(VertexId, (Double, Array[VertexId]))] = {
      val distanceCandidate = triplet.srcAttr._2 + triplet.attr // Path length to source and new length
      if (distanceCandidate < triplet.dstAttr._2 // New shorter path
        || (distanceCandidate != Double.PositiveInfinity // or todo I think infinity messages where sent
        && distanceCandidate == triplet.dstAttr._2 // new path length is the same as old
        && !triplet.dstAttr._3.contains(triplet.srcId))) // and source vertex is not known as predecessor
          Iterator((triplet.dstId, (distanceCandidate, Array(triplet.srcId)))) // send new length and candidate
      else
        Iterator.empty
    }

    def msgCombiner(a: (Double, Array[VertexId]), b: (Double, Array[VertexId])): (Double, Array[VertexId]) = {
      if (a._1 < b._1)
        a
      else if (a._1 == b._1)
        (a._1, a._2 ++ b._2)
      else
        b
    }

    Pregel(graph, (Double.PositiveInfinity, Array[VertexId]()))(
      vertexProgramm, sendMsg, msgCombiner)
  }
}
