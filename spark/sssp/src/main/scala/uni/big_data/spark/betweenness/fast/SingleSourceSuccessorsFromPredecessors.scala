package uni.big_data.spark.betweenness.fast

import org.apache.spark.graphx._

/**
  * Calculate for every vertex how many successors there are.
  * Store them in Long value.
  * Created by wolf on 04.12.15.
  **/
object SingleSourceSuccessorsFromPredecessors {

  // BetweennessBase value (1), Double Variable for calulations (2), Predeccessors (3), Nr of Successors (4)
  def run(graph: Graph[(Double, Double, Array[VertexId], Long), Double],
          sourceId: VertexId):
  Graph[(Double, Double, Array[VertexId], Long), Double] = {
    def vertexProgramm(id: VertexId,
                       nodeData: (Double, Double, Array[VertexId], Long),
                       newData: Long):
    (Double, Double, Array[VertexId], Long) = {
      if (newData == Long.MinValue) //initial message (first round)
        (nodeData._1, 1.0, nodeData._3, 0L) //initialize with flag: "ready to sent"
      else
        (nodeData._1, 0.0, nodeData._3, nodeData._4 + newData) //flag: "do not send any more"
    }

    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId], Long), Double]):
    Iterator[(VertexId, Long)] = {
      if (triplet.dstAttr._3.contains(triplet.srcId) // If the source is in predecessor list from destination
        && triplet.dstAttr._2 == 1.0 // and this is the first message
        && triplet.srcId != sourceId) // and the source is not the global sourceId
      {
        Iterator((triplet.srcId, 1L), // There is one successor in source
          (triplet.dstId, 0L)) // No messages needed to be sent from destination
      }
      else
        Iterator.empty
    }

    def msgCombiner(a: Long, b: Long): Long = {
      a + b //sum up successors
    }

    Pregel(graph, Long.MinValue)(
      vertexProgramm, sendMsg, msgCombiner
    )
  }
}
