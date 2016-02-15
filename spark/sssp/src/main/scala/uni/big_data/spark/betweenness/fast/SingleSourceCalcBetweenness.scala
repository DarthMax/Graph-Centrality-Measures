package uni.big_data.spark.betweenness.fast

import org.apache.spark.graphx._

/**
  * Calculate the betweenness value for each vertex.
  * Sending back betweenness knowledge to each predecessor
  * Created by wolf on 04.12.15.
  **/
object SingleSourceCalcBetweenness {

  // BetweennessBase value (1), Double Variable for calulations (2), Predeccessors (3), Nr of Successors (4)
  def run(graph: Graph[(Double, Double, Array[VertexId], Long), Double], sourceId: VertexId):
  Graph[(Double, Double, Array[VertexId], Long), Double] = {
    println("calc BetweennessBase")
    def vertexProgramm(id: VertexId,
                       nodeData: (Double, Double, Array[VertexId], Long),
                       newData: (Double, Long)):
    (Double, Double, Array[VertexId], Long) = {
      // send messages only if vertex have got all messages form successors.
      // every time this vertex gets a message it substracts number of successors from counter (4)
      if (newData._2 == Long.MinValue) // Initial message
        (nodeData._1, newData._1, nodeData._3, nodeData._4)
      else if (newData._2 != 0L) {
        // Collecting mode:
        (nodeData._1 + newData._1, //  add values to own betweenness
          nodeData._2 + newData._1, //  add values to 'betweenness to send'
          nodeData._3, //  leave predecessor list untouched
          nodeData._4 - newData._2) //  subtract the number of successor
      }
      else {
        // Everything is done mode:
        assert(nodeData._4 == 0L) //  control if there are really no successors left
        (nodeData._1, 0.0, Array[VertexId](), nodeData._4) //  reset everything but the betweenness value (1)
      }
    }

    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId], Long), Double]):
    Iterator[(VertexId, (Double, Long))] = {

      // except one predecessor is the source!
      if (triplet.dstAttr._4 == 0L // if the successor counter is zero
        && triplet.dstAttr._3.length != 0 // and there are predecessors to be informed
        && !triplet.dstAttr._3.contains(sourceId)) // and non of the predecessors are the global source
      {
        Iterator((triplet.srcId, (triplet.dstAttr._2 / triplet.dstAttr._3.length, 1L)), // a message to the predecessor
          (triplet.dstId, (0.0, 0L))) // No messages needed to be sent from destination
      }
      else
        Iterator.empty
    }

    def msgCombiner(a: (Double, Long), b: (Double, Long)): (Double, Long) = {
      (a._1 + b._1, a._2 + b._2)
    }
    Pregel(graph, (1.0, Long.MinValue))(
      vertexProgramm, sendMsg, msgCombiner
    )
  }
}
