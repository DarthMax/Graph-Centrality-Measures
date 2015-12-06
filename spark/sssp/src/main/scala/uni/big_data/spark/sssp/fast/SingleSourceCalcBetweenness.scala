package uni.big_data.spark.sssp.fast

import org.apache.spark.graphx._

/**
  * Created by wolf on 04.12.15.
  **/
object SingleSourceCalcBetweenness {

  // Betweenness value (1), Double Variable for calulations (2), Predeccessors (3), Successors (4)
  def run[T](graph: Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double], sourceId: VertexId):
  Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double] = {
    println("Calculate Betweenness")
    def vertexProgramm(id: VertexId,
                       nodeData: (Double, Double, Array[VertexId], Array[VertexId]),
                       newData: (Array[VertexId], Double)):
    (Double, Double, Array[VertexId], Array[VertexId]) = {
      // send messages only if vertex have got all messages form successors.
      // every time this vertex gets a message it deletes the successor id from own list (4)
      if (newData._1.length == 0) //first message
        (nodeData._1, newData._2, nodeData._3, nodeData._4)
      else if (newData._1.contains(id)) {
        //sign that the vertex have already sent betweenness to predecessor
        // leave betweenness untouched
        // reset betweennness variable,
        // SET PREDECESSOR LIST empty to prevent from sending new messages
        // Leave Successor list empty
        assert(nodeData._4.length == 0)
        (nodeData._1, 0.0, Array[VertexId](), nodeData._4) //to sign that no messages have to be sent any more successor array set t o empty array
      }
      else // Collecting mode:
        (nodeData._1 + newData._2, //add values to own betweenness
          nodeData._2 + newData._2, // add values to betweenness to send
          nodeData._3,
          nodeData._4.filter(suc => !newData._1.contains(suc)) //delete id from successor list
          )
    }

    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId], Array[VertexId]), Double]):
    Iterator[(VertexId, (Array[VertexId], Double))] = {
      // if the successor list is empty and predecessor list is not empty send message to all predecessors
      // except one predecessor is the source!
      if (triplet.dstAttr._4.length == 0 && triplet.dstAttr._3.length != 0 && !triplet.dstAttr._3.contains(sourceId)) {
        //        println("\nMessage")
        //        println("To: " + triplet.srcId)
        //        println("From: " + triplet.dstId)
        //        println("betweenness to send : " + triplet.dstAttr._2)
        //        println("number of vertexes to send to : " + triplet.dstAttr._3.length)
        Iterator((triplet.srcId, (Array(triplet.dstId), triplet.dstAttr._2 / triplet.dstAttr._3.length)),
          (triplet.dstId, (Array(triplet.dstId), 0.0))) //send message to be sure that every betweenness value
        // is only sent one time "I have received the message"
      }
      else
        Iterator.empty
    }

    def msgCombiner(a: (Array[VertexId], Double), b: (Array[VertexId], Double)): (Array[VertexId], Double) = {
      (a._1 ++ b._1, a._2 + b._2)
    }

    Pregel(graph, (Array[VertexId](), 1.0))(
      vertexProgramm, sendMsg, msgCombiner
    )
  }
}
