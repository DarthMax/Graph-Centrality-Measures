package uni.big_data.spark.sssp.fast

import org.apache.spark.graphx._

/**
  * This is the mediate phase.
  * Try to find the last elements
  * and find out for each vertex,
  * from which it will get new messages
  * Created by wolf on 04.12.15.
  **/
object SingleSourceSuccessorsFromPredecessors {

  // Betweenness value (1), Double Variable for calulations (2), Predeccessors (3), Successors (4)
  def run[T](graph: Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double], sourceId: VertexId):
  Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double] = {
    // if a vertex has no successors it knows, that it is the last node
    // if there are successors store the node ids in a list to know in next step,
    // if all successors has send messages to the current vertex.
    println("Destinate Successors")
    def vertexProgramm(id: VertexId,
                       nodeData: (Double, Double, Array[VertexId], Array[VertexId]),
                       newData: (Double, Array[VertexId])):
    (Double, Double, Array[VertexId], Array[VertexId]) = {
      //The new Node ist the old Node with sending boolean form new Data and successor list added with new Data of successors
      // the predeccessor is the same
      if (newData._1 == 1.0 && newData._2.length == 0) //initial message (first round)
      //initialize with flag to send
        (nodeData._1, newData._1, nodeData._3, newData._2)
      else
        (nodeData._1, newData._1, nodeData._3, nodeData._4 ++ newData._2)
    }

    // if a vertex is a last vertex: send a messaage to all
    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId], Array[VertexId]), Double]):
    Iterator[(VertexId, (Double, Array[VertexId]))] = {
      // if there is a predeccessor send a message with own id to it.
      // src and dst has to be reversed because it is a directed graph
      // Is the id of the source in the predecessor list of the destionation (value 3)?
      // And there has no message been send yet (stored in value 2
      // Then send a message to the source to let it know which successor there are
      println("one Successor Message send")
      if (triplet.dstAttr._3.contains(triplet.srcId) && (triplet.dstAttr._2 == 1.0)) {
        Iterator((triplet.srcId, (0.0, Array(triplet.dstId))),
          // make destination clear that there is no need to send anymore
          (triplet.dstId, (0.0, Array[VertexId]()))
        )
      }
      else
        Iterator.empty
    }

    def msgCombiner(a: (Double, Array[VertexId]), b: (Double, Array[VertexId])): (Double, Array[VertexId]) = {
      (Math.min(a._1, b._1), a._2 ++ b._2)
    }

    Pregel(graph, (1.0, Array[VertexId]()))(
      vertexProgramm, sendMsg, msgCombiner
    )
  }
}
