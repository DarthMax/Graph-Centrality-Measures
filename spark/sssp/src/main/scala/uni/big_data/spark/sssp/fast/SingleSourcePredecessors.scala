package uni.big_data.spark.sssp.fast

import org.apache.spark.graphx._

/**
  *
  * value._1 is not touched here. This value is for betweenness.
  * Created by wolf on 01.12.2015
  **/
object SingleSourcePredecessors {

  // Betweenness value (1), Double Variable for calulations (2), Predeccessors (3), Successors (4)
  def run[T](graph: Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double], sourceId: VertexId):
  Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double] = {
    //initialize sssp graph and leave value._1 untouched
    //    val ssspGraph = graph.mapVertices((id, value) =>
    //      if (id == sourceId)
    //        (value._1, 0.0, Array[VertexId](), Array[VertexId]())
    //      else
    //        (value._1, Double.PositiveInfinity, Array[VertexId](), Array[VertexId]())
    //    ).cache()
    println("Destinate Predecessors")
    def vertexProgramm(id: VertexId, nodeData: (Double, Double, Array[VertexId], Array[VertexId]), newData: (Double, Array[VertexId])):
    ((Double, Double, Array[VertexId], Array[VertexId])) = {
      if (newData._2.length == 0) {
        //first message
        if (id == sourceId)
          (nodeData._1, 0.0, Array[VertexId](), Array[VertexId]())
        else
          (nodeData._1, Double.PositiveInfinity, Array[VertexId](), Array[VertexId]())
      }
      else if (nodeData._2 > newData._1)
        (nodeData._1, newData._1, newData._2, nodeData._4)
      //todo next line is untested
      else if (nodeData._2 == newData._1)
        (nodeData._1, nodeData._2, nodeData._3 ++ newData._2, nodeData._4)
      else
        nodeData
    }

    //possibly the same messages where send very often
    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId], Array[VertexId]), Double]):
    Iterator[(VertexId, (Double, Array[VertexId]))] = {
      val distanceCandidate = triplet.srcAttr._2 + triplet.attr
      if (distanceCandidate < triplet.dstAttr._2
        || (distanceCandidate != Double.PositiveInfinity
        && distanceCandidate == triplet.dstAttr._2
        && !triplet.dstAttr._3.contains(triplet.srcId)))
        Iterator((triplet.dstId, (distanceCandidate, Array(triplet.srcId))))
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
