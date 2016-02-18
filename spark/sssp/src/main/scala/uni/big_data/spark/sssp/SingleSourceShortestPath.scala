package uni.big_data.spark.sssp

import org.apache.spark.graphx._


/**
  * Created by max on 25.11.15.
  **/
object SingleSourceShortestPath {

  def run[T](graph: Graph[T,Double], sourceId:VertexId): Graph[(Double,Array[VertexId]), Double] =
  {
    val ssspGraph = graph.mapVertices( (id, _) =>
      if (id == sourceId)
        (0.0,Array[VertexId]())
      else
        (Double.PositiveInfinity,Array[VertexId]())
    ).cache()

    def vertexProgramm(id:VertexId, nodeData:(Double,Array[VertexId]), newData:(Double,Array[VertexId])): (Double,Array[VertexId]) = {
      if (nodeData._1 > newData._1)
        newData
      else if (nodeData._1 == newData._1)
        (nodeData._1, nodeData._2 ++ newData._2)
      else
        nodeData
    }

    def sendMsg(triplet: EdgeTriplet[(Double,Array[VertexId]),Double]): Iterator[(VertexId,(Double,Array[VertexId]))] = {
      val tripletDistance = triplet.srcAttr._1 + triplet.attr

      if (tripletDistance < triplet.dstAttr._1 || (tripletDistance == triplet.dstAttr._1 && !triplet.dstAttr._2.contains(triplet.srcId) && !(tripletDistance==Double.PositiveInfinity) )) {
        Iterator((triplet.dstId, (tripletDistance, Array(triplet.srcId))))
      } else {
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

    Pregel(ssspGraph,(Double.PositiveInfinity,Array[VertexId]()))(
      vertexProgramm,sendMsg,msgCombiner
    )
  }

}
