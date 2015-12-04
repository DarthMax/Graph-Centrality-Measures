package uni.big_data.spark.sssp

import org.apache.spark.graphx._

/**
  * Created by wolf on 04.12.15.
  **/
object SingleSourceCalcBetweenness {

  def run[T](graph: Graph[(Double, Double, Array[VertexId]), Double], sourceId: VertexId):
  Graph[(Double, Double, Array[VertexId]), Double] = {
    //initialize Graph
    val ssspGraph = graph.mapVertices((id, value) =>
      (value._1, 0.0, value._3)
    ).cache()

    def vertexProgramm(id: VertexId, nodeData: (Double, Double, Array[VertexId]), newData: Double):
    (Double, Double, Array[VertexId]) = {
      if (newData != Double.PositiveInfinity)
        (nodeData._1 + newData, newData / nodeData._3.length, nodeData._3)
      else //first time
        (nodeData._1, 1.0 / nodeData._3.length, nodeData._3)
    }

    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId]), Double]): Iterator[(VertexId, Double)] = {
      if (triplet.dstAttr._2 > 0.0
        && triplet.dstAttr._3.contains(triplet.srcId)
        && triplet.srcId != sourceId)
        Iterator((triplet.srcId, triplet.dstAttr._2),
          (triplet.dstId, 0.0)) //send message to be shure that every betweenness value is only sent one time
      else
        Iterator.empty
    }

    def msgCombiner(a: Double, b: Double): Double = {
      a + b
    }

    Pregel(ssspGraph, Double.PositiveInfinity)(
      vertexProgramm, sendMsg, msgCombiner
    )
  }
}
