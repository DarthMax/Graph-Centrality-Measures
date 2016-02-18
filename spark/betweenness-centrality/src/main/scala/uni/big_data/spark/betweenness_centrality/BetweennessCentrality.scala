package uni.big_data.spark.sssp

import org.apache.spark.graphx._

/**
  * Created by max on 17.02.16.
  */
object BetweennessCentrality {

  def run[T](graph: Graph[T,Double]): Graph[Double, Double] ={

    var centralityValues = graph.mapVertices( (id, _) => 0.0)
    var i = 1

    graph.vertices.collect().foreach { (vertex) =>
      println(s"Processing vertex ${i}")
      val sssp = SingleSourceShortestPath.run(graph, vertex._1)
      println("Done sssp")

      val singleCentralityValues = calculateBetwennessCentrality(sssp, vertex._1)
      println("Done centrality")

      centralityValues = centralityValues.joinVertices(singleCentralityValues.vertices)((id, oldValue, newValue) => oldValue + newValue)
      println("Done Merge")

      i = i+1
    }


    centralityValues
  }

  def calculateBetwennessCentrality(graph: Graph[(Double,Array[VertexId]),Double], sourceId: VertexId): Graph[Double, Double] = {

    val centralityValues = graph.mapVertices( (_, value) => (0.0,0.0,value._2))

    def vertexProgramm(id:VertexId, nodeData:(Double, Double,Array[VertexId]), newData: Double): (Double, Double, Array[VertexId]) = {
      if(newData == -1.0)
        (0.0, 1.0 / nodeData._3.length, nodeData._3)
      else
        (nodeData._1 + newData, newData  / nodeData._3.length, nodeData._3)
    }

    def sendMsg(triplet: EdgeTriplet[(Double, Double, Array[VertexId]),Double]): Iterator[(VertexId,Double)] = {
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
