package uni.big_data.spark.betweenness.fast

import org.apache.spark.graphx._

/**
  *
  * value._1 is not touched here. This value is for betweenness.
  * Created by wolf on 01.12.2015
  **/
object BetweennessFast {

  def run[T](graph: Graph[T, Double]): Graph[Double, Double] = {
    var betweennessGraph = graph.mapVertices((id, _) =>
      (0.0, 0.0, Array[VertexId](), 0L)
    )
    def runAll(graph: Graph[(Double, Double, Array[VertexId], Long), Double],
                source: (VertexId, (Double, Double, Array[VertexId], Long))
               ): Graph[(Double, Double, Array[VertexId], Long), Double] = {
     // println(s"Knoten: ${source._1}")
      SingleSourceCalcBetweenness.run(
        SingleSourceSuccessorsFromPredecessors.run(
          SingleSourcePredecessors.run(graph, source._1),
          source._1),
        source._1)
    }
    val graphCopy = betweennessGraph
    for (vertex <- graphCopy.vertices.collect()) {
        betweennessGraph.joinVertices(runAll(betweennessGraph, vertex))
        betweennessGraph
    }
    betweennessGraph.vertices.toLocalIterator.foldLeft(betweennessGraph.cache())(runAll)
    betweennessGraph.mapVertices((id, value) => value._1)
  }

}
