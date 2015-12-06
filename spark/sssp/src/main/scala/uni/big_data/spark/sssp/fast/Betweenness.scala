package uni.big_data.spark.sssp.fast

import org.apache.spark.graphx._

/**
  *
  * value._1 is not touched here. This value is for betweenness.
  * Created by wolf on 01.12.2015
  **/
object Betweenness {

  def run[T](graph: Graph[T, Double]): Graph[Double, Double] = {
    val betweennessGraph = graph.mapVertices((id, _) =>
      (0.0, 0.0, Array[VertexId](), Array[VertexId]())
    ).cache()

    def runBoth(graph: Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double],
                source: (VertexId, (Double, Double, Array[VertexId], Array[VertexId]))
               ): Graph[(Double, Double, Array[VertexId], Array[VertexId]), Double] = {
      println("\n\n" + source._1)
      SingleSourceCalcBetweenness.run(
        SingleSourceSuccessorsFromPredecessors.run(
          SingleSourcePredecessors.run(graph, source._1),
          source._1),
        source._1)
    }

    betweennessGraph.vertices.toLocalIterator.foldLeft(betweennessGraph)(runBoth)
      .mapVertices((id, value) => value._1)
  }

}