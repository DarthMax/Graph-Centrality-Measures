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
    )//.cache()

    def runBoth(graph: Graph[(Double, Double, Array[VertexId], Long), Double],
                source: (VertexId, (Double, Double, Array[VertexId], Long))
               ): Graph[(Double, Double, Array[VertexId], Long), Double] = {
      println("\n\n" + source._1)
      //graph.vertices.collect
       // .foreach((data) => {
        //  println(s"\tVertex ${data._1}: ${data._2._1}")
      //})
      SingleSourceCalcBetweenness.run(
        SingleSourceSuccessorsFromPredecessors.run(
          SingleSourcePredecessors.run(graph, source._1),
          source._1),
        source._1)
    }
   // val graphCopy = betweennessGraph//.clone().asInstanceOf[Graph[(Double, Double, Array[VertexId], Long), Double]]
    //for (vertex <- graphCopy.vertices.collect()) {
    //  betweennessGraph = runBoth(betweennessGraph, vertex)
   // }
   // graphCopy.mapVertices((id, value) => value._1)
    betweennessGraph.vertices.toLocalIterator.foldLeft(betweennessGraph)(runBoth)
      .mapVertices((id, value) => value._1)
  }

}
