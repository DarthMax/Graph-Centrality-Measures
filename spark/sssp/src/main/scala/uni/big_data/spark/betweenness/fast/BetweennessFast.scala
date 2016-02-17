package uni.big_data.spark.betweenness.fast

import org.apache.spark.graphx._

/**
  *
  * value._1 is not touched here. This value is for betweenness.
  * Created by wolf on 01.12.2015
  **/
object BetweennessFast {

  def run[T](graph: Graph[T, Double]): Graph[Double, Double] = {
    val workingGraph = graph.mapVertices((id, _) =>
      (0.0, 0.0, Array[VertexId](), 0L)
    ).cache()

    var betweennessGraph = graph.mapVertices((id,_) => 0.0)


    betweennessGraph.vertices.collect().foreach { vertex =>
      val shortestPaths = SingleSourcePredecessors.run(workingGraph, vertex._1)
      val successorsAndPredecessors = SingleSourceSuccessorsFromPredecessors.run(shortestPaths, vertex._1)
      val betweennessValues =  SingleSourceCalcBetweenness.run(successorsAndPredecessors, vertex._1)

      betweennessGraph = betweennessGraph.joinVertices(betweennessValues.vertices)((id,a,b) => a + b._1)
      betweennessGraph.cache()
    }

    betweennessGraph
    //betweennessGraph.vertices.toLocalIterator.foldLeft(betweennessGraph)(runBoth)
    //   .mapVertices((id, value) => value._1)
  }

}
