package uni.big_data.spark.betweenness_centrality.less_messages

import org.apache.spark.graphx._

/**
  * Alternative Implementation of Betweenness Graph Centrality
  *
  * The idea is to reduce the number of messages in collecting step.
  * Each vertex wait till every possible message was sent to it before
  * sending sssp infos to its predecessors.
  * For this a new second step is introduced.
  * The task of this step is to calculate for each vertex how many
  * successors there are.
  *
  * Created by wolf on 01.12.2015
  **/
object BetweennessCentralityLessMessages {

  def run[T](graph: Graph[T, Double]): Graph[Double, Double] = {
    val workingGraph = graph.mapVertices(
      (id, _) => (0.0, 0.0, Array[VertexId](), 0L)
    ).cache()

    var betweennessGraph = graph.mapVertices((id,_) => 0.0).cache()

    val vertices = workingGraph.vertices.collect()

    vertices.foreach { vertex =>
      val shortestPaths = SingleSourcePredecessors.run(workingGraph, vertex._1)

      val successorsAndPredecessors = SingleSourceSuccessorsFromPredecessors.run(shortestPaths, vertex._1)

      val betweennessValues =  SingleSourceCalcBetweenness.run(successorsAndPredecessors, vertex._1)

      betweennessGraph = betweennessGraph.joinVertices(betweennessValues.vertices)((id,a,b) => a + b._1)

      betweennessGraph.cache()
    }
    betweennessGraph
  }
}
