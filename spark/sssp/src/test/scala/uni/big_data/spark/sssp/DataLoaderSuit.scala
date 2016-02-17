package uni.big_data.spark.sssp

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.scalatest.FunSuite

/**
  * Created by max on 03.12.15.
  */
class DataLoaderSuit extends FunSuite with SharedSparkContext{
  test("Load Large Datasets") {
    //Dataset is social network edge list found at http://snap.stanford.edu/data/higgs-twitter.html
    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, "data/higgs-social_network.edgelist",true)

    val weightedGraph = graph.mapEdges((e:Edge[Int]) => e.attr.toDouble)
    val shortestPaths = SingleSourceShortestPath.run(weightedGraph,1L)



    assert(456626 == graph.vertices.collect().length)
  }
}