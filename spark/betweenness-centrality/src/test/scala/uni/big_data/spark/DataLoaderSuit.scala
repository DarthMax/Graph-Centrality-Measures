package uni.big_data.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.scalatest.FunSuite
import uni.big_data.spark.sssp.SingleSourceShortestPath

/**
  * Created by max on 03.12.15.
  */
class DataLoaderSuit extends FunSuite with SharedSparkContext{
  test("Load Large Datasets") {
    //Dataset is social network edge list found at http://snap.stanford.edu/data/higgs-twitter.html
    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, "data/higgs-social_network.edgelist", canonicalOrientation = true)

    val weightedGraph = graph.mapEdges((e:Edge[Int]) => e.attr.toDouble)
    //val shortestPaths = SingleSourceShortestPath.run(weightedGraph,1L)



    assert(456626 == graph.vertices.collect().length)
  }
}