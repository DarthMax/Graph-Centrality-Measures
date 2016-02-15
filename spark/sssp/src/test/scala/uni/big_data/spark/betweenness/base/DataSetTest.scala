package uni.big_data.spark.betweenness.base

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.scalatest.FunSuite

/**
  * Created by wilhelm on 15.02.16.
  */
class DataSetTest extends FunSuite with SharedSparkContext{
  test("Load Large Datasets") {
    //Dataset is social network edge list found at http://snap.stanford.edu/data/higgs-twitter.html
    //val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, "data/higgs-social_network_smallest.edgelist",true)
    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, "data/test_from_file.edgelist", canonicalOrientation = true)
    val weightedGraph = graph.mapEdges((e:Edge[Int]) => e.attr.toDouble)
    //val shortestPaths = SingleSourceShortestPath.run(weightedGraph,1L)

    //assert(8 == graph.vertices.collect().length)

    val betweennessGraph = BetweennessBase.run(weightedGraph)
    println("BetweennessBase of vertices done")
    betweennessGraph.vertices.collect
      .foreach((data) => {
        println(s"\tVertex ${data._1}: ${data._2}")
      })



  }
}
