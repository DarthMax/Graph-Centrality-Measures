package uni.big_data.spark.betweenness.fast

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
  * Created by wilhelm on 15.02.16.
  */
class DataSetTest extends FunSuite with SharedSparkContext{
  test("Load Large Datasets") {
    sc.setCheckpointDir("data/")
    //Dataset is social network edge list found at http://snap.stanford.edu/data/higgs-twitter.html
    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(
      sc,
      "data/higgs-social_network_smallest.edgelist",
      //"data/test_from_file.edgelist",
      canonicalOrientation = true)
    //val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc,,true)
    val weightedGraph = graph.mapEdges((e:Edge[Int]) => e.attr.toDouble)
    //val shortestPaths = SingleSourceShortestPath.run(weightedGraph,1L)

    //assert(8 == graph.vertices.collect().length)

    val betweennessGraph = BetweennessFast.run(weightedGraph)
    println("BetweennessBase of vertices done")
    //betweennessGraph.vertices.collect
    //  .foreach((data) => {
    //    println(s"\tVertex ${data._1}: ${data._2}")
    //  })



  }
}
