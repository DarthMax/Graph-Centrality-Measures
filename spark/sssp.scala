import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Set Source
val sourceId:VertexId = 1L


// Create Vertices
val vertices: RDD[(VertexId,Int)] = sc.parallelize(
  Array(
    (1L,0),
    (2L,0),
    (3L,0),
    (4L,0),
    (5L,0),
    (6L,0),
    (7L,0)
  )
)

// Create an RDD for edges
val relationships: RDD[Edge[Int]] = sc.parallelize(
  Array(
    Edge(1L, 2L, 1),
    Edge(1L, 3L, 1),
    Edge(3L, 4L, 1),
    Edge(3L, 5L, 1),
    Edge(4L, 6L, 1),
    Edge(4L, 7L, 1),
    Edge(5L, 7L, 1)
  )
)


// Create the graph
val graph = Graph(vertices, relationships)

// Set initial distances to infinity and 0 for source
val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) (0.0,Array[VertexId]()) else (Double.PositiveInfinity,Array[VertexId]()))

// use Dijkstra to calculate SSSP
val sssp = initialGraph.pregel((Double.PositiveInfinity,Array[VertexId]()))(

  //Transform vertice using new message
  (id, nodeData, newData) => {
    if (nodeData._1 > newData._1)
      newData
    else if (nodeData._1 == newData._1)
      (nodeData._1, nodeData._2 ++ newData._2)
    else
      nodeData
  },

  // sent messages to neighbours
  triplet => {  // Send Message
    val tripletDistance = triplet.srcAttr._1 + triplet.attr

    if (tripletDistance < triplet.dstAttr._1 || (tripletDistance == triplet.dstAttr._1 && !triplet.dstAttr._2.contains(triplet.srcId) )) {
      Iterator((triplet.dstId, (tripletDistance, Array(triplet.srcId))))
    } else {
      Iterator.empty
    }
  },

  // select message
  (a,b) => {
    if (a._1 < b._1)
      a
    else if (a._1 == b._1)
      (a._1, a._2 ++ b._2)
    else
      b
  }
)

// Print
sssp.vertices.collect.foreach( (data) => {
    println(s"${data._1}: -----")
    println(s"Distance: ${data._2._1}")
    println(s"Predecessors: ${data._2._2.mkString(",")}")
})
