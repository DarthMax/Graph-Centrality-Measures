import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Set Source
val sourceId:VertexId = 1L                       


// Create Vertices
val vertices: RDD[(VertexId, Double)] =
  sc.parallelize(
    Array(
        (1L, 7.),
        (2L, 3.),
        (3L, 2.),
        (4L, 6.)
    )
  )

// Create an RDD for edges
val relationships: RDD[Edge[Int]] =
  sc.parallelize(Array(Edge(1L, 2L, 1), Edge(1L, 4L, 1),
                       Edge(2L, 4L, 1), Edge(3L, 1L, 1), 
                       Edge(3L, 4L, 1)))

                       
// Create the graph
val graph = Graph(vertices, relationships)

// Set initial distances to infinity and 0 for source
val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

// use Dijkstra to calculate SSSP
val sssp = initialGraph.pregel(Double.PositiveInfinity)(

  //Transform vertice using new message  
  (id, dist, newDist) => math.min(dist, newDist),
  
  // sent messages to neighbours
  triplet => {  // Send Message
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  
  // select message
  (a,b) => math.min(a,b) // Merge Message
)
  
// Print
println(sssp.vertices.collect.mkString("\n"))