package bigdata.GellyTest;

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

/**
 * Hello world!
 *
 */
public class App 
{
	
	public static String source = "";
	
    public static void main( String[] args )
    {
   
    	// graph creation
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    	
    	ArrayList< Vertex< String, Tuple2< Double, ArrayList< String > > > > vertices = new ArrayList< Vertex< String, Tuple2< Double, ArrayList< String > > > >();
    	vertices.add( new Vertex< String, Tuple2< Double, ArrayList< String > > >( "A", new Tuple2< Double, ArrayList< String > >( 0.0, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Double, ArrayList< String > > >( "B", new Tuple2< Double, ArrayList< String > >( Double.POSITIVE_INFINITY, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Double, ArrayList< String > > >( "C", new Tuple2< Double, ArrayList< String > >( Double.POSITIVE_INFINITY, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Double, ArrayList< String > > >( "D", new Tuple2< Double, ArrayList< String > >( Double.POSITIVE_INFINITY, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Double, ArrayList< String > > >( "E", new Tuple2< Double, ArrayList< String > >( Double.POSITIVE_INFINITY, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Double, ArrayList< String > > >( "F", new Tuple2< Double, ArrayList< String > >( Double.POSITIVE_INFINITY, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Double, ArrayList< String > > >( "G", new Tuple2< Double, ArrayList< String > >( Double.POSITIVE_INFINITY, new ArrayList< String >() ) ) );
    	
    	ArrayList< Edge< String, Double > > edges = new ArrayList< Edge< String, Double > >();
    	edges.add( new Edge< String, Double >( "A", "B", 7.0 ) );
    	edges.add( new Edge< String, Double >( "A", "D", 5. ) );
    	edges.add( new Edge< String, Double >( "B", "C", 8. ) );
    	edges.add( new Edge< String, Double >( "B", "D", 9. ) );
    	edges.add( new Edge< String, Double >( "B", "E", 7. ) );
    	edges.add( new Edge< String, Double >( "C", "E", 5. ) );
    	edges.add( new Edge< String, Double >( "D", "E", 15. ) );
    	edges.add( new Edge< String, Double >( "D", "F", 6. ) );
    	edges.add( new Edge< String, Double >( "E", "F", 8. ) );
    	edges.add( new Edge< String, Double >( "E", "G", 8. ) );
    	edges.add( new Edge< String, Double >( "F", "G", 11. ) );
    	
    	Graph< String, Tuple2< Double, ArrayList< String > >, Double > graph = Graph.fromCollection(vertices, edges, env);
    	
        // iterative processing
        int maxIterations = 100;
        
        source = "A";
        Graph< String, Tuple2< Double, ArrayList< String > >, Double > result = graph.runVertexCentricIteration( new VertexDistanceUpdater(),  new MinDistanceMessenger(), maxIterations);
        
        DataSet< Vertex< String, Tuple2< Double, ArrayList< String > > > > sssp = result.getVertices();
        
        try {
			sssp.print();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        
    }

	@SuppressWarnings("serial")
	public static final class MinDistanceMessenger extends MessagingFunction< String, Tuple2< Double, ArrayList< String > >, Tuple2< Double, ArrayList< String > >, Double > {
	
		@Override
		public void sendMessages( Vertex< String, Tuple2< Double, ArrayList< String > > > vertex ) throws Exception {
			
			/*vertex.f1.f1 = new ArrayList< String>();
			vetex.f1.f1.add( vertex.f0 );*/
			for( Edge< String, Double > edge : getEdges() ) {
//				if( vertex.f1.f0 != -1L ) sendMessageTo( edge.getTarget(), new Tuple2< Double, ArrayList< String > >( vertex.f1.f0 + edge.getValue(), vertex.f1.f1 ) );
//				else sendMessageTo( edge.getTarget(), new Tuple2< Double, ArrayList< String > >( 0 + edge.getValue(), vertex.f1.f1 ) );
				ArrayList< String > temp = new ArrayList< String >();
				temp.add( vertex.f0 );
				sendMessageTo( edge.getTarget(), new Tuple2< Double, ArrayList< String > >( vertex.f1.f0 + edge.getValue(), temp ) );
				//System.out.println( vertex.f0 + ": " + (vertex.f1.f0 + edge.getValue()) );
			}
		}
		
	}
	
	@SuppressWarnings("serial")
	public static final class VertexDistanceUpdater extends VertexUpdateFunction< String, Tuple2< Double, ArrayList< String > >, Tuple2< Double, ArrayList< String > > > {
	
		@Override
		public void updateVertex( Vertex<String, Tuple2< Double, ArrayList< String > > > vertex, MessageIterator< Tuple2< Double, ArrayList< String > > > inMessages ) throws Exception {
			Double minDistance = vertex.getValue().f0;
			ArrayList< String > predecessors = vertex.f1.f1;
			
			for( Tuple2< Double, ArrayList< String > > msg : inMessages ) {
				if( msg.f0 < minDistance ) {
					minDistance = msg.f0;
					predecessors = new ArrayList< String >( msg.f1 );
				}
				else if( msg.f0 == minDistance && !vertex.f1.f1.contains( msg.f1.get(0)) ) {
					predecessors.add( msg.f1.get(0) );
					System.out.println( msg.f1.get(0) );
				}
			}
			
			//if( vertex.getValue().f0 > minDistance ) {
			setNewVertexValue( new Tuple2< Double, ArrayList< String > >( minDistance, predecessors ) );
			//}
		}
		
	}

}




















