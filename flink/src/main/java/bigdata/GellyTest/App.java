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
    	
    	ArrayList< Vertex< String, Tuple2< Long, ArrayList< String > > > > vertices = new ArrayList< Vertex< String, Tuple2< Long, ArrayList< String > > > >();
    	vertices.add( new Vertex< String, Tuple2< Long, ArrayList< String > > >( "A", new Tuple2< Long, ArrayList< String > >( -1L, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Long, ArrayList< String > > >( "B", new Tuple2< Long, ArrayList< String > >( -1L, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Long, ArrayList< String > > >( "C", new Tuple2< Long, ArrayList< String > >( -1L, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Long, ArrayList< String > > >( "D", new Tuple2< Long, ArrayList< String > >( -1L, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Long, ArrayList< String > > >( "E", new Tuple2< Long, ArrayList< String > >( -1L, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Long, ArrayList< String > > >( "F", new Tuple2< Long, ArrayList< String > >( -1L, new ArrayList< String >() ) ) );
    	vertices.add( new Vertex< String, Tuple2< Long, ArrayList< String > > >( "G", new Tuple2< Long, ArrayList< String > >( -1L, new ArrayList< String >() ) ) );
    	
    	ArrayList< Edge< String, Long > > edges = new ArrayList< Edge< String, Long > >();
    	edges.add( new Edge< String, Long >( "A", "B", 7l ) );
    	edges.add( new Edge< String, Long >( "A", "D", 5l ) );
    	edges.add( new Edge< String, Long >( "B", "C", 8l ) );
    	edges.add( new Edge< String, Long >( "B", "D", 9l ) );
    	edges.add( new Edge< String, Long >( "B", "E", 7l ) );
    	edges.add( new Edge< String, Long >( "C", "E", 5l ) );
    	edges.add( new Edge< String, Long >( "D", "E", 15l ) );
    	edges.add( new Edge< String, Long >( "D", "F", 6l ) );
    	edges.add( new Edge< String, Long >( "E", "F", 8l ) );
    	edges.add( new Edge< String, Long >( "E", "G", 9l ) );
    	edges.add( new Edge< String, Long >( "F", "G", 11l ) );
    	
    	Graph< String, Tuple2< Long, ArrayList< String > >, Long > graph = Graph.fromCollection(vertices, edges, env);
    	
        // iterative processing
        int maxIterations = 100;
        
        source = "A";
        Graph< String, Tuple2< Long, ArrayList< String > >, Long > result = graph.runVertexCentricIteration( new VertexDistanceUpdater(),  new MinDistanceMessenger(), maxIterations);
        
        DataSet< Vertex< String, Tuple2< Long, ArrayList< String > > > > sssp = result.getVertices();
        
        try {
			sssp.print();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        
    }

	@SuppressWarnings("serial")
	public static final class MinDistanceMessenger extends MessagingFunction< String, Tuple2< Long, ArrayList< String > >, Tuple2< Long, ArrayList< String > >, Long > {
	
		@Override
		public void sendMessages( Vertex< String, Tuple2< Long, ArrayList< String > > > vertex ) throws Exception {
			
			vertex.f1.f1.add( vertex.f0 );
			for( Edge< String, Long > edge : getEdges() ) {
				if( vertex.f1.f0 != -1L ) sendMessageTo( edge.getTarget(), new Tuple2< Long, ArrayList< String > >( vertex.f1.f0 + edge.getValue(), vertex.f1.f1 ) );
				else sendMessageTo( edge.getTarget(), new Tuple2< Long, ArrayList< String > >( 0 + edge.getValue(), vertex.f1.f1 ) );
				//System.out.println( vertex.f0 + ": " + (vertex.f1.f0 + edge.getValue()) );
			}
		}
		
	}
	
	@SuppressWarnings("serial")
	public static final class VertexDistanceUpdater extends VertexUpdateFunction< String, Tuple2< Long, ArrayList< String > >, Tuple2< Long, ArrayList< String > > > {
	
		@Override
		public void updateVertex( Vertex<String, Tuple2< Long, ArrayList< String > > > vertex, MessageIterator< Tuple2< Long, ArrayList< String > > > inMessages ) throws Exception {
			Long minDistance = Long.MAX_VALUE;
			ArrayList< String > predecessors = new ArrayList< String >();
			
			for( Tuple2< Long, ArrayList< String > > msg : inMessages ) {
				if( msg.f0 < minDistance && msg.f1.contains( source ) ) {
					minDistance = msg.f0;
					predecessors = new ArrayList< String >( msg.f1 );
				}
			}
			
			if( vertex.getValue().f0 > minDistance || vertex.getValue().f0 == -1L ) {
				setNewVertexValue( new Tuple2< Long, ArrayList< String > >( minDistance, predecessors ) );
			}
		}
		
	}

}




















