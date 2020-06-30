package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * A Janus massive insertion strategy that uses custom vertex ids. 
 */
public class JanusMassiveCustomIds extends JanusMassiveInsertion {

  private final VertexLabel nodeLabel;
  private final IDManager idManager;

  public JanusMassiveCustomIds(StandardJanusGraph graph, GraphDatabaseType type) {
    super(graph, type);
    this.nodeLabel = tx.getVertexLabel(NODE_LABEL);
    this.idManager = graph.getIDManager();
  }

  @Override
  public Vertex getOrCreate(String value) {
    final Long longVal = Long.valueOf(value);
    final Long longPositiveVal = longVal + 1;
    final long janusVertexId = idManager.toVertexId(longPositiveVal);
    if (!vertexCache.containsKey(longVal)) {
      final JanusGraphVertex vertex = tx.addVertex(janusVertexId, nodeLabel);
      vertex.property(NODEID, longVal.intValue());
      vertexCache.put(longVal, vertex);
    }
    return vertexCache.get(longVal);
  }

}
