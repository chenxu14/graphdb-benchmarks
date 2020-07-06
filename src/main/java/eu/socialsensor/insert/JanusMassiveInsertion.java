package eu.socialsensor.insert;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in JanusGraph database
 */
public class JanusMassiveInsertion extends InsertionBase<Vertex> {
  private static final Logger logger = LogManager.getLogger();
  protected final StandardJanusGraph graph;
  protected final StandardJanusGraphTx tx;
  Map<Long, JanusGraphVertex> vertexCache;
  private final IDManager idManager;
  private final VertexLabel nodeLabel;

  public JanusMassiveInsertion(StandardJanusGraph graph, GraphDatabaseType type) {
    super(type);
    this.graph = graph;
    Preconditions.checkArgument(graph.getOpenTransactions().isEmpty(),
        "graph may not have open transactions at this point");
    graph.tx().open();
    this.tx = (StandardJanusGraphTx) Iterables.getOnlyElement(graph.getOpenTransactions());
    this.vertexCache = Maps.newHashMap();
    this.nodeLabel = tx.getVertexLabel(NODE_LABEL);
    this.idManager = graph.getIDManager();
  }

  @Override
  public void relateNodes(Vertex src, Vertex dest, int edgeId) {
    src.addEdge(SIMILAR, dest);
  }

  @Override
  protected void postInsert() {
    logger.trace("vertices: " + vertexCache.size());
    tx.commit(); // mutation work is done here
    Preconditions.checkState(graph.getOpenTransactions().isEmpty());
  }

  @Override
  public Vertex getOrCreate(String value) {
    Long id = Long.valueOf(value) + 1;
    final long janusVertexId = idManager.toVertexId(id);
    if (!vertexCache.containsKey(id)) {
      final JanusGraphVertex vertex = tx.addVertex(janusVertexId, nodeLabel);
      vertex.property(NODEID, id.intValue());
      vertexCache.put(id, vertex);
    }
    return vertexCache.get(id);
  }
}
