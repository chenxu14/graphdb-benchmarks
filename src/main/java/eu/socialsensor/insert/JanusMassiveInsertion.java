package eu.socialsensor.insert;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in JanusGraph database
 */
public abstract class JanusMassiveInsertion extends InsertionBase<Vertex> {
  private static final Logger logger = LogManager.getLogger();
  protected final StandardJanusGraph graph;
  protected final StandardJanusGraphTx tx;
  Map<Long, JanusGraphVertex> vertexCache;

  public JanusMassiveInsertion(StandardJanusGraph graph, GraphDatabaseType type) {
    super(type, null);
    this.graph = graph;
    Preconditions.checkArgument(graph.getOpenTransactions().isEmpty(),
        "graph may not have open transactions at this point");
    graph.tx().open();
    this.tx = (StandardJanusGraphTx) Iterables.getOnlyElement(graph.getOpenTransactions());
    this.vertexCache = Maps.newHashMap();
  }

  @Override
  public void relateNodes(Vertex src, Vertex dest) {
    src.addEdge(SIMILAR, dest);
  }

  @Override
  protected void post() {
    logger.trace("vertices: " + vertexCache.size());
    tx.commit(); // mutation work is done here
    Preconditions.checkState(graph.getOpenTransactions().isEmpty());
  }

  public static final JanusMassiveInsertion create(StandardJanusGraph graph, GraphDatabaseType type,
      boolean customIds) {
    if (customIds) {
      return new JanusMassiveCustomIds(graph, type);
    } else {
      return new JanusMassiveDefaultIds(graph, type);
    }
  }
}
