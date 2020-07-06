package eu.socialsensor.insert;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Janus graph database
 */
public class JanusSingleInsertion extends InsertionBase<Vertex> {
  private static final Logger LOG = LogManager.getLogger();
  private final StandardJanusGraph graph;
  private final IDManager idManager;

  public JanusSingleInsertion(StandardJanusGraph jaunsGraph, GraphDatabaseType type) {
    super(type);
    this.graph = jaunsGraph;
    this.idManager = graph.getIDManager();
  }

  @Override
  public Vertex getOrCreate(String value) {
    Long id = Long.valueOf(value) + 1;
    final long janusVertexId = idManager.toVertexId(id);
    final GraphTraversal<Vertex, Vertex> t = graph.traversal().V(janusVertexId);
    Vertex vertex;
    if (t.hasNext()) {
      vertex = t.next();
    } else {
      vertex = graph.addVertex(T.label, NODE_LABEL, T.id, janusVertexId, NODEID, id.intValue());
      LOG.trace("add new Vertex, id is {}, realId is {}", id, janusVertexId);
      graph.tx().commit();
    }
    return vertex;
  }

  @Override
  public void relateNodes(Vertex src, Vertex dest, int edgeId) {
    try {
      src.addEdge(SIMILAR, dest);
      LOG.trace("add new Edge, src is {}, dest is {}", src.id(), dest.id());
      graph.tx().commit();
    } catch (Exception e) {
      graph.tx().rollback();
    }
  }
}
