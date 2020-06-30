package eu.socialsensor.insert;

import java.io.File;
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
  private final StandardJanusGraph graph;
  private final IDManager idManager;

  public JanusSingleInsertion(StandardJanusGraph jaunsGraph, GraphDatabaseType type, File resultsPath) {
    super(type, resultsPath);
    this.graph = jaunsGraph;
    this.idManager = graph.getIDManager();
  }

  @Override
  public Vertex getOrCreate(String value) {
    final Long longVal = Long.valueOf(value);
    final Long longPositiveVal = longVal + 1;
    final long janusVertexId = idManager.toVertexId(longPositiveVal);
    final GraphTraversal<Vertex, Vertex> t = graph.traversal().V(T.id, janusVertexId);
    // TODO use Map to cache this
    final Vertex vertex = t.hasNext() ? t.next()
        : graph.addVertex(T.label, NODE_LABEL, T.id, janusVertexId, NODEID, longVal.intValue());
    graph.tx().commit();
    return vertex;
  }

  @Override
  public void relateNodes(Vertex src, Vertex dest) {
    try {
      src.addEdge(SIMILAR, dest);
      graph.tx().commit();
    } catch (Exception e) {
      graph.tx().rollback();
    }
  }
}
