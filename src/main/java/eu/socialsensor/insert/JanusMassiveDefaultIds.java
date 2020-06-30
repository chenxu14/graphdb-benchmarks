package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * A Janus insertion strategy for using Jauns-generated vertex ids.
 */
public class JanusMassiveDefaultIds extends JanusMassiveInsertion {

  public JanusMassiveDefaultIds(StandardJanusGraph graph, GraphDatabaseType type) {
    super(graph, type);
  }

  @Override
  public Vertex getOrCreate(String value) {
    // the value used in data files
    final Long longVal = Long.valueOf(value);

    // add to cache for first time
    if (!vertexCache.containsKey(longVal)) {
      vertexCache.put(longVal, tx.addVertex(T.label, JanusMassiveInsertion.NODE_LABEL, NODEID, longVal.intValue()));
    }
    return vertexCache.get(longVal);
  }

}
