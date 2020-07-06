package eu.socialsensor.insert;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.vesoft.nebula.client.graph.GraphClient;
import com.vesoft.nebula.client.graph.ResultSet;

import eu.socialsensor.main.GraphDatabaseType;

public class NebulaSingleInsertion extends InsertionBase<Long> {
  protected static final Logger LOG = LogManager.getLogger();
  protected GraphClient graph;

  public NebulaSingleInsertion(GraphClient client, GraphDatabaseType type) {
    super(type);
    this.graph = client;
  }

  @Override
  public void relateNodes(Long src, Long dest, int edgeId) {
    StringBuilder ngql = new StringBuilder("INSERT EDGE similar() VALUES ").append(src)
        .append(" -> ").append(dest).append(":();");
    int code = graph.execute(ngql.toString());
    LOG.trace(ngql.toString() + ", result code is " + code);
  }

  protected void createVertex(long id) {
    StringBuilder ngql = new StringBuilder("INSERT VERTEX node(nodeId) VALUES ").append(id)
        .append(":(").append(id).append(");");
    int code = graph.execute(ngql.toString());
    LOG.trace(ngql.toString() + ", result code is " + code);
  }

  @Override
  public Long getOrCreate(String value) {
    Long id = Long.valueOf(value) + 1;
    try {
      ResultSet resultSet = graph.executeQuery("FETCH PROP ON node " + id + ";");
      if (resultSet.getRows() == null || resultSet.getRows().isEmpty()) {
        createVertex(id);
      }
    } catch (Exception e) {
      // if target row not exist, NGQLException will throw
      createVertex(id);
    }
    return id;
  }
}
