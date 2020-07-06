package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.Iterator;

import com.google.common.base.Preconditions;

import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;

public abstract class GraphDatabaseBase<VertexIteratorType, EdgeIteratorType, VertexType, EdgeType>
    implements GraphDatabase {
  protected final File dbStorageDirectory;
  protected final GraphDatabaseType type;
  protected BenchmarkConfiguration config;

  protected GraphDatabaseBase(GraphDatabaseType type, File dbStorageDirectory, BenchmarkConfiguration config) {
    this.type = type;
    this.config = config;
    this.dbStorageDirectory = dbStorageDirectory;
    if (!this.dbStorageDirectory.exists()) {
      this.dbStorageDirectory.mkdirs();
    }
  }

  public abstract VertexType getVertex(Integer i);

  /**
   * Execute findShortestPaths query from the Query interface
   * @param fromNode
   * @param toNode
   *            any number of random nodes
   */
  public abstract void shortestPath(final VertexType fromNode, Integer toNode);

  public int getNodeCount() {
    return config.getVertexTotal();
  }

  @Override
  public void shortestPaths() {
    // randomness of selected node comes from the hashing function of hash set
    final Iterator<Integer> it = config.getRandomNodes().iterator();
    Preconditions.checkArgument(it.hasNext());
    final VertexType from = getVertex(it.next());
    while (it.hasNext()) {
      shortestPath(from, it.next());
    }
  }
}
