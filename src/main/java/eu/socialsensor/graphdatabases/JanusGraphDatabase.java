package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chen.janusgraph.client.JanusGraphClient;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import eu.socialsensor.insert.JanusMassiveInsertion;
import eu.socialsensor.insert.JanusSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

public class JanusGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge> {
  private static final Logger LOG = LogManager.getLogger();
  private final JanusGraph graph;
  private final BenchmarkConfiguration config;

  public JanusGraphDatabase(GraphDatabaseType type, BenchmarkConfiguration config, File dbStorageDirectory) {
    super(type, dbStorageDirectory, config);
    this.config = config;
    if (!GraphDatabaseType.JANUS_FLAVORS.contains(type)) {
      throw new IllegalArgumentException(String.format("The graph database %s is not a Janus database.",
          type == null ? "null" : type.name()));
    }
    Properties props = new Properties();
    props.put("graph.set-vertex-id", "true");
    boolean batchLoading = config.isBatch();
    props.put("storage.batch-loading", batchLoading);
    props.put("storage.transactions", !batchLoading);
    props.put("storage.buffer-size", config.getBatchSize());
    props.put("storage.page-size", config.getJanusPageSize());
    props.put("storage.parallel-backend-ops", "true");
    props.put("ids.block-size", config.getJanusIdsBlocksize());
    props.put("query.force-index", "false");
    graph = JanusGraphClient.getInstance("janus-benchmark", props);
  }

  @Override
  public Vertex getVertex(Integer id) {
    long vertexId = ((StandardJanusGraph)graph).getIDManager().toVertexId(id);
    final Vertex vertex = graph.traversal().V(vertexId).next();
    return vertex;
  }

  public void khopQuery() {
    for (Integer id : config.getRandomNodes()) {
      long vertexId = ((StandardJanusGraph)graph).getIDManager().toVertexId(id);
      GraphTraversal<Vertex, Vertex> g = graph.traversal().V(vertexId);
      for (int i = 0; i < config.getKpopStep(); i++) {
        g = g.out(SIMILAR);
      }
      if (config.withProps()) {
        g = g.values(NODE_ID);
      }
      int size = g.toSet().size();
      LOG.trace("khop query, from vertex {}, step is {}, withProps is {}, result size is {}",
          vertexId, config.getKpopStep(), config.withProps(), size);
    }
  }

  @Override
  public void massiveModeLoading() {
    JanusMassiveInsertion janusMassiveInsertion = new JanusMassiveInsertion((StandardJanusGraph)graph, type);
    janusMassiveInsertion.createGraph(config.getDataset());
  }

  @Override
  public void singleModeLoading() {
    JanusSingleInsertion janusSingleInsertion = new JanusSingleInsertion((StandardJanusGraph)graph, type);
    janusSingleInsertion.createGraph(config.getDataset());
  }

  @Override
  public void shutdown() {
    graph.close();
  }

  @Override
  public void delete() {
    try {
      JanusGraphFactory.drop(graph);
    } catch (BackendException e) {
      LOG.error(e.getMessage(), e);
    }
    Utils.deleteRecursively(dbStorageDirectory);
  }

  @Override
  public void shortestPath(Vertex fromNode, Integer targetNode) {
    final GraphTraversalSource g = graph.traversal();
    final Stopwatch watch = Stopwatch.createStarted();
    int maxHops = this.config.getShortestPathMaxHops();
    final DepthPredicate maxDepth = new DepthPredicate(maxHops);
    final Integer fromNodeId = fromNode.<Integer>value(NODE_ID);
    LOG.debug("finding path from {} to {} max hops {}", fromNodeId, targetNode, maxHops);
    // g.V().has("nodeId", 775).repeat(out('similar').simplePath()).until(has('nodeId', 990)
    // .and().filter {it.path().size() <= 6}).limit(1).path().by('nodeId')
    final GraphTraversal<?, Path> t = g.V().has(NODE_ID, fromNodeId)
        .repeat(__.both(SIMILAR).simplePath())
        .until(__.has(NODE_ID, targetNode).and(__.filter(maxDepth)))
        .limit(1).path().by("nodeId");
    t.tryNext().ifPresent(it -> {
      final int pathSize = it.size();
      final long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
      watch.stop();
      if (elapsed > 2000) { // threshold for debugging
        LOG.warn("from @ " + fromNode.value(NODE_ID) + " to @ " + targetNode.toString() + " took " + elapsed
            + " ms, " + pathSize + ": " + it.toString());
      }
    });
  }

  @Override
  public Set<Integer> getNeighborsIds(int nodeId) {
    final Vertex vertex = getVertex(nodeId);
    Set<Integer> neighbors = new HashSet<Integer>();
    Iterator<Vertex> iter = vertex.vertices(Direction.OUT, SIMILAR);
    while (iter.hasNext()) {
      Integer neighborId = Integer.valueOf(iter.next().property(NODE_ID).value().toString());
      neighbors.add(neighborId);
    }
    return neighbors;
  }

  public double getNodeInDegree(Vertex vertex) {
    return (double) Iterators.size(vertex.edges(Direction.IN, SIMILAR));
  }

  public double getNodeOutDegree(Vertex vertex) {
    return (double) Iterators.size(vertex.edges(Direction.OUT, SIMILAR));
  }

  @Override
  public void initCommunityProperty() {
    int communityCounter = 0;
    LOG.info("init community property for janusGraph, vertex size is {}", getNodeCount());
    final GraphTraversalSource g = graph.traversal();
    for (int id = 1; id <= getNodeCount(); id++) {
      long vertexId = ((StandardJanusGraph)graph).getIDManager().toVertexId(id);    	
      Vertex v = g.V(vertexId).next();
      v.property(NODE_COMMUNITY, communityCounter);
      v.property(COMMUNITY, communityCounter);
      communityCounter++;
    }
    graph.tx().commit();
  }

  @Override
  public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
    Set<Integer> communities = new HashSet<Integer>();
    final GraphTraversalSource g = graph.traversal();

    for (Property<?> p : g.V().has(NODE_COMMUNITY, nodeCommunities)
        .out(SIMILAR).properties(COMMUNITY).toSet()) {
      communities.add((Integer) p.value());
    }
    return communities;
  }

  @Override
  public Set<Integer> getNodesFromCommunity(int community) {
    final GraphTraversalSource g = graph.traversal();
    Set<Integer> nodes = new HashSet<Integer>();
    for (Vertex v : g.V().has(COMMUNITY, community).toList()) {
      Integer nodeId = (Integer) v.property(NODE_ID).value();
      nodes.add(nodeId);
    }
    return nodes;
  }

  @Override
  public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
    Set<Integer> nodes = new HashSet<Integer>();
    final GraphTraversalSource g = graph.traversal();
    for (Property<?> property : g.V().has(NODE_COMMUNITY, nodeCommunity).properties(NODE_ID).toList()) {
      nodes.add((Integer) property.value());
    }
    return nodes;
  }

  @Override
  public double getCommunityWeight(int community) {
    double communityWeight = 0;
    final List<Vertex> list = graph.traversal().V().has(COMMUNITY, community).toList();
    if (list.size() <= 1) {
      return communityWeight;
    }
    for (Vertex vertex : list) {
      communityWeight += getNodeOutDegree(vertex);
    }
    return communityWeight;
  }

  @Override
  public void moveNode(int nodeCommunity, int toCommunity) {
    for (Vertex vertex : graph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).toList()) {
      vertex.property(COMMUNITY, toCommunity);
    }
  }

  @Override
  public double getGraphWeightSum() {
    return config.getEdgeTotal();
  }

  @Override
  public int reInitializeCommunities() {
    Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
    int communityCounter = 0;
    final GraphTraversalSource g = graph.traversal();
    for (int id = 1; id <= getNodeCount(); id++) {
      long vertexId = ((StandardJanusGraph)graph).getIDManager().toVertexId(id);    	
      Vertex v = g.V(vertexId).next();
      int communityId = (Integer) v.property(COMMUNITY).value();
      if (!initCommunities.containsKey(communityId)) {
        initCommunities.put(communityId, communityCounter);
        communityCounter++;
      }
      int newCommunityId = initCommunities.get(communityId);
      v.property(COMMUNITY, newCommunityId);
      v.property(NODE_COMMUNITY, newCommunityId);
    }
    g.tx().commit();
    LOG.info("reinit community property for janusGraph, vertex size is {}, new community Counter is {}",
        getNodeCount(), communityCounter);
    return communityCounter;
  }

  @Override
  public int getCommunityFromNode(int nodeId) {
    Vertex vertex = getVertex(nodeId);
    return (Integer) vertex.property(COMMUNITY).value();
  }

  @Override
  public int getCommunity(int nodeCommunity) {
    Vertex vertex = graph.traversal().V().has(NODE_COMMUNITY, nodeCommunity).next();
    int community = (Integer) vertex.property(COMMUNITY).value();
    return community;
  }

  @Override
  public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
    Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
    for (int i = 0; i < numberOfCommunities; i++) {
      GraphTraversal<Vertex, Vertex> t = graph.traversal().V().has(COMMUNITY, i);
      List<Integer> vertices = new ArrayList<Integer>();
      while (t.hasNext()) {
        Integer nodeId = (Integer) t.next().property(NODE_ID).value();
        vertices.add(nodeId);
      }
      communities.put(i, vertices);
    }
    return communities;
  }
}
