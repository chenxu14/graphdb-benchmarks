package eu.socialsensor.graphdatabases;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tinkerpop.api.impl.Neo4jGraphAPIImpl;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.google.common.collect.Lists;

import eu.socialsensor.insert.Neo4jMassiveInsertion;
import eu.socialsensor.insert.Neo4jSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Neo4j graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class Neo4jGraphDatabase extends GraphDatabaseBase<Iterator<Node>, Iterator<Relationship>, Node, Relationship> {
  private final GraphDatabaseService neo4jGraph;
  private final Neo4jGraph neo4jTp;
  private final Schema schema;
  private final BatchInserter inserter;

  public enum RelTypes implements RelationshipType {
    SIMILAR
  }

  public static Label NODE_LABEL = DynamicLabel.label("Node");

  public Neo4jGraphDatabase(File dbStorageDirectoryIn, BenchmarkConfiguration benchConf) {
    super(GraphDatabaseType.NEO4J, dbStorageDirectoryIn, benchConf);
    if (benchConf.isBatch()) {
      neo4jGraph = null;
      neo4jTp = null;
      schema = null;

      Map<String, String> config = new HashMap<String, String>();
      config.put("cache_type", "none");
      config.put("use_memory_mapped_buffers", "true");
      config.put("neostore.nodestore.db.mapped_memory", "200M");
      config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
      config.put("neostore.propertystore.db.mapped_memory", "250M");
      config.put("neostore.propertystore.db.strings.mapped_memory", "250M");

      try {
        // the BatchInserters are deprecated and will become private in a future
        // release.
        inserter = BatchInserters.inserter(dbStorageDirectory, config);
      } catch (IOException e) {
        throw new IllegalStateException("unable to create batch inserter in dir " + dbStorageDirectory);
      }
      inserter.createDeferredSchemaIndex(NODE_LABEL).on(NODE_ID).create();
      inserter.createDeferredSchemaIndex(NODE_LABEL).on(COMMUNITY).create();
      inserter.createDeferredSchemaIndex(NODE_LABEL).on(NODE_COMMUNITY).create();
    } else {
      inserter = null;
      neo4jTp = Neo4jGraph.open(dbStorageDirectory.getAbsolutePath());
      neo4jGraph = ((Neo4jGraphAPIImpl) neo4jTp.getBaseGraph()).getGraphDatabase();
      try (final Transaction tx = neo4jGraph.beginTx()) {
        schema = neo4jGraph.schema();
        if (!schemaHasIndexOnVertexLabelProperty(NODE_LABEL.name(), NODE_ID)) {
          schema.indexFor(NODE_LABEL).on(NODE_ID).create();
        }
        if (!schemaHasIndexOnVertexLabelProperty(NODE_LABEL.name(), COMMUNITY)) {
          schema.indexFor(NODE_LABEL).on(COMMUNITY).create();
        }
        if (!schemaHasIndexOnVertexLabelProperty(NODE_LABEL.name(), NODE_COMMUNITY)) {
          schema.indexFor(NODE_LABEL).on(NODE_COMMUNITY).create();
        }
        schema.awaitIndexesOnline(10l, TimeUnit.MINUTES);
        tx.success();
      }
    }
  }

  private boolean schemaHasIndexOnVertexLabelProperty(String label, String propertyName) {
    final List<String> targetPropertyList = Lists.newArrayList(propertyName);
    for (IndexDefinition def : schema.getIndexes()) {
      if (!def.getLabel().name().equals(label)) {
        continue;
      }
      // the label is the same here
      final List<String> definitionProps = Lists.newArrayList(def.getPropertyKeys());
      if (definitionProps.equals(targetPropertyList)) {
        return true;
      } else {
        continue; // keep looking
      }
    }
    return false;
  }

  @Override
  public void singleModeLoading() {
    Neo4jSingleInsertion neo4jSingleInsertion = new Neo4jSingleInsertion(this.neo4jGraph);
    neo4jSingleInsertion.createGraph(config.getDataset());
  }

  @Override
  public void massiveModeLoading() {
    Neo4jMassiveInsertion neo4jMassiveInsertion = new Neo4jMassiveInsertion(this.inserter);
    neo4jMassiveInsertion.createGraph(config.getDataset());
  }

  @Override
  public void shutdown() {
    if (neo4jGraph == null) {
      return;
    }
    neo4jGraph.shutdown();
  }

  @Override
  public void delete() {
    Utils.deleteRecursively(dbStorageDirectory);
  }

  @Override
  public void shortestPaths() {
    try (Transaction tx = neo4jGraph.beginTx()) {
      try {
        super.shortestPaths();
        tx.success();
      } catch (Exception e) {
        tx.failure();
      }
    }
  }

  public void khopQuery() {
	  
  }

  @Override
  public void shortestPath(Node n1, Integer i) {
    final GraphTraversalSource g = neo4jTp.traversal();
    int maxHops = this.config.getShortestPathMaxHops();
    final DepthPredicate maxDepth = new DepthPredicate(maxHops);
    final Integer fromNodeId = (Integer) n1.getProperty(NODE_ID);
    final GraphTraversal<?, org.apache.tinkerpop.gremlin.process.traversal.Path> t = g.V().has(NODE_ID, fromNodeId)
        .repeat(__.out(SIMILAR).simplePath()).until(__.has(NODE_ID, i).and(__.filter(maxDepth))).limit(1)
        .path();

    t.tryNext().ifPresent(it -> {
      it.size();
    });
  }

  @Override
  public Set<Integer> getNeighborsIds(int nodeId) {
    Set<Integer> neighbors = new HashSet<Integer>();
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        Node n = neo4jGraph.findNodes(NODE_LABEL, NODE_ID, String.valueOf(nodeId)).next();
        for (Relationship relationship : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING)) {
          Node neighbour = relationship.getOtherNode(n);
          String neighbourId = (String) neighbour.getProperty(NODE_ID);
          neighbors.add(Integer.valueOf(neighbourId));
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get neighbors ids", e);
      }
    }

    return neighbors;
  }

  public double getNodeInDegree(Node node) {
    Iterable<Relationship> rel = node.getRelationships(Direction.OUTGOING, RelTypes.SIMILAR);
    return (double) (IteratorUtil.count(rel));
  }

  public double getNodeOutDegree(Node node) {
    Iterable<Relationship> rel = node.getRelationships(Direction.INCOMING, RelTypes.SIMILAR);
    return (double) (IteratorUtil.count(rel));
  }

  @Override
  public void initCommunityProperty() {
    int communityCounter = 0;

    // maybe commit changes every 1000 transactions?
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        for (Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
          n.setProperty(NODE_COMMUNITY, communityCounter);
          n.setProperty(COMMUNITY, communityCounter);
          communityCounter++;
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to initialize community property", e);
      }
    }
  }

  @Override
  public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
    Set<Integer> communities = new HashSet<Integer>();
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        ResourceIterator<Node> nodes = neo4jGraph.findNodes(Neo4jGraphDatabase.NODE_LABEL, NODE_COMMUNITY,
            nodeCommunities);
        while (nodes.hasNext()) {
          final Node n = nodes.next();
          for (Relationship r : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING)) {
            Node neighbour = r.getOtherNode(n);
            Integer community = (Integer) (neighbour.getProperty(COMMUNITY));
            communities.add(community);
          }
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get communities connected to node communities", e);
      }
    }

    return communities;
  }

  @Override
  public Set<Integer> getNodesFromCommunity(int community) {
    Set<Integer> nodes = new HashSet<Integer>();
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        ResourceIterator<Node> iter = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, community);
        while (iter.hasNext()) {
          final Node n = iter.next();
          String nodeIdString = (String) (n.getProperty(NODE_ID));
          nodes.add(Integer.valueOf(nodeIdString));
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get nodes from community", e);
      }
    }
    return nodes;
  }

  @Override
  public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
    Set<Integer> nodes = new HashSet<Integer>();

    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        ResourceIterator<Node> iter = neo4jGraph.findNodes(NODE_LABEL, NODE_COMMUNITY, nodeCommunity);
        while (iter.hasNext()) {
          final Node n = iter.next();
          String nodeIdString = (String) (n.getProperty(NODE_ID));
          nodes.add(Integer.valueOf(nodeIdString));
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get nodes from node community", e);
      }
    }

    return nodes;
  }

  @Override
  public double getCommunityWeight(int community) {
    double communityWeight = 0;
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        List<Node> nodes = Lists.newArrayList(neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, community));
        if (nodes.size() > 1) {
          for (Node n : nodes) {
            communityWeight += getNodeOutDegree(n);
          }
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get community weight", e);
      }
    }

    return communityWeight;
  }

  @Override
  public void moveNode(int nodeCommunity, int toCommunity) {
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        ResourceIterator<Node> fromIter = neo4jGraph.findNodes(NODE_LABEL, NODE_COMMUNITY, nodeCommunity);
        while (fromIter.hasNext()) {
          final Node node = fromIter.next();
          node.setProperty(COMMUNITY, toCommunity);
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to move node", e);
      }
    }
  }

  @Override
  public double getGraphWeightSum() {
    int edgeCount = 0;

    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        edgeCount = IteratorUtil.count(GlobalGraphOperations.at(neo4jGraph).getAllRelationships());
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get graph weight sum", e);
      }
    }

    return (double) edgeCount;
  }

  @Override
  public int reInitializeCommunities() {
    Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
    int communityCounter = 0;

    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        for (Node n : GlobalGraphOperations.at(neo4jGraph).getAllNodes()) {
          Integer communityId = (Integer) (n.getProperty(COMMUNITY));
          if (!initCommunities.containsKey(communityId)) {
            initCommunities.put(communityId, communityCounter);
            communityCounter++;
          }
          int newCommunityId = initCommunities.get(communityId);
          n.setProperty(COMMUNITY, newCommunityId);
          n.setProperty(NODE_COMMUNITY, newCommunityId);
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to reinitialize communities", e);
      }
    }

    return communityCounter;
  }

  @Override
  public int getCommunity(int nodeCommunity) {
    Integer community = 0;

    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        final Node node = neo4jGraph.findNodes(NODE_LABEL, NODE_COMMUNITY, nodeCommunity).next();
        community = (Integer) (node.getProperty(COMMUNITY));
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get community", e);
      }
    }

    return community;
  }

  @Override
  public int getCommunityFromNode(int nodeId) {
    Integer community = 0;
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        final Node node = neo4jGraph.findNodes(NODE_LABEL, NODE_ID, String.valueOf(nodeId)).next();
        community = (Integer) (node.getProperty(COMMUNITY));
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to get community from node", e);
      }
    }

    return community;
  }

  @Override
  public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
    Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();

    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        for (int i = 0; i < numberOfCommunities; i++) {
          ResourceIterator<Node> nodesIter = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, i);
          List<Integer> nodes = new ArrayList<Integer>();
          while (nodesIter.hasNext()) {
            final Node n = nodesIter.next();
            String nodeIdString = (String) (n.getProperty(NODE_ID));
            nodes.add(Integer.valueOf(nodeIdString));
          }
          communities.put(i, nodes);
        }
        tx.success();
      } catch (Exception e) {
        tx.failure();
        throw new BenchmarkingException("unable to map communities", e);
      }
    }

    return communities;
  }

  @Override
  public Node getVertex(Integer i) {
    Node result = null;
    try (final Transaction tx = neo4jGraph.beginTx()) {
      try {
        result = neo4jGraph.findNodes(Neo4jGraphDatabase.NODE_LABEL, NODE_ID, i).next();
        tx.success();
      } catch (Exception e) {
        tx.failure();
      }
    }
    return result;
  }
}
