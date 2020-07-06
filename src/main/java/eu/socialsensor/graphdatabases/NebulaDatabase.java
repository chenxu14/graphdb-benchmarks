package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.thrift.TException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.vesoft.nebula.client.graph.GraphClient;
import com.vesoft.nebula.client.graph.GraphClientImpl;
import com.vesoft.nebula.client.graph.ResultSet;
import com.vesoft.nebula.graph.ErrorCode;
import com.vesoft.nebula.graph.RowValue;
import eu.socialsensor.insert.NebulaMassiveInsertion;
import eu.socialsensor.insert.NebulaSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

public class NebulaDatabase implements GraphDatabase {
  private static final Logger LOG = LogManager.getLogger();
  private final GraphClient client;
  private BenchmarkConfiguration config;
  private final GraphDatabaseType type;
  private final File dbStorageDirectory;

  public NebulaDatabase(GraphDatabaseType type, BenchmarkConfiguration config, File dbStorageDirectory) {
    this.config = config;
    this.type = type;
    this.dbStorageDirectory = dbStorageDirectory;
    // client = new GraphClientImpl(config.getNebulaHost(), config.getNebulaPort());
    client = new GraphClientImpl(Lists.newArrayList(
        HostAndPort.fromParts(config.getNebulaHost(), config.getNebulaPort())), 10000, 3, 3);
    client.setUser(config.getNebulaUser());
    client.setPassword(config.getNebulaPwd());
    try {
      client.connect();
    } catch (TException e) {
      LOG.fatal(e.getMessage(), e);
      System.exit(-1);
    }
    int code = client.switchSpace(config.getNebulaSpace());
    if (ErrorCode.SUCCEEDED != code) {
      LOG.error(String.format("Switch Space %s Failed", config.getNebulaSpace()));
      LOG.error(String.format("Please confirm %s have been created", config.getNebulaSpace()));
      System.exit(-1);
    }
  }

  public void khopQuery() {
    String step = config.getKpopStep() > 1 ? config.getKpopStep() + " STEPS " : "";
    for (Integer vertexId : config.getRandomNodes()) {
      StringBuilder query = new StringBuilder("GO ").append(step).append("FROM ").append(vertexId).append(" OVER similar");
      if (config.withProps()) {
        query.append(" YIELD DISTINCT $$.node.nodeId as nodeId;");
      }
      try {
        ResultSet resSet = this.client.executeQuery(query.toString());
        LOG.trace("query is : {} result size is {}.", query.toString(),
            resSet.getRows() == null ? 0 : resSet.getRows().size());
      } catch (Exception e) {
        throw new BenchmarkingException("khopQuery error with vertex " + vertexId, e);
      }
    }
  }

  @Override
  public void massiveModeLoading() {
    NebulaMassiveInsertion massiveInsertion = new NebulaMassiveInsertion(this.client, config.getBatchSize());
    massiveInsertion.createGraph(config.getDataset());
  }

  @Override
  public void singleModeLoading() {
    NebulaSingleInsertion singleInsertion = new NebulaSingleInsertion(this.client, type);
    singleInsertion.createGraph(config.getDataset());
  }

  @Override
  public void shutdown() {
    try {
      client.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void delete() {
    client.execute("DROP SPACE IF EXISTS nebula-benchmark");
    Utils.deleteRecursively(dbStorageDirectory);
  }

  @Override
  public int getNodeCount() {
    return this.config.getVertexTotal();
  }

  @Override
  public Set<Integer> getNeighborsIds(int nodeId) {
    StringBuilder query = new StringBuilder("GO FROM ").append(nodeId)
        .append(" OVER similar YIELD similar._dst AS id");
    Set<Integer> res = new HashSet<>();
    try {
      ResultSet resSet = client.executeQuery(query.toString());
      if (resSet.getRows() != null) {
        for (RowValue value : resSet.getRows()) {
          res.add((int)value.columns.get(0).getId());
        }
      }
    } catch (Exception e) {
      throw new BenchmarkingException("getNeighborsIds error with vertex " + nodeId, e);
    }
    LOG.trace("query is : {}, result size is {}", query.toString(), res.size());
    return res;
  }

  @Override
  public void initCommunityProperty() {
    int communityCounter = 0;
    LOG.info("init community property for NebulaGraph, vertex size is {}", getNodeCount());
    StringBuilder updateGql;
    for (int id = 1; id <= getNodeCount(); id++) {
      updateGql = new StringBuilder("UPDATE VERTEX ").append(id)
          .append(" SET node.nodeCommunity = ").append(communityCounter)
          .append(", node.community = ").append(communityCounter);
      client.execute(updateGql.toString());
      LOG.trace(updateGql.toString());
      communityCounter++;
    }
  }

  @Override
  public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
    Set<Integer> communities = new HashSet<Integer>();
    StringBuilder query = new StringBuilder("LOOKUP ON node WHERE node.nodeCommunity == ").append(nodeCommunities)
        .append(" | GO FROM $-.VertexID OVER similar YIELD $$.node.community as community");
    ResultSet resSet = null;
	try {
      resSet = client.executeQuery(query.toString());
	} catch (Exception e) {
      throw new BenchmarkingException("getCommunitiesConnectedToNodeCommunities error with target nodeCommunity " + nodeCommunities, e);
	}
    if (resSet.getRows() != null) {
      for (RowValue value : resSet.getRows()) {
        communities.add((int)value.columns.get(0).getInteger());
      }
    }
    LOG.trace("query is {}, result size is {}", query.toString(), communities.size());
    return communities;
  }

  @Override
  public Set<Integer> getNodesFromCommunity(int community) {
    Set<Integer> nodes = new HashSet<Integer>();
    StringBuilder query = new StringBuilder("LOOKUP ON node WHERE node.community == ").append(community);
    ResultSet resSet = null;
    try {
      resSet = client.executeQuery(query.toString());
    } catch (Exception e) {
      throw new BenchmarkingException("getNodesFromCommunity error with target community " + community, e);
    }
    if (resSet.getRows() != null) {
      for (RowValue value : resSet.getRows()) {
        nodes.add((int)value.columns.get(0).getInteger());
      }
    }
    LOG.trace("query is {}, result size is {}", query.toString(), nodes.size());
    return nodes;
  }

  @Override
  public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
    Set<Integer> nodes = new HashSet<Integer>();
    StringBuilder query = new StringBuilder("LOOKUP ON node WHERE node.nodeCommunity == ").append(nodeCommunity);
    ResultSet resSet = null;
    try {
      resSet = client.executeQuery(query.toString());
    } catch (Exception e) {
      throw new BenchmarkingException("getNodesFromNodeCommunity error with target nodeCommunity " + nodeCommunity, e);
    }
    if (resSet.getRows() != null) {
      for (RowValue value : resSet.getRows()) {
        nodes.add((int)value.columns.get(0).getInteger());
      }
    }
    LOG.trace("query is {}, result size is {}", query.toString(), nodes.size());
    return nodes;
  }

  @Override
  public double getCommunityWeight(int community) {
    double communityWeight = 0;
    StringBuilder query = new StringBuilder("LOOKUP ON node WHERE node.community == ").append(community)
        .append(" | GO FROM $-.VertexID OVER similar YIELD similar._dst AS id");
    ResultSet resSet = null;
    try {
      resSet = client.executeQuery(query.toString());
    } catch (Exception e) {
      throw new BenchmarkingException("getCommunityWeight error with target community " + community, e);
    }
    if (resSet.getRows() != null) {
      communityWeight = resSet.getRows().size();
    }
    LOG.trace("query is {}, communityWeight is {}", query.toString(), communityWeight);
    return communityWeight;
  }

  @Override
  public void moveNode(int nodeCommunity, int toCommunity) {
    StringBuilder updateGql;
    Set<Integer> ids = getNodesFromNodeCommunity(nodeCommunity);
    for(int id : ids) {
      updateGql = new StringBuilder("UPDATE VERTEX ").append(id)
          .append(" SET node.community = ").append(toCommunity);
      this.client.execute(updateGql.toString());
    }
    LOG.debug("move {} node's Community from {} to {}", ids.size(), nodeCommunity, toCommunity);
  }

  @Override
  public double getGraphWeightSum() {
    return config.getEdgeTotal();
  }

  @Override
  public int reInitializeCommunities() {
    Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
    int communityCounter = 0;
    StringBuilder query;
    for (int id = 1; id <= getNodeCount(); id++) {
      int communityId = getCommunityFromNode(id);
      if (!initCommunities.containsKey(communityId)) {
        initCommunities.put(communityId, communityCounter);
        communityCounter++;
      }
      int newCommunityId = initCommunities.get(communityId);
      try {
        query = new StringBuilder("UPDATE VERTEX ").append(id)
            .append(" SET node.nodeCommunity = ").append(newCommunityId)
            .append(", node.community = ").append(newCommunityId);
        client.execute(query.toString());
      } catch (Exception e) {
    	throw new BenchmarkingException("reInitializeCommunities error with vertex " + id, e);  
      }
	}
    LOG.info("reinit community property for NebulaGraph, vertex size is {}, new community Counter is {}",
        getNodeCount(), communityCounter);
    return communityCounter;
  }

  @Override
  public int getCommunityFromNode(int nodeId) {
    StringBuilder query = new StringBuilder("FETCH PROP ON node ").append(nodeId).append(" YIELD node.community");
    int communityId = -1;
	try {
      communityId = (int)client.executeQuery(query.toString()).getRows().get(0).getColumns().get(0).getInteger();
	} catch (Exception e) {
      throw new BenchmarkingException("getCommunityFromNode error with vertex " + nodeId, e);
	}
	return communityId;
  }

  @Override
  public int getCommunity(int nodeCommunity) {
    StringBuilder query = new StringBuilder("LOOKUP ON node WHERE node.nodeCommunity == ")
        .append(nodeCommunity).append("YIELD node.community");
    int communityId = -1;
    try {
      communityId = (int) client.executeQuery(query.toString()).getRows().get(0).getColumns().get(0).getInteger();
	} catch (Exception e) {
      throw new BenchmarkingException("getCommunity error with nodeCommunity " + nodeCommunity, e);
	}
    return communityId;
  }

  @Override
  public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
    Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
    String lookup = "LOOKUP ON node WHERE node.community == ";
    for (int i = 0; i < numberOfCommunities; i++) {
      List<Integer> vertices = new ArrayList<Integer>();
      ResultSet resSet = null;
      try {
        resSet = client.executeQuery(lookup + i);
      } catch (Exception e) {
        throw new BenchmarkingException("mapCommunities error with target community " + i, e);
      }
      if (resSet.getRows() != null) {
        for (RowValue row : resSet.getRows()) {
          Integer nodeId = (int)row.columns.get(0).getInteger();
          vertices.add(nodeId);
        }
        communities.put(i, vertices);
      }
    }
    return communities;
  }

  @Override
  public void shortestPaths() {
    Iterator<Integer> it = config.getRandomNodes().iterator();
    Preconditions.checkArgument(it.hasNext());
    int from = it.next();
    while (it.hasNext()) {
      int to = it.next();
      StringBuilder query = new StringBuilder("FIND SHORTEST PATH FROM ").append(from).append(" TO ").append(to)
          .append(" OVER similar UPTO ").append(config.getShortestPathMaxHops()).append(" STEPS");
      try {
        ResultSet resSet = this.client.executeQuery(query.toString());
        LOG.trace("query is : {} result size is {}.", query.toString(),
            resSet.getRows() == null ? 0 : resSet.getRows().size());
      } catch (Exception e) {
        throw new BenchmarkingException("shortestPaths error with from " + from + "to " + to, e);
      }
    }
  }
}
