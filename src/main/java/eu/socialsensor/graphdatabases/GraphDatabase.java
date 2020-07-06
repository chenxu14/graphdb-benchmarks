package eu.socialsensor.graphdatabases;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a graph database
 */
public interface GraphDatabase {
  public static final String SIMILAR = "similar";
  public static final String QUERY_CONTEXT = ".query.";
  public static final String NODE_ID = "nodeId";
  public static final String NODE_COMMUNITY = "nodeCommunity";
  public static final String COMMUNITY = "community";
  public static final String NODE_LABEL = "node";

  /**
   * K-pop query
   */
  public void khopQuery();

  /**
   * Inserts data in massive mode
   */
  public void massiveModeLoading();

  /**
   * Inserts data in single mode
   */
  public void singleModeLoading();

  /**
   * Shut down the graph database
   */
  public void shutdown();

  /**
   * Delete the graph database
   */
  public void delete();

  /**
   * Find the shortest path between vertex 1 and each of the vertexes in the list
   * any number of random nodes
   */
  public void shortestPaths();

  /**
   * @return the number of nodes
   */
  public int getNodeCount();

  /**
   * @param nodeId
   * @return the neighbours of a particular node
   */
  public Set<Integer> getNeighborsIds(int nodeId);

  /**
   * Initializes the community and nodeCommunity property in each database
   */
  public void initCommunityProperty();

  /**
   * @param nodeCommunities
   * @return the communities (communityId) that are connected with a particular
   *         nodeCommunity
   */
  public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities);

  /**
   * @param community
   * @return the nodes a particular community contains
   */
  public Set<Integer> getNodesFromCommunity(int community);

  /**
   * @param nodeCommunity
   * @return the nodes a particular nodeCommunity contains
   */
  public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity);

  /**
   * @param community
   * @return the sum of node degrees
   */
  public double getCommunityWeight(int community);

  /**
   * Moves a node from a community to another
   * 
   * @param from
   * @param to
   */
  public void moveNode(int from, int to);

  /**
   * @return the number of edges of the graph database
   */
  public double getGraphWeightSum();

  /**
   * Reinitializes the community and nodeCommunity property
   * 
   * @return the number of communities
   */
  public int reInitializeCommunities();

  /**
   * @param nodeId
   * @return in which community a particular node belongs
   */
  public int getCommunityFromNode(int nodeId);

  /**
   * @param nodeCommunity
   * @return in which community a particular nodeCommunity belongs
   */
  public int getCommunity(int nodeCommunity);

  /**
   * @param numberOfCommunities
   * @return a map where the key is the community id and the value is the nodes
   *         each community has.
   */
  public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities);

}
