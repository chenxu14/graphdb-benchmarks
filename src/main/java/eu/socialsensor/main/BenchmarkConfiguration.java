package eu.socialsensor.main;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Benchmark Configurations
 */
public class BenchmarkConfiguration {
  private static final Logger LOG = LogManager.getLogger();
  // Janus specific configuration
  private static final String JANUS = "janus";
  private static final String IDS_BLOCKSIZE = GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getName();
  private static final String PAGE_SIZE = GraphDatabaseConfiguration.PAGE_SIZE.getName();
  public static final String GRAPHITE = GraphDatabaseConfiguration.METRICS_GRAPHITE_NS.getName();
  private static final String BATCH = GraphDatabaseConfiguration.STORAGE_BATCH.getName();
  private final int blocksize;
  private final int pageSize;
  private final boolean batch;

  // nebula specific configuration
  private final String nebulaHost;
  private final int nebulaPort;
  private final String nebulaUser;
  private final String nebulaPwd;
  private final String nebulaSpace;
  
  // benchmark configuration
  private static final String DATASET = "dataset";
  private static final String DATABASE_STORAGE_DIRECTORY = "database-storage-directory";
  private static final String RANDOM_NODES_INTERVAL = "random-nodes-interval";
  private static final String RANDOM_SEED = "random-seed";
  private static final String MAX_HOPS = "max-hops";
  private final File dataset;
  private final List<BenchmarkType> benchmarkTypes;
  private final SortedSet<GraphDatabaseType> selectedDatabases;
  private final File resultsPath;
  private final int numRandomNodesInterval;
  private final int batchSize;
  private final File dbStorageDirectory;
  private int shortestPathMaxHops;
  private final Random random;
  private final Set<Integer> randomNodes;
  private int vertexTotal;
  private int edgeTotal;
  private int kpopStep = 1;
  private boolean withProps = true;

  // clustering
  private static final String ACTUAL_COMMUNITIES = "actual-communities";
  private static final String RANDOMIZE_CLUSTERING = "randomize";
  private static final String CACHE_PERCENTAGES = "cache-percentages";
  private final Boolean randomizedClustering;
  private final List<Integer> cachePercentages;
  private final File actualCommunities;

  public BenchmarkConfiguration(Configuration appconfig) {
    if (appconfig == null) {
      throw new IllegalArgumentException("appconfig may not be null");
    }

    Configuration graph = appconfig.subset("graph");
    Configuration benchmark = graph.subset("benchmark");

    dbStorageDirectory = new File(benchmark.getString(DATABASE_STORAGE_DIRECTORY, "storage"));

    // JaunsGraph conf
    Configuration janus = benchmark.subset(JANUS);
    blocksize = janus.getInt(IDS_BLOCKSIZE, GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getDefaultValue());
    pageSize = janus.getInt(PAGE_SIZE, GraphDatabaseConfiguration.PAGE_SIZE.getDefaultValue());
    batch = janus.getBoolean(BATCH, GraphDatabaseConfiguration.STORAGE_BATCH.getDefaultValue());

    // nebula conf
    Configuration nebula = benchmark.subset("nebula");
    nebulaHost = nebula.getString("host");
    nebulaPort = nebula.getInt("port", 3699);
    nebulaUser = nebula.getString("user", "root");
    nebulaPwd = nebula.getString("password", "nebula");
    nebulaSpace = nebula.getString("space");

    // load the dataset
    random = new Random(benchmark.getInt(RANDOM_SEED, 17));
    numRandomNodesInterval = benchmark.getInteger(RANDOM_NODES_INTERVAL, 1000);
    batchSize = benchmark.getInt("batchSize", 100);
    String fileName = benchmark.getString(DATASET);
    dataset = validateReadableFile(fileName, DATASET);
    randomNodes = new HashSet<>();
    generateRandomNodes();

    List<?> benchmarkList = benchmark.getList("testcase");
    benchmarkTypes = new ArrayList<BenchmarkType>();
    for (Object str : benchmarkList) {
      benchmarkTypes.add(BenchmarkType.valueOf(str.toString()));
    }

    selectedDatabases = new TreeSet<GraphDatabaseType>();
    for (Object database : benchmark.getList("databases")) {
      if (!GraphDatabaseType.STRING_REP_MAP.keySet().contains(database.toString())) {
        throw new IllegalArgumentException(
            String.format("selected database %s not supported", database.toString()));
      }
      selectedDatabases.add(GraphDatabaseType.STRING_REP_MAP.get(database));
    }

    resultsPath = new File(System.getProperty("user.dir"), benchmark.getString("results-path", "results"));
    if (!resultsPath.exists() && !resultsPath.mkdirs()) {
      throw new IllegalArgumentException("unable to create results directory");
    }
    if (!resultsPath.canWrite()) {
      throw new IllegalArgumentException("unable to write to results directory");
    }

    if (this.benchmarkTypes.contains(BenchmarkType.KHOP)) {
      Configuration khop = benchmark.subset("khop");
      this.kpopStep = khop.getInt("step");
      this.withProps = khop.getBoolean("with-props");
    }

    if (this.benchmarkTypes.contains(BenchmarkType.FIND_SHORTEST_PATH)) {
      Configuration shortestpath = benchmark.subset("shortestpath");
      shortestPathMaxHops = shortestpath.getInteger(MAX_HOPS, 5);	
    }

    if (this.benchmarkTypes.contains(BenchmarkType.CLUSTERING)) {
      Configuration clustering = benchmark.subset("clustering");
      if (!clustering.containsKey(RANDOMIZE_CLUSTERING)) {
        throw new IllegalArgumentException("the CW benchmark requires randomize-clustering bool in config");
      }
      randomizedClustering = clustering.getBoolean(RANDOMIZE_CLUSTERING);

      if (!clustering.containsKey(ACTUAL_COMMUNITIES)) {
        throw new IllegalArgumentException("the CW benchmark requires a file with actual communities");
      }
      actualCommunities = new File(clustering.getString(ACTUAL_COMMUNITIES));

      final boolean notGenerating = clustering.containsKey(CACHE_PERCENTAGES);
      if (notGenerating) {
        List<?> objects = clustering.getList(CACHE_PERCENTAGES);
        cachePercentages = new ArrayList<Integer>(objects.size());
        for (Object o : objects) {
          cachePercentages.add(Integer.valueOf(o.toString()));
        }
      } else {
        throw new IllegalArgumentException("when doing CW benchmark, must provide cache-percentages");
      }
    } else {
      randomizedClustering = null;
      cachePercentages = null;
      actualCommunities = null;
    }
  }

  private File validateReadableFile(String fileName, String fileType) {
    if (fileName == null) {
      throw new IllegalArgumentException("configuration must specify dataset");
    }
    String[] graphInfo = fileName.substring(fileName.lastIndexOf(".") + 1).split("_");
    if (graphInfo.length != 2) {
       throw new IllegalArgumentException("dataset file must suffix with .vertexNum_edgeNum");	
    }
    vertexTotal = Integer.parseInt(graphInfo[0]);
    edgeTotal = Integer.parseInt(graphInfo[1]);
    LOG.info("target dataset with {} vertex and {} edges.", vertexTotal, edgeTotal);
    File file = new File(fileName);
    if (!file.exists()) {
      throw new IllegalArgumentException(String.format("the %s does not exist", fileType));
    }
    if (!(file.isFile() && file.canRead())) {
      throw new IllegalArgumentException(
          String.format("the %s must be a file that this user can read", fileType));
    }
    return file;
  }

  private void generateRandomNodes() {
    for (int i = 1; i <= vertexTotal; i += numRandomNodesInterval) {
      randomNodes.add(i);
    }
    LOG.info("generate {} random nodes from {}.", randomNodes.size(), vertexTotal);
  }

  public File getDataset() {
    return dataset;
  }

  public SortedSet<GraphDatabaseType> getSelectedDatabases() {
    return selectedDatabases;
  }

  public File getDbStorageDirectory() {
    return dbStorageDirectory;
  }

  public File getResultsPath() {
    return resultsPath;
  }

  public List<BenchmarkType> getBenchmarkTypes() {
    return benchmarkTypes;
  }

  public Boolean randomizedClustering() {
    return randomizedClustering;
  }

  public List<Integer> getCachePercentages() {
    return cachePercentages;
  }

  public File getActualCommunitiesFile() {
    return actualCommunities;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getJanusIdsBlocksize() {
    return blocksize;
  }

  public int getJanusPageSize() {
    return pageSize;
  }

  public boolean isBatch() {
    return this.batch;
  }

  public Random getRandom() {
    return random;
  }

  public int getShortestPathMaxHops() {
    return shortestPathMaxHops;
  }

  public Set<Integer> getRandomNodes() {
    return randomNodes;
  }

  public int getVertexTotal() {
    return this.vertexTotal;
  }

  public int getEdgeTotal() {
    return this.edgeTotal;
  }

  public int getKpopStep() {
    return this.kpopStep;
  }

  public boolean withProps() {
    return this.withProps;
  }

  public String getNebulaHost() {
    return this.nebulaHost;
  }

  public int getNebulaPort() {
    return this.nebulaPort;
  }

  public String getNebulaUser() {
    return this.nebulaUser;
  }

  public String getNebulaPwd() {
    return this.nebulaPwd;
  }

  public String getNebulaSpace() {
    return this.nebulaSpace;
  }
}
