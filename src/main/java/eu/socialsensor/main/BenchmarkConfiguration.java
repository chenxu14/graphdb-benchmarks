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
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.google.common.primitives.Ints;
import eu.socialsensor.dataset.DatasetFactory;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class BenchmarkConfiguration
{
    // Janus specific configuration
    private static final String JANUS = "janus";
    private static final String BUFFER_SIZE = GraphDatabaseConfiguration.BUFFER_SIZE.getName();
    private static final String IDS_BLOCKSIZE = GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getName();
    private static final String PAGE_SIZE = GraphDatabaseConfiguration.PAGE_SIZE.getName();
    public static final String CSV_INTERVAL = GraphDatabaseConfiguration.METRICS_CSV_INTERVAL.getName();
    public static final String CSV = GraphDatabaseConfiguration.METRICS_CSV_NS.getName();
    private static final String CSV_DIR = GraphDatabaseConfiguration.METRICS_CSV_DIR.getName();
    public static final String GRAPHITE = GraphDatabaseConfiguration.METRICS_GRAPHITE_NS.getName();
    private static final String GRAPHITE_HOSTNAME = GraphDatabaseConfiguration.GRAPHITE_HOST.getName();
    private static final String CUSTOM_IDS = "custom-ids";

    // benchmark configuration
    private static final String DATASET = "dataset";
    private static final String DATABASE_STORAGE_DIRECTORY = "database-storage-directory";
    private static final String ACTUAL_COMMUNITIES = "actual-communities";
    private static final String RANDOMIZE_CLUSTERING = "randomize-clustering";
    private static final String CACHE_PERCENTAGES = "cache-percentages";
    private static final String PERMUTE_BENCHMARKS = "permute-benchmarks";
    private static final String RANDOM_NODES = "shortest-path-random-nodes";
    private static final String RANDOM_SEED = "random-seed";
    private static final String MAX_HOPS = "shortest-path-max-hops";
    
    private static final Set<String> metricsReporters = new HashSet<String>();
    static {
        metricsReporters.add(CSV);
        metricsReporters.add(GRAPHITE);
    }

    private final File dataset;
    private final List<BenchmarkType> benchmarkTypes;
    private final SortedSet<GraphDatabaseType> selectedDatabases;
    private final File resultsPath;

    // storage directory
    private final File dbStorageDirectory;

    // metrics (optional)
    private final long csvReportingInterval;
    private final File csvDir;
    private final String graphiteHostname;
    private final long graphiteReportingInterval;

    // shortest path
    private final int numShortestPathRandomNodes;

    // clustering
    private final Boolean randomizedClustering;
    private final List<Integer> cachePercentages;
    private final File actualCommunities;
    private final boolean permuteBenchmarks;
    private final int scenarios;
    private final int bufferSize;
    private final int blocksize;
    private final int pageSize;
    private final boolean customIds;
    private final int shortestPathMaxHops;
    private final Random random;

    public BenchmarkConfiguration(Configuration appconfig)
    {
        if (appconfig == null)
        {
            throw new IllegalArgumentException("appconfig may not be null");
        }

        Configuration eu = appconfig.subset("eu");
        Configuration socialsensor = eu.subset("socialsensor");
        
        //metrics
        final Configuration metrics = socialsensor.subset(GraphDatabaseConfiguration.METRICS_NS.getName());

        final Configuration graphite = metrics.subset(GRAPHITE);
        this.graphiteHostname = graphite.getString(GRAPHITE_HOSTNAME, null);
        this.graphiteReportingInterval = graphite.getLong(GraphDatabaseConfiguration.GRAPHITE_INTERVAL.getName(), 1000 /*default 1sec*/);

        final Configuration csv = metrics.subset(CSV);
        this.csvReportingInterval = metrics.getLong(CSV_INTERVAL, 1000 /*ms*/);
        this.csvDir = csv.containsKey(CSV_DIR) ? new File(csv.getString(CSV_DIR, System.getProperty("user.dir") /*default*/)) : null;

        // JaunsGraph conf
        Configuration janus = socialsensor.subset(JANUS);
        bufferSize = janus.getInt(BUFFER_SIZE, GraphDatabaseConfiguration.BUFFER_SIZE.getDefaultValue());
        blocksize = janus.getInt(IDS_BLOCKSIZE, GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getDefaultValue());
        pageSize = janus.getInt(PAGE_SIZE, GraphDatabaseConfiguration.PAGE_SIZE.getDefaultValue());
        customIds = janus.getBoolean(CUSTOM_IDS, false /*default*/);

        // database storage directory
        if (!socialsensor.containsKey(DATABASE_STORAGE_DIRECTORY))
        {
            throw new IllegalArgumentException("configuration must specify database-storage-directory");
        }
        dbStorageDirectory = new File(socialsensor.getString(DATABASE_STORAGE_DIRECTORY));
        dataset = validateReadableFile(socialsensor.getString(DATASET), DATASET);

        // load the dataset
        random = new Random(socialsensor.getInt(RANDOM_SEED, 17 /*default*/));
        numShortestPathRandomNodes = socialsensor.getInteger(RANDOM_NODES, new Integer(101));
        shortestPathMaxHops = socialsensor.getInteger(MAX_HOPS, 5);
        DatasetFactory.getInstance().createAndGetDataset(dataset, random, numShortestPathRandomNodes);

        if (!socialsensor.containsKey(PERMUTE_BENCHMARKS))
        {
            throw new IllegalArgumentException("configuration must set permute-benchmarks to true or false");
        }
        permuteBenchmarks = socialsensor.getBoolean(PERMUTE_BENCHMARKS);

        List<?> benchmarkList = socialsensor.getList("benchmarks");
        benchmarkTypes = new ArrayList<BenchmarkType>();
        for (Object str : benchmarkList)
        {
            benchmarkTypes.add(BenchmarkType.valueOf(str.toString()));
        }

        selectedDatabases = new TreeSet<GraphDatabaseType>();
        for (Object database : socialsensor.getList("databases"))
        {
            if (!GraphDatabaseType.STRING_REP_MAP.keySet().contains(database.toString()))
            {
                throw new IllegalArgumentException(String.format("selected database %s not supported",
                    database.toString()));
            }
            selectedDatabases.add(GraphDatabaseType.STRING_REP_MAP.get(database));
        }
        scenarios = permuteBenchmarks ? Ints.checkedCast(CombinatoricsUtils.factorial(selectedDatabases.size())) : 1;

        resultsPath = new File(System.getProperty("user.dir"), socialsensor.getString("results-path"));
        if (!resultsPath.exists() && !resultsPath.mkdirs())
        {
            throw new IllegalArgumentException("unable to create results directory");
        }
        if (!resultsPath.canWrite())
        {
            throw new IllegalArgumentException("unable to write to results directory");
        }

        if (this.benchmarkTypes.contains(BenchmarkType.CLUSTERING))
        {
            if (!socialsensor.containsKey(RANDOMIZE_CLUSTERING))
            {
                throw new IllegalArgumentException("the CW benchmark requires randomize-clustering bool in config");
            }
            randomizedClustering = socialsensor.getBoolean(RANDOMIZE_CLUSTERING);

            if (!socialsensor.containsKey(ACTUAL_COMMUNITIES))
            {
                throw new IllegalArgumentException("the CW benchmark requires a file with actual communities");
            }
            actualCommunities = validateReadableFile(socialsensor.getString(ACTUAL_COMMUNITIES), ACTUAL_COMMUNITIES);

            final boolean notGenerating = socialsensor.containsKey(CACHE_PERCENTAGES);
            if (notGenerating)
            {
                List<?> objects = socialsensor.getList(CACHE_PERCENTAGES);
                cachePercentages = new ArrayList<Integer>(objects.size());
                for (Object o : objects)
                {
                    cachePercentages.add(Integer.valueOf(o.toString()));
                }
            }
            else
            {
                throw new IllegalArgumentException(
                    "when doing CW benchmark, must provide cache-percentages");
            }
        }
        else
        {
            randomizedClustering = null;
            cachePercentages = null;
            actualCommunities = null;
        }
    }

    public File getDataset()
    {
        return dataset;
    }

    public SortedSet<GraphDatabaseType> getSelectedDatabases()
    {
        return selectedDatabases;
    }

    public File getDbStorageDirectory()
    {
        return dbStorageDirectory;
    }

    public File getResultsPath()
    {
        return resultsPath;
    }

    public List<BenchmarkType> getBenchmarkTypes()
    {
        return benchmarkTypes;
    }

    public Boolean randomizedClustering()
    {
        return randomizedClustering;
    }

    public List<Integer> getCachePercentages()
    {
        return cachePercentages;
    }

    public File getActualCommunitiesFile()
    {
        return actualCommunities;
    }

    public boolean permuteBenchmarks()
    {
        return permuteBenchmarks;
    }

    public int getScenarios()
    {
        return scenarios;
    }

    private static final File validateReadableFile(String fileName, String fileType) {
        File file = new File(fileName);
        if (!file.exists()) {
            throw new IllegalArgumentException(String.format("the %s does not exist", fileType));
        }

        if (!(file.isFile() && file.canRead())) {
            throw new IllegalArgumentException(String.format("the %s must be a file that this user can read", fileType));
        }
        return file;
    }

    public long getCsvReportingInterval()
    {
        return csvReportingInterval;
    }

    public long getGraphiteReportingInterval()
    {
        return graphiteReportingInterval;
    }

    public File getCsvDir()
    {
        return csvDir;
    }

    public String getGraphiteHostname()
    {
        return graphiteHostname;
    }

    public int getJanusBufferSize()
    {
        return bufferSize;
    }

    public int getJanusIdsBlocksize()
    {
        return blocksize;
    }

    public int getJanusPageSize()
    {
        return pageSize;
    }

    public boolean publishCsvMetrics()
    {
        return csvDir != null;
    }

    public boolean publishGraphiteMetrics()
    {
        return graphiteHostname != null && !graphiteHostname.isEmpty();
    }

    public boolean isCustomIds() {
        return customIds;
    }

    public Random getRandom() {
        return random;
    }
    public List<Integer> getRandomNodeList() {
        return DatasetFactory.getInstance().getDataset(this.dataset).getRandomNodes();
    }

    public int getShortestPathMaxHops() {
        return shortestPathMaxHops;
    }
}
