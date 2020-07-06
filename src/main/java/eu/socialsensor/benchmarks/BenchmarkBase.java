package eu.socialsensor.benchmarks;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.base.Stopwatch;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Base class abstracting the logic of permutations
 */
public abstract class BenchmarkBase implements Benchmark {
  private static final Logger LOG = LogManager.getLogger();
  protected final BenchmarkConfiguration config;
  protected final File outputFile;
  protected final BenchmarkType type;

  public BenchmarkBase(BenchmarkConfiguration config, BenchmarkType type) {
    this.config = config;
    this.outputFile = new File(config.getResultsPath(), type.getResultsFileName());
    this.type = type;
  }

  @Override
  public void startBenchmark() {
    Map<GraphDatabaseType, List<Double>> times  = new HashMap<GraphDatabaseType, List<Double>>();
    for (GraphDatabaseType type : config.getSelectedDatabases()) {
      times.put(type, new ArrayList<Double>());
      GraphDatabase graphDatabase = Utils.createDatabaseInstance(config, type);
      Stopwatch watch = Stopwatch.createStarted();
      benchmarkWithDB(graphDatabase);
      graphDatabase.shutdown();
      times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
    LOG.info(String.format("%s Benchmark Finished", type.longname()));
    Utils.writeResults(outputFile, times, type.longname());
  }

  public abstract void benchmarkWithDB(GraphDatabase db);
	  
}
