package eu.socialsensor.main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.MetricRegistry;

import eu.socialsensor.benchmarks.Benchmark;
import eu.socialsensor.benchmarks.BenchmarkBase;
import eu.socialsensor.benchmarks.ClusteringBenchmark;
import eu.socialsensor.graphdatabases.GraphDatabase;

/**
 * Main class for the execution of GraphDatabaseBenchmark.
 */
public class GraphDatabaseBenchmark {
  public static final Logger logger = LogManager.getLogger();
  public static final MetricRegistry metrics = new MetricRegistry();
  public static final String DEFAULT_INPUT_PROPERTIES = "META-INF/input.properties";
  private final BenchmarkConfiguration config;

  public static final Configuration getAppconfigFromClasspath() {
    Configuration appconfig;
    try {
      ClassLoader classLoader = GraphDatabaseBenchmark.class.getClassLoader();
      URL resource = classLoader.getResource(DEFAULT_INPUT_PROPERTIES);
      appconfig = new PropertiesConfiguration(resource);
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException(
          String.format("Unable to load properties file from classpath because %s", e.getMessage()));
    }
    return appconfig;
  }

  public GraphDatabaseBenchmark(String inputPath) throws IllegalArgumentException {
    final Configuration appconfig;
    try {
      appconfig = inputPath == null ? getAppconfigFromClasspath()
          : new PropertiesConfiguration(new File(inputPath));
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException(
          String.format("Unable to load properties file %s because %s", inputPath, e.getMessage()));
    }
    config = new BenchmarkConfiguration(appconfig);
  }

  public void run() {
    for (BenchmarkType type : config.getBenchmarkTypes()) {
      runBenchmark(type);
    }
  }

  private final void runBenchmark(BenchmarkType type) {
    final Benchmark benchmark;
    logger.info(type.longname() + " Benchmark Selected");
    switch (type) {
    case MASSIVE_INSERTION:
      benchmark = new BenchmarkBase(config, BenchmarkType.MASSIVE_INSERTION) {
        @Override
        public void benchmarkWithDB(GraphDatabase db) {
          db.massiveModeLoading();
        }
      };
      break;
    case SINGLE_INSERTION:
      benchmark = new BenchmarkBase(config, BenchmarkType.SINGLE_INSERTION) {
        @Override
        public void benchmarkWithDB(GraphDatabase db) {
          db.singleModeLoading();
        }
      };
      break;
    case CLUSTERING:
      benchmark = new ClusteringBenchmark(config);
      break;
    case KHOP:
      benchmark = new BenchmarkBase(config, BenchmarkType.KHOP) {
        @Override
        public void benchmarkWithDB(GraphDatabase db) {
          db.khopQuery();
        }
      };
      break;
    case FIND_SHORTEST_PATH:
      benchmark = new BenchmarkBase(config, BenchmarkType.FIND_SHORTEST_PATH) {
        @Override
        public void benchmarkWithDB(GraphDatabase db) {
          db.shortestPaths();
        }
      };
      break;
    case DELETION:
      benchmark = new BenchmarkBase(config, BenchmarkType.DELETION) {
        @Override
        public void benchmarkWithDB(GraphDatabase db) {
          db.delete();
        }
      };
      break;
    default:
      throw new UnsupportedOperationException("unsupported benchmark " + type == null ? "null" : type.toString());
    }
    benchmark.startBenchmark();
  }

  /**
   * This is the main function. Set the proper property file and run
   * 
   * @throws ExecutionException
   */
  public static void main(String[] args) throws ExecutionException {
    final String inputPath = args.length != 1 ? null : args[0];
    GraphDatabaseBenchmark benchmarks = new GraphDatabaseBenchmark(inputPath);
    try {
      benchmarks.run();
    } catch (Throwable t) {
      logger.fatal(t.getMessage(), t);
      System.exit(1);
    }
    System.exit(0);
  }

  public void cleanup() {
    try {
      FileDeleteStrategy.FORCE.delete(config.getDbStorageDirectory());
    } catch (IOException e) {
      logger.fatal("Unable to clean up db storage directory: " + e.getMessage());
      System.exit(1);
    }
  }
}
