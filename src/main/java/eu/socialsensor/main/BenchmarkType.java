package eu.socialsensor.main;

import java.util.HashSet;
import java.util.Set;

public enum BenchmarkType {
  MASSIVE_INSERTION("Massive Insertion", "MassiveInsertion"),
  SINGLE_INSERTION("Single Insertion", "SingleInsertion"),
  DELETION("Delete Graph", "DeleteGraph"),
  KHOP("K-hop query", "KHop"),
  FIND_SHORTEST_PATH("Find Shortest Path", "FindShortest"),
  CLUSTERING("Clustering", "Clustering");

  public static final Set<BenchmarkType> INSERTING_BENCHMARK_SET = new HashSet<BenchmarkType>();
  static {
    INSERTING_BENCHMARK_SET.add(MASSIVE_INSERTION);
    INSERTING_BENCHMARK_SET.add(SINGLE_INSERTION);
  }

  private final String longname;
  private final String filenamePrefix;

  private BenchmarkType(String longName, String filenamePrefix) {
    this.longname = longName;
    this.filenamePrefix = filenamePrefix;
  }

  public String longname() {
    return longname;
  }

  public String getResultsFileName() {
    return filenamePrefix + ".csv";
  }
}
