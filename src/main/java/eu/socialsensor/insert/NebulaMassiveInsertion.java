package eu.socialsensor.insert;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.vesoft.nebula.client.graph.GraphClient;

import eu.socialsensor.main.BenchmarkingException;

/**
 * Implementation of massive Insertion in nebula graph database
 */
public class NebulaMassiveInsertion implements Insertion<Long> {
  private static final Logger LOG = LogManager.getLogger();
  private final Set<Long> cache;
  private final GraphClient graph;
  private final int batch;

  public NebulaMassiveInsertion(GraphClient client, int batch) {
    this.graph = client;
    cache = new HashSet<>();
    this.batch = batch;
  }

  @Override
  public void createGraph(File datasetFile) {
    LOG.info("Loading data in {} database...");
    final AtomicInteger i = new AtomicInteger(1);
    LineIterator it = null;
    Multimap<String,String> graph = ArrayListMultimap.create(); 
    try {
      it = FileUtils.lineIterator(datasetFile, "UTF-8");
      while (it.hasNext()) {
        String line = it.nextLine();
        if (!line.startsWith("#")) {
          String[] vertexs = line.split("\t");
          graph.put(vertexs[0], vertexs[1]);
          if (i.getAndIncrement() % batch == 0) {
            batchInsert(graph);
          }
        }
      }
      if (graph.size() > 0) {
        batchInsert(graph);
      }
    } catch (IOException e) {
      throw new BenchmarkingException("Unable to read lines from file: " + datasetFile.getAbsolutePath(), e);
    } finally {
      LineIterator.closeQuietly(it);
    }
  }

  private void batchInsert(Multimap<String,String> graph) {
    StringBuilder vertexAdd = new StringBuilder("INSERT VERTEX node(nodeId) VALUES ");
    StringBuilder edgeAdd = new StringBuilder("INSERT EDGE similar() VALUES ");
    for(Map.Entry<String, String> entry : graph.entries()) {
      Long srcId = Long.valueOf(entry.getKey()) + 1;
      Long targetId = Long.valueOf(entry.getValue()) + 1;
      if (!cache.contains(srcId)) {
        vertexAdd.append(srcId).append(":(").append(srcId).append("),");
        cache.add(srcId);
      }
      if (!cache.contains(targetId)) {
        vertexAdd.append(targetId).append(":(").append(targetId).append("),");
        cache.add(targetId);
      }
      edgeAdd.append(srcId).append(" -> ").append(targetId).append(":(),");
    }
    vertexAdd.deleteCharAt(vertexAdd.length() - 1).append(";");
    edgeAdd.deleteCharAt(edgeAdd.length() - 1).append(";");
    this.graph.execute(vertexAdd.toString());
    this.graph.execute(edgeAdd.toString());
    LOG.trace(vertexAdd.toString());
    LOG.trace(edgeAdd.toString());
    graph.clear();
  }
}
