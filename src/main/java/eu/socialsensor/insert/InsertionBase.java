package eu.socialsensor.insert;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Base class for business logic of insertion workloads
 * 
 * @author Alexander Patrikalakis
 *
 * @param <T> the Type of vertexes (graph database vendor specific)
 */
public abstract class InsertionBase<T> implements Insertion<T> {
  private static final Logger logger = LogManager.getLogger();
  public static final String INSERTION_CONTEXT = ".eu.socialsensor.insertion.";
  public static final String SIMILAR = GraphDatabaseBase.SIMILAR;
  public static final String NODEID = GraphDatabaseBase.NODE_ID;
  public static final String NODE_LABEL = GraphDatabaseBase.NODE_LABEL;
  protected final GraphDatabaseType type;

  protected InsertionBase(GraphDatabaseType type) {
    this.type = type;
  }

  public final void createGraph(File datasetFile) {
    logger.info("Loading data in {} database...", type.getApi());
    final AtomicInteger i = new AtomicInteger(1);
    LineIterator it = null;
    try {
      it = FileUtils.lineIterator(datasetFile, "UTF-8");
      while (it.hasNext()) {
        String line = it.nextLine();
        if (!line.startsWith("#")) {
          String[] vertexs = line.split("\t");
          T srcVtx = getOrCreate(vertexs[0]);
          T targetVtx = getOrCreate(vertexs[1]);
          relateNodes(srcVtx, targetVtx, i.getAndIncrement());
        }
      }
    } catch (IOException e) {
      throw new BenchmarkingException("Unable to read lines from file: " + datasetFile.getAbsolutePath(), e);
    } finally {
      LineIterator.closeQuietly(it);
    }
    postInsert();
    logger.trace("Edges: " + i.get());
  }

  public abstract T getOrCreate(final String value);

  public abstract void relateNodes(final T src, final T dest, final int edgeId);

  protected void postInsert() {
    // NOOP
  }
}
