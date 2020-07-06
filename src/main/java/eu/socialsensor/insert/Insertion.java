package eu.socialsensor.insert;

import java.io.File;

/**
 * Represents the insertion of data in each graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 */
public interface Insertion<T> {
    /**
     * Loads the data in each graph database
     * @param datasetDir
     * @return the generate random vertex id
     */
    public void createGraph(File dataset);
}
