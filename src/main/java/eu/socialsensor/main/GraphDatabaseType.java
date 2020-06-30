package eu.socialsensor.main;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Enum containing constants that correspond to each database.
 * 
 * @author Alexander Patrikalakis
 */
public enum GraphDatabaseType
{
    JANUS_HBASE("Janus", "hbase", "jhb"),
    NEO4J("Neo4j", null, "neo4j");

    private final String backend;
    private final String api;
    private final String shortname;

    public static final Map<String, GraphDatabaseType> STRING_REP_MAP = new HashMap<String, GraphDatabaseType>();
    public static final Set<GraphDatabaseType> JANUS_FLAVORS = new HashSet<GraphDatabaseType>();
    static
    {
        for (GraphDatabaseType db : values())
        {
            STRING_REP_MAP.put(db.getShortname(), db);
        }
        JANUS_FLAVORS.add(JANUS_HBASE);
    }

    private GraphDatabaseType(String api, String backend, String shortname)
    {
        this.api = api;
        this.backend = backend;
        this.shortname = shortname;
    }

    public String getBackend()
    {
        return backend;
    }

    public String getApi()
    {
        return api;
    }

    public String getShortname()
    {
        return shortname;
    }
}
