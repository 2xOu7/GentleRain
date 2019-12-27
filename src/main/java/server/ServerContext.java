package server;

/**
 * Runtime context for the server class
 */

public class ServerContext {

    private static Server server; // the current server instance

    private static GSTAggregator gstAggregator; // the current GST Aggregator instance

    public static GSTAggregator getGstAggregator() {
        return gstAggregator;
    }

    public static Server getServer() {
        return server;
    }

    public static void setGstAggregator(GSTAggregator gstAggregator) {
        ServerContext.gstAggregator = gstAggregator;
    }

    public static void setServer(Server server) {
        ServerContext.server = server;
    }
}
