package client;
import spark.Request;
import spark.Response;
/**
 * Prepares all queries prior to the query being processed
 */

public class QueryPreparer {

    private static String CONTENT_TYPE= "Content-Type";
    private static String JSON_RES = "application/json";

    /**
     * Prepare a query before being processed
     * @param req - request of query
     * @param res - response of query
     */

    public static void prepareQuery(Request req, Response res) {
        res.header(CONTENT_TYPE, JSON_RES);
    }

}
