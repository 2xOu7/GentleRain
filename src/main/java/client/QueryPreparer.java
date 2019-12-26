package client;
import spark.Request;
import spark.Response;
/**
 * Prepares all queries prior to the query being processed
 */

public class QueryPreparer {

    private static String CONTENT_TYPE= "Content-Type";
    private static String JSON_RES = "application/json";

    public static void prepare(Request req, Response res) {
        res.header(CONTENT_TYPE, JSON_RES);
    }

}
