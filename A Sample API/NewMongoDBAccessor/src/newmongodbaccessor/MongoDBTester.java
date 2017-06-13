package newmongodbaccessor;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.HashMap;
public abstract class MongoDBTester
{

    private static final Logger LOGGER = LogManager.getLogger(MongoDBTester.class.getName());
    public static void testing()
    {
        MongoDBHandler mongoDBHandlerInstance = MongoDBHandler.getMongoDBHandlerInstance();
        //mongoDBHandlerInstance.initMongoDBHandler();
        //slready initialized
        LOGGER.info("fetchSpecificRowsWhereBasicQuery is called");
        HashMap<String,Object> whereNotEqualToDetails=new HashMap<>();
        whereNotEqualToDetails.put("Age",23);
        ArrayList<String> queryResultVector
                = mongoDBHandlerInstance.fetchSpecificRowsWhereNotEqualToQuery("uniqueIDCard2",whereNotEqualToDetails,null,null);
        for (String queryResult : queryResultVector)
        {
            LOGGER.info(queryResult);
        }
    }
}
//