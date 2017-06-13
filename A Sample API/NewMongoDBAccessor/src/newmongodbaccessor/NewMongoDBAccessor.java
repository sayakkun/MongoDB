package newmongodbaccessor;
import debugSignalHandler.DebugSignalHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class NewMongoDBAccessor
{
    private static final Logger LOGGER = LogManager.getLogger(NewMongoDBAccessor.class.getName());
    public static boolean initDataBaseConnection()
    {
        MongoDBConfiguration mongoDBConfigurationInstance = MongoDBConfiguration.getMongoDBConfigurationInstance();
        if (mongoDBConfigurationInstance == null)
        {
            LOGGER.fatal("MongoDBConfiguration cannot be instantiated");
            return false;
        }
        mongoDBConfigurationInstance.reloadConfig();
        MongoDBHandler mongoDBHandlerInstance = MongoDBHandler.getMongoDBHandlerInstance();
        if(mongoDBHandlerInstance==null)
        {
            LOGGER.fatal("MongoDBHandler cannot be instantiated");
            return false;
        }
        return mongoDBHandlerInstance.initMongoDBHandler();
    }
    public static void main(String[] args)
    {
        //handle signal siguser2 
        DebugSignalHandler.listenTo("USR2");
        initDataBaseConnection();
        MongoDBTester.testing();
    }    
}
