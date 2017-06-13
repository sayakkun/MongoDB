package newmongodbaccessor;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import org.apache.logging.log4j.Logger;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Semaphore;
import org.apache.logging.log4j.LogManager;

public class MongoDBHandler
{

    //if reloadConfig of MongoDBConfiguration 
    private static final Logger LOGGER = LogManager.getLogger(MongoDBHandler.class.getName());
    private String hostIP;
    private short hostPort;
    private String databaseName;
    private String userName;
    private String password;
    private String validDataTable;
    private String historyTable;
    private boolean isInitMongoDBHandler = false;
    private static MongoDBHandler databaseHandlerInstance;
    private MongoClient mongoClientInstance;
    private DB databaseInstance;
    private Set<String> collectionNames = null;
    private HashMap<String, DBCollection> CollectionNameInstanceMapping = null;
    private static final Semaphore MONGO_DB_INSTANCE_CREATION_PURPOSED_MUTEX = new Semaphore(1);
    private MongoDBHandler(String hostIP, short hostPort, String databaseName,
            String userName, String password)
    {
        this.hostIP = hostIP;
        this.hostPort = hostPort;
        this.databaseName = databaseName;
        this.userName = userName;
        this.password = password;
        //now, collectionNames cannot be instantiated as collectionNames is a set
        //as java.util.set is an interface\
        LOGGER.info("HostIP is : " + this.hostIP);
        LOGGER.info("Host Port is : " + this.hostPort);
        LOGGER.info("Database Name is: " + this.databaseName);
        LOGGER.info("User Name is: " + this.userName);
        LOGGER.info("Password is: " + this.password);
        collectionNames = new HashSet<>();
        CollectionNameInstanceMapping = new HashMap<>();
        //initialise the hashtable
    }
    public static MongoDBHandler getMongoDBHandlerInstance()
    {
        //that means instance is not created
        if (databaseHandlerInstance == null)
        {
            //never instantiated
            MongoDBConfiguration dataBaseConfigurationInstance = MongoDBConfiguration.getMongoDBConfigurationInstance();
            String tempHostIP = dataBaseConfigurationInstance.returnHostIP();
            short tempHostPort = dataBaseConfigurationInstance.returnHostPort();
            String tempDatabaseName = dataBaseConfigurationInstance.returnDataBaseName();
            String tempUserName = dataBaseConfigurationInstance.returnUserName();
            String tempPassword = dataBaseConfigurationInstance.returnPassword();
            try
            {
                MONGO_DB_INSTANCE_CREATION_PURPOSED_MUTEX.acquire();
                databaseHandlerInstance = new MongoDBHandler(tempHostIP, tempHostPort,
                        tempDatabaseName, tempUserName, tempPassword);
                //now, databaseHandlerInstance is created
                MONGO_DB_INSTANCE_CREATION_PURPOSED_MUTEX.release();
                return databaseHandlerInstance;
            }
            catch (InterruptedException e)
            {
                LOGGER.fatal("The MONGO_DB_INSTANCE_CREATION_PURPOSED_MUTEX cannot be"
                        + " accquied");
                return null;
            }
            finally
            {
                MONGO_DB_INSTANCE_CREATION_PURPOSED_MUTEX.release();
            }
        }
        else
        {
            return databaseHandlerInstance;
        }
    }
    public boolean initMongoDBHandler()
    {
        if (isInitMongoDBHandler)
        {
            try
            {
                mongoClientInstance = new MongoClient(hostIP, hostPort);
                //also establish the connection
            }
            catch (UnknownHostException e)
            {
                LOGGER.fatal("The mongoClientInstance cannot be created successfully");
                return false;
            }
            LOGGER.debug("The mongoclientInstance is created succssfully");
            DB adminDataBaseInstance = mongoClientInstance.getDB("admin");
            if (adminDataBaseInstance == null)
            {
                LOGGER.fatal("The Database admin cannot be accessed");
                return false;
            }
            LOGGER.debug("The admin database is successfuly accessed");
            try
            {
                boolean auth = adminDataBaseInstance.authenticate(userName, password.toCharArray());
                if (auth == false)
                {
                    LOGGER.fatal("database authentication is failed");
                    return false;
                }
            }
            catch (Exception e)
            {
                LOGGER.fatal("Authentication is false");
                LOGGER.fatal("It could happen for multiple reasons. Including wrong IP "
                        + "and password");
                return false;
            }
            LOGGER.info("Database authentication is successful");
            databaseInstance = mongoClientInstance.getDB(databaseName);
            if (databaseInstance == null)
            {
                LOGGER.fatal("The Database " + databaseName + " cannot be accessed");
                return false;
            }
            LOGGER.info("The connection to database " + databaseName + " is done successfully");
            collectionNames.clear();
            collectionNames = databaseInstance.getCollectionNames();
            if (collectionNames == null)
            {
                LOGGER.error("No collections/tables are found in the database: "
                        + databaseName);
            }
            /*LOGGER.info("The tables present in the database are following:");
            for (String name : collectionNames)
            {
                LOGGER.info(name);
            }*/
            isInitMongoDBHandler = true;
            return true;
        }
        else
        {
            MongoDBConfiguration dataBaseConfigurationInstance
                    = MongoDBConfiguration.getMongoDBConfigurationInstance();
            hostIP = dataBaseConfigurationInstance.returnHostIP();
            hostPort = dataBaseConfigurationInstance.returnHostPort();
            databaseName = dataBaseConfigurationInstance.returnDataBaseName();
            userName = dataBaseConfigurationInstance.returnUserName();
            password = dataBaseConfigurationInstance.returnPassword();
            try
            {
                mongoClientInstance = new MongoClient(hostIP, hostPort);
                //also establish the connection
            }
            catch (UnknownHostException e)
            {
                LOGGER.fatal("The mongoClientInstance cannot be created successfully");
                return false;
            }
            
            LOGGER.debug("The mongoclientInstance is created succssfully");
            DB adminDataBaseInstance = mongoClientInstance.getDB("admin");
            if (adminDataBaseInstance == null)
            {
                LOGGER.fatal("The Database admin cannot be accessed");
                return false;
            }
            LOGGER.debug("The admin database is successfuly accessed");
            try
            {
                boolean auth = adminDataBaseInstance.authenticate(userName, password.toCharArray());
                if (auth == false)
                {
                    LOGGER.fatal("database authentication is failed");
                    return false;
                }
            }
            catch (Exception e)
            {
                LOGGER.fatal("Authentication is false");
                LOGGER.fatal("It could happen for multiple reasons. Including wrong IP "
                        + "and password");
                return false;
            }
            LOGGER.info("Database authentication is successful");
            databaseInstance = mongoClientInstance.getDB(databaseName);
            if(databaseInstance == null)
            {
                LOGGER.fatal("The Database " + databaseName + " cannot be accessed");
                return false;
            }
            LOGGER.info("The connection to database " + databaseName + " is done successfully");
            collectionNames.clear();
            CollectionNameInstanceMapping.clear();
            collectionNames=databaseInstance.getCollectionNames();
            if (collectionNames == null)
            {
                LOGGER.error("No collections/tables are found in the database: "
                        + databaseName);
                return false;
                //for simplicity we return false
                /*
            We will offer a function o create a collection in the specified db
            separately
                 */

            }
            return true;
        }   
    }
    public boolean createATable(String tableName, Set<String> uniqueKeys)
    {
        //Suppose, you want to prohibit/ban duplicate etries
        /*
        So, you have to call createIndex function 
         */
        LOGGER.debug("In createATable function which accepts a tableName and"
                + " a set of uniqueKeys");
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        synchronized (this)
        {
            //synchronized must be put here, as the Hahstable we use is common resource
            DBCollection tableInstance = databaseInstance.createCollection(tableName,
                    new BasicDBObject());
            //anyways, DBCOllection tableInstance-databaseInstance.getCollection(collectionName)
            //actually works fine
            //first create the table
            //after it create the index
            DBObject uniqueKeyandValues;
            DBObject optionsandValues;
            uniqueKeyandValues = new BasicDBObject();
            for (String key : uniqueKeys)
            {
                uniqueKeyandValues.put(key, 1);
                //every key is chosen to be in ascending order
            }
            LOGGER.debug("uniqueKeyandValues is constructed successfully");
            optionsandValues = new BasicDBObject();
            //optionsandValues.put("background", true);
            //so, indexing process is done in background
            /*
            Optional. Builds the index in the background so that building an index 
            does not block other database activities. Specify true to build in the 
            background. The default value is false.
             */
            optionsandValues.put("unique", true);
            LOGGER.debug("optionsndValues is constructed successfully");
            try
            {
                tableInstance.ensureIndex(uniqueKeyandValues, optionsandValues);
            }
            catch (Exception e)
            {
                LOGGER.debug("Exception in createIndex function " + e.getClass().getSimpleName());
            }
            try
            {
                boolean addInSetFlag = collectionNames.add(tableName);
                if (!addInSetFlag)
                {
                    LOGGER.error("The table name " + tableName + " cannot be added in collectionNames set");
                    return false;
                }
            }
            catch (Exception e)
            {
                LOGGER.error("The tableName " + tableName + " cannot be added in the set collectionNames"
                        + " due to exception: " + e.getClass().getSimpleName());
                return false;

            }
            LOGGER.info("The tableName " + tableName + " is successfully added to collectionNames");
            //add the table name to the existing collectionNames set
            //put procedure does not throw any exception
            CollectionNameInstanceMapping.put(tableName, tableInstance);
            //we want the rightTableInstance to be saved for right table
            return true;
        }
        //let's hope it behaves as I thought
    }
    public boolean createATable(String tableName)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        synchronized (this)
        {
            DBCollection tableInstance = databaseInstance.createCollection(tableName,
                    new BasicDBObject());
            //anyways, DBCOllection tableInstance-databaseInstance.getCollection(collectionName)
            //actually works fine
            if (tableInstance != null)
            {
                LOGGER.error("The table" + tableName + " cannnot be created");
                return false;
            }
            if (!databaseInstance.collectionExists(tableName))
            {
                LOGGER.error("The table" + tableName + " cannot be created");
                return false;
            }
            try
            {
                boolean addInCollectionFlag = collectionNames.add(tableName);
                if (!addInCollectionFlag)
                {
                    LOGGER.error("The tableName " + tableName + " cannot be added to "
                            + "collectionNames");
                    return false;
                }

            }
            catch (Exception e)
            {
                LOGGER.error("The tableName " + tableName + " cannot be added to collectionNames"
                        + "due to exception: " + e.getClass().getSimpleName());
                return false;
            }
            LOGGER.info("The tableName " + tableName + " is successfully added to collectionNames");
            //add the table name to the existing collectionNames set
            CollectionNameInstanceMapping.put(tableName, tableInstance);
            return true;
            //similarly, synchronized block ensure that righttableinstance is saved for righttablename
        }
    }
    public synchronized void showExistingTable()
    {
        //use synchronized since, if two threads simultaneously calls it, the log file will be a common resoure
        LOGGER.info("The existing tables in the database are shown below");
        collectionNames.forEach((tableName) ->
        {
            LOGGER.info(tableName);
        });
    }

    public boolean insertIntoTable(String tableName, BasicDBObject toInsertData)
    {
        //we are assuming that indexing is already done
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        if (toInsertData == null)
        {
            LOGGER.error("The data passed to the function is null");
            return false;
        }
        /*LOGGER.debug("The table names are the following:");
        for (String collectionName : collectionNames)
        {
            LOGGER.info(collectionName);
        }*/
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            //we need not to check if tableInstance is empty hashTable or not
            try
            {
                WriteResult insertResult = tableInstance.insert(toInsertData);
                if (insertResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The insertion process invokes some error: "
                            + insertResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            catch (Exception e)
            {
                LOGGER.error("Insertion is unsuccessful " + e);
                return false;
            }
            //try to print it during a successful insertion
            //insert the data
            /*
        is actually this version
        public WriteResult insert(DBObject[] arr)
        is called?
             */
            return true;
        }
    }

    public boolean insertIntoTable(String tableName, HashMap<String, Object> data)
    {
        //we are assuming that indexing is already done
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        if (data == null)
        {
            LOGGER.error("The data passed to the function is null");
            return false;
        }
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            //we need not to check if tableInstance is empty hashTable or not
            BasicDBObject toInsertData = new BasicDBObject(data);
            //this parses a json string and returns a basicDBObject
            if (toInsertData == null)
            {
                LOGGER.error("The BasicObject toInsertData is null");
                return false;
            }
            LOGGER.info("BasicDBObject toInsertData is created successfully");
            try
            {
                WriteResult insertResult;
                insertResult = tableInstance.insert(toInsertData);
                if (insertResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The insertion process invokes some error: "
                            + insertResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            catch (Exception e)
            {
                LOGGER.error("Insertion is unsuccessful");
                return false;
            }
            //try to print it during a successful insertion
            //insert the data
            /*
        is actually this version
        public WriteResult insert(DBObject[] arr)
        is called?
             */
            return true;
        }
    }

    //again synchronised is used to ensure that inserttion is done in right table
    public ArrayList<String> fetchAllRows(String tableName)
    {
        /*
        I cannot define another function 
        public void fetchAllRows(String tableName)
        Because, function signature must be different for overloading
        and function signature consists of function name and function arguments 
        not return type
         */
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor findResults;

            findResults = tableInstance.find();
            if (findResults == null)
            {
                LOGGER.error("No entry is found in the tableName: " + tableName);
                return null;
            }
            /*
        Now, from from vector creation to pushing results in 
             */
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (findResults.hasNext())
            {
                String result = findResults.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    /*public ArrayList<String> fetchAllRowsSpecificColumns()
    {
        return null;
    }*/

 /*obsolete collection warning due to use of vector*/
    public ArrayList<String> fetchSpecificRowsWhereBasicQuery(String tableName,
            HashMap<String, Object> whereQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereQueryResult;
            BasicDBObject whereQuery = new BasicDBObject();
            if ((whereQueryDetails != null) && (whereQueryDetails.isEmpty() == false))
            {

                for (String keyWhereQuery : whereQueryDetails.keySet())
                {
                    Object valWhereQuery = whereQueryDetails.get(keyWhereQuery);
                    whereQuery.put(keyWhereQuery, valWhereQuery);
                }
                //if we do not mention anything in whereQueryDetails
            }
            if (limit == null)
            {
                if (skipFirstFew == null)
                {
                    whereQueryResult = tableInstance.find(whereQuery);
                }
                else
                {
                    whereQueryResult = tableInstance.find(whereQuery).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereQueryResult = tableInstance.find(whereQuery).limit(limit.intValue());
                }
                else
                {
                    whereQueryResult = tableInstance.find(whereQuery).limit(limit.intValue()).skip(limit.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereQueryResult.hasNext())
            {
                String result = whereQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereNotEqualToQuery(String tableName,
            HashMap<String, Object> whereNotEqualToQueryDetails, Integer limit,
            Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereNotEqualToQueryResult;
            BasicDBObject whereNotEqualToQuery = new BasicDBObject();
            if ((whereNotEqualToQueryDetails != null) && (whereNotEqualToQueryDetails.isEmpty() == false))
            {
                for (String whereNotEqualToQueryKey : whereNotEqualToQueryDetails.keySet())
                {
                    Object whereNotEqualToQueryValue = whereNotEqualToQueryDetails.get(whereNotEqualToQueryKey);
                    whereNotEqualToQuery.put(whereNotEqualToQueryKey, new BasicDBObject("$ne", whereNotEqualToQueryValue));
                }

            }
            if (limit == null)
            {
                if (skipFirstFew == null)
                {
                    whereNotEqualToQueryResult = tableInstance.find(whereNotEqualToQuery);
                }
                else
                {
                    whereNotEqualToQueryResult
                            = tableInstance.find(whereNotEqualToQuery).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereNotEqualToQueryResult = tableInstance.find(whereNotEqualToQuery).limit(limit.intValue());
                }
                else
                {
                    whereNotEqualToQueryResult
                            = tableInstance.find(whereNotEqualToQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereNotEqualToQueryResult.hasNext())
            {
                String result = whereNotEqualToQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereInQuery(String tableName,
            HashMap<String, List> whereInQueryDetails, Integer limit, Integer skipFirstFew)
    {
        /*
        SELECT column_name(s)
        FROM table_name
        WHERE column_name IN (value1, value2, ...);
         */
 /*
        Suppose, we face a query like
        SELECT COLUMN_NAME1,COLUMN_NAME2 FROM TABLE_NAME WHERE COLUMN_NAME1 
        IN (VALUE11,VALUE12) AND COLUMN_NMAE2 IN (VALUE21, VALUE22)
         */
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereInQueryResult;
            BasicDBObject whereInQuery = new BasicDBObject();
            if ((whereInQueryDetails != null) && (whereInQueryDetails.isEmpty() == false))
            {
                for (String whereInQueryKey : whereInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereInQueryDetails.get(whereInQueryKey);
                    whereInQuery.put(whereInQueryKey, new BasicDBObject("$in", whereInQueryValueLists));
                }
            }
            if (limit == null)
            {
                if (skipFirstFew == null)
                {
                    whereInQueryResult = tableInstance.find(whereInQuery);
                }
                else
                {
                    whereInQueryResult = tableInstance.find(whereInQuery).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereInQueryResult
                            = tableInstance.find(whereInQuery).limit(limit.intValue());
                }
                else
                {
                    whereInQueryResult
                            = tableInstance.find(whereInQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereInQueryResult.hasNext())
            {
                String result = whereInQueryResult.next().toString();
                findResultsArrayList.add(result);
            }

            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereNotInQuery(String tableName,
            HashMap<String, List> whereNotInQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        /*
        SELECT column_name(s)
        FROM table_name
        WHERE column_name IN (value1, value2, ...);
         */
 /*
        Suppose, we face a query like
        SELECT COLUMN_NAME1,COLUMN_NAME2 FROM TABLE_NAME WHERE COLUMN_NAME1 
        IN (VALUE11,VALUE12) AND COLUMN_NMAE2 IN (VALUE21, VALUE22)
         */
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereNotInQueryResult;
            BasicDBObject whereNotInQuery = new BasicDBObject();
            if ((whereNotInQueryDetails != null) && (whereNotInQueryDetails.isEmpty() == false))
            {
                for (String whereNotInQueryKey : whereNotInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereNotInQueryDetails.get(whereNotInQueryKey);
                    whereNotInQuery.put(whereNotInQueryKey, new BasicDBObject("$nin", whereInQueryValueLists));
                }
            }
            if (limit != null)
            {
                if (skipFirstFew == null)
                {
                    whereNotInQueryResult = tableInstance.find(whereNotInQuery).limit(limit.intValue());
                }
                else
                {
                    whereNotInQueryResult
                            = tableInstance.find(whereNotInQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereNotInQueryResult = tableInstance.find(whereNotInQuery);
                }
                else
                {
                    whereNotInQueryResult = tableInstance.find(whereNotInQuery).skip(skipFirstFew.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereNotInQueryResult.hasNext())
            {
                String result = whereNotInQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereGreaterThanQuery(String tableName,
            HashMap<String, Object> whereGreaterThanQueryDetails,
            Integer limit, Integer skipFirstFew)
    {

        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereGreaterThanQueryResult;
            BasicDBObject whereGreaterThanQuery = new BasicDBObject();
            if ((whereGreaterThanQueryDetails != null) && (whereGreaterThanQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanQueryKey : whereGreaterThanQueryDetails.keySet())
                {
                    Object whereGreaterThanQueryValue = whereGreaterThanQueryDetails.get(whereGreaterThanQueryKey);
                    whereGreaterThanQuery.put(whereGreaterThanQueryKey, new BasicDBObject("$gt", whereGreaterThanQueryValue));
                }
            }
            if (limit == null)
            {
                if (skipFirstFew == null)
                {
                    whereGreaterThanQueryResult = tableInstance.find(whereGreaterThanQuery);
                }
                else
                {
                    whereGreaterThanQueryResult
                            = tableInstance.find(whereGreaterThanQuery).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereGreaterThanQueryResult = tableInstance.find(whereGreaterThanQuery).limit(limit.intValue());
                }
                else
                {
                    whereGreaterThanQueryResult
                            = tableInstance.find(whereGreaterThanQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereGreaterThanQueryResult.hasNext())
            {
                String result = whereGreaterThanQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereGreaterThanOrEqualQuery(String tableName, HashMap<String, Object> whereGreaterThanOrEqualQueryDetails,
            Integer limit, Integer skipFirstFew)
    {

        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereGreaterThanOrEqualQueryResult;
            BasicDBObject whereGreaterThanOrEqualQuery = new BasicDBObject();
            if ((whereGreaterThanOrEqualQueryDetails != null) && (whereGreaterThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanOrEqualQueryKey : whereGreaterThanOrEqualQueryDetails.keySet())
                {
                    Object whereGreaterThanOrEqualQueryValue = whereGreaterThanOrEqualQueryDetails.get(whereGreaterThanOrEqualQueryKey);
                    whereGreaterThanOrEqualQuery.put(whereGreaterThanOrEqualQueryKey, new BasicDBObject("$gte", whereGreaterThanOrEqualQueryValue));
                }
            }
            if (limit != null)
            {
                if (skipFirstFew != null)
                {
                    whereGreaterThanOrEqualQueryResult
                            = tableInstance.find(whereGreaterThanOrEqualQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereGreaterThanOrEqualQueryResult
                            = tableInstance.find(whereGreaterThanOrEqualQuery).limit(limit.intValue());
                }
            }
            else
            {
                if (skipFirstFew != null)
                {
                    whereGreaterThanOrEqualQueryResult = tableInstance.find(whereGreaterThanOrEqualQuery).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereGreaterThanOrEqualQueryResult = tableInstance.find(whereGreaterThanOrEqualQuery);
                }
            }
            ArrayList<String> findResultsArrayList;

            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereGreaterThanOrEqualQueryResult.hasNext())
            {
                String result = whereGreaterThanOrEqualQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereLessThanQuery(String tableName,
            HashMap<String, Object> whereLessThanQueryDetails, Integer limit,
            Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereLessThanQueryResult;
            BasicDBObject whereLessThanQuery = new BasicDBObject();
            if ((whereLessThanQueryDetails != null) && (whereLessThanQueryDetails.isEmpty() == false))
            {

                for (String whereLessThanQueryKey : whereLessThanQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanQueryDetails.get(whereLessThanQueryKey);
                    whereLessThanQuery.put(whereLessThanQueryKey, new BasicDBObject("$lt", whereLessThanQueryValue));
                }
            }
            if (limit == null)
            {
                if (skipFirstFew == null)
                {
                    whereLessThanQueryResult = tableInstance.find(whereLessThanQuery);
                }
                else
                {
                    whereLessThanQueryResult
                            = tableInstance.find(whereLessThanQuery).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereLessThanQueryResult
                            = tableInstance.find(whereLessThanQuery).limit(limit.intValue());
                }
                else
                {
                    whereLessThanQueryResult
                            = tableInstance.find(whereLessThanQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereLessThanQueryResult.hasNext())
            {
                String result = whereLessThanQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereLessThanOrEqualQuery(String tableName,
            HashMap<String, Object> whereLessThanOrEqualQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereLessThanOrEqualQueryResult;
            BasicDBObject whereLessThanOrEqualQuery = new BasicDBObject();
            if ((whereLessThanOrEqualQueryDetails != null) && (whereLessThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanOrEqualQueryKey : whereLessThanOrEqualQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanOrEqualQueryDetails.get(whereLessThanOrEqualQueryKey);
                    whereLessThanOrEqualQuery.put(whereLessThanOrEqualQueryKey, new BasicDBObject("$lte", whereLessThanQueryValue));
                }
            }
            if (limit != null)
            {
                if (skipFirstFew == null)
                {
                    whereLessThanOrEqualQueryResult
                            = tableInstance.find(whereLessThanOrEqualQuery).limit(limit.intValue());
                }
                else
                {
                    whereLessThanOrEqualQueryResult
                            = tableInstance.find(whereLessThanOrEqualQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereLessThanOrEqualQueryResult = tableInstance.find(whereLessThanOrEqualQuery);
                }
                else
                {
                    whereLessThanOrEqualQueryResult
                            = tableInstance.find(whereLessThanOrEqualQuery).skip(skipFirstFew.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereLessThanOrEqualQueryResult.hasNext())
            {
                String result = whereLessThanOrEqualQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsInBetweenExclusiveQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereInBetweenQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereInBetweenQueryResult;
            BasicDBObject whereInBetweenQuery = new BasicDBObject();
            if ((whereInBetweenQueryDetails != null) && (whereInBetweenQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenQueryFieldName : whereInBetweenQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenQueryFieldRangeValues
                            = whereInBetweenQueryDetails.get(whereInBetweenQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereInBetweenQuery.put(whereInBetweenQueryFieldName,
                                new BasicDBObject("$gt", greaterThanValue).append("$lt", lessThanValue));
                    }
                }

            }
            if (limit != null)
            {
                if (skipFirstFew != null)
                {
                    whereInBetweenQueryResult
                            = tableInstance.find(whereInBetweenQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereInBetweenQueryResult = tableInstance.find(whereInBetweenQuery).limit(limit.intValue());
                }
            }
            else
            {
                if (skipFirstFew != null)
                {
                    whereInBetweenQueryResult = tableInstance.find(whereInBetweenQuery).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereInBetweenQueryResult = tableInstance.find(whereInBetweenQuery);
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereInBetweenQueryResult.hasNext())
            {
                String result = whereInBetweenQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsInBetweenInclusiveQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereInBetweenQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereInBetweenQueryResult;
            BasicDBObject whereInBetweenQuery = new BasicDBObject();
            if ((whereInBetweenQueryDetails != null) && (whereInBetweenQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenQueryFieldName : whereInBetweenQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenQueryFieldRangeValues
                            = whereInBetweenQueryDetails.get(whereInBetweenQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereInBetweenQuery.put(whereInBetweenQueryFieldName,
                                new BasicDBObject("$gte", greaterThanValue).append("$lte", lessThanValue));
                    }
                }
            }
            if (limit != null)
            {
                if (skipFirstFew == null)
                {
                    whereInBetweenQueryResult = tableInstance.find(whereInBetweenQuery).limit(limit.intValue());
                }
                else
                {
                    whereInBetweenQueryResult = tableInstance.find(whereInBetweenQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
            }
            else
            {
                if (skipFirstFew == null)
                {
                    whereInBetweenQueryResult = tableInstance.find(whereInBetweenQuery);
                }
                else
                {
                    whereInBetweenQueryResult = tableInstance.find(whereInBetweenQuery).skip(skipFirstFew.intValue());
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereInBetweenQueryResult.hasNext())
            {
                String result = whereInBetweenQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereLikeQueryCaseSensitive(String tableName,
            HashMap<String, Object> whereLikeQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereLikeQueryResult;
            BasicDBObject whereLikeQuery = new BasicDBObject();
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    Object regexPattern = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    whereLikeQuery.put(whereLikeQueryFieldName,
                            new BasicDBObject("$regex", regexPattern));

                }
                whereLikeQueryResult = tableInstance.find(whereLikeQuery);

            }
            if (limit != null)
            {
                if (skipFirstFew != null)
                {
                    whereLikeQueryResult
                            = tableInstance.find(whereLikeQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery).limit(limit.intValue());
                }
            }
            else
            {
                if (skipFirstFew != null)
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery);
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereLikeQueryResult.hasNext())
            {
                String result = whereLikeQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereLikeQueryCaseInSensitive(String tableName,
            HashMap<String, Object> whereLikeQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereLikeQueryResult;
            BasicDBObject whereLikeQuery = new BasicDBObject();
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    Object regexPattern = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    whereLikeQuery.put(whereLikeQueryFieldName,
                            new BasicDBObject("$regex", regexPattern).append("$options", "i"));

                }

            }
            if (limit != null)
            {
                if (skipFirstFew != null)
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery).limit(limit.intValue());
                }
            }
            else
            {
                if (skipFirstFew != null)
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery);
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereLikeQueryResult.hasNext())
            {
                String result = whereLikeQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereLikeQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereLikeQueryDetails,
            Integer limit, Integer skipFirstFew)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereLikeQueryResult;
            BasicDBObject whereLikeQuery = new BasicDBObject();
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    HashMap<Object, Object> regexPatternAndOptions
                            = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    for (Object regexPattern : regexPatternAndOptions.keySet())
                    {
                        String options = (String) regexPatternAndOptions.get(regexPattern);
                        if (options != null)
                        {
                            for (int l = 0; l < options.length(); l++)
                            {
                                switch (options.charAt(l))
                                {
                                    case PatternMatchingOptions.CASE_INSENSITIVE:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.ALLOW_DOT_CHARACTER_TO_MATCH_ALL_CHARACTER:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.INCLUDE_ANCHOR:
                                    {
                                        //do not do anything 
                                        break;
                                    }
                                    default:
                                    {
                                        LOGGER.error("Invalid option is set");
                                        return null;
                                    }
                                }
                            }
                            whereLikeQuery.put(whereLikeQueryFieldName,
                                    new BasicDBObject("$regex", regexPattern).append("$options", options));
                        }
                        else
                        {
                            whereLikeQuery.put(whereLikeQueryFieldName, new BasicDBObject("$regex", regexPattern));
                        }
                    }
                }
            }
            if (limit != null)
            {
                if (skipFirstFew != null)
                {
                    whereLikeQueryResult
                            = tableInstance.find(whereLikeQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery).limit(limit.intValue());
                }
            }
            else
            {
                if (skipFirstFew != null)
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery).skip(skipFirstFew.intValue());
                }
                else
                {
                    whereLikeQueryResult = tableInstance.find(whereLikeQuery);
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereLikeQueryResult.hasNext())
            {
                String result = whereLikeQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    public ArrayList<String> fetchSpecificRowsWhereQuery(String tableName,
            HashMap<String, Object> whereBasicQueryDetails,
            HashMap<String, Object> whereGreaterThanOrEqualQueryDetails,
            HashMap<String, Object> whereGreaterThanQueryDetails,
            HashMap<String, Object> whereLessThanQueryDetails,
            HashMap<String, Object> whereLessThanOrEqualQueryDetails,
            HashMap<String, List> whereInQueryDetails,
            HashMap<String, List> whereNotInQueryDetails,
            HashMap<String, Object> whereNotEqualToQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereInBetweenExclusiveQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereInBetweenInclusiveQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereLikeQueryDetails,
            Integer limit,
            Integer skipFirstFew
    )
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return null;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return null;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return null;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBCursor whereQueryResult;
            DBObject whereQuery = new BasicDBObject();
            boolean isQueryConstructed = false;
            if (whereBasicQueryDetails != null && whereBasicQueryDetails.isEmpty() != true)
            {
                for (String whereBasicQueryFieldName : whereBasicQueryDetails.keySet())
                {
                    Object whereBasicQueryValue = whereBasicQueryDetails.get(whereBasicQueryFieldName);
                    whereQuery.put(whereBasicQueryFieldName, whereBasicQueryValue);
                }
                isQueryConstructed = true;
            }
            if ((whereNotEqualToQueryDetails != null) && (whereNotEqualToQueryDetails.isEmpty() == false))
            {
                for (String whereNotEqualToQueryKey : whereNotEqualToQueryDetails.keySet())
                {
                    Object whereNotEqualToQueryValue = whereNotEqualToQueryDetails.get(whereNotEqualToQueryKey);
                    whereQuery.put(whereNotEqualToQueryKey, new BasicDBObject("$ne", whereNotEqualToQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereInQueryDetails != null) && (whereInQueryDetails.isEmpty() == false))
            {
                for (String whereInQueryKey : whereInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereInQueryDetails.get(whereInQueryKey);
                    whereQuery.put(whereInQueryKey, new BasicDBObject("$in", whereInQueryValueLists));
                }
                isQueryConstructed = true;
            }
            if ((whereNotInQueryDetails != null) && (whereNotInQueryDetails.isEmpty() == false))
            {
                for (String whereNotInQueryKey : whereNotInQueryDetails.keySet())
                {
                    List whereNotInQueryValueLists = whereNotInQueryDetails.get(whereNotInQueryKey);
                    whereQuery.put(whereNotInQueryKey, new BasicDBObject("$nin", whereNotInQueryValueLists));
                }
                isQueryConstructed = true;
            }
            if ((whereGreaterThanQueryDetails != null) && (whereGreaterThanQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanQueryKey : whereGreaterThanQueryDetails.keySet())
                {
                    Object whereGreaterThanQueryValue = whereGreaterThanQueryDetails.get(whereGreaterThanQueryKey);
                    whereQuery.put(whereGreaterThanQueryKey, new BasicDBObject("$gt", whereGreaterThanQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereGreaterThanOrEqualQueryDetails != null) && (whereGreaterThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanOrEqualQueryKey : whereGreaterThanOrEqualQueryDetails.keySet())
                {
                    Object whereGreaterThanOrEqualQueryValue = whereGreaterThanOrEqualQueryDetails.get(whereGreaterThanOrEqualQueryKey);
                    whereQuery.put(whereGreaterThanOrEqualQueryKey, new BasicDBObject("$gte", whereGreaterThanOrEqualQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereLessThanQueryDetails != null) && (whereLessThanQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanQueryKey : whereLessThanQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanQueryDetails.get(whereLessThanQueryKey);
                    whereQuery.put(whereLessThanQueryKey, new BasicDBObject("$lt", whereLessThanQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereLessThanOrEqualQueryDetails != null) && (whereLessThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanOrEqualQueryKey : whereLessThanOrEqualQueryDetails.keySet())
                {
                    Object whereLessThanOrEqualQueryValue = whereLessThanOrEqualQueryDetails.get(whereLessThanOrEqualQueryKey);
                    whereQuery.put(whereLessThanOrEqualQueryKey, new BasicDBObject("$lte", whereLessThanOrEqualQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereInBetweenExclusiveQueryDetails != null) && (whereInBetweenExclusiveQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenExclusiveQueryFieldName : whereInBetweenExclusiveQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenExclusiveQueryFieldRangeValues
                            = whereInBetweenExclusiveQueryDetails.get(whereInBetweenExclusiveQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenExclusiveQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenExclusiveQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereQuery.put(whereInBetweenExclusiveQueryFieldName,
                                new BasicDBObject("$gt", greaterThanValue).append("$lt", lessThanValue));
                    }
                }
                isQueryConstructed = true;
            }
            if ((whereInBetweenInclusiveQueryDetails != null) && (whereInBetweenInclusiveQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenInclusiveQueryFieldName : whereInBetweenInclusiveQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenInclusiveQueryFieldRangeValues
                            = whereInBetweenInclusiveQueryDetails.get(whereInBetweenInclusiveQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenInclusiveQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenInclusiveQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereQuery.put(whereInBetweenInclusiveQueryFieldName,
                                new BasicDBObject("$gte", greaterThanValue).append("$lte", lessThanValue));
                    }
                }
                isQueryConstructed = true;
            }
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    HashMap<Object, Object> regexPatternAndOptions
                            = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    for (Object regexPattern : regexPatternAndOptions.keySet())
                    {
                        String options = (String) regexPatternAndOptions.get(regexPattern);
                        if (options != null)
                        {
                            for (int l = 0; l < options.length(); l++)
                            {
                                switch (options.charAt(l))
                                {
                                    case PatternMatchingOptions.CASE_INSENSITIVE:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.ALLOW_DOT_CHARACTER_TO_MATCH_ALL_CHARACTER:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.INCLUDE_ANCHOR:
                                    {
                                        //do not do anything 
                                        break;
                                    }
                                    default:
                                    {
                                        LOGGER.error("Invalid option is set");
                                        return null;
                                    }
                                }
                            }
                            whereQuery.put(whereLikeQueryFieldName,
                                    new BasicDBObject("$regex", regexPattern).append("$options", options));
                        }
                        else
                        {
                            whereQuery.put(whereLikeQueryFieldName,
                                    new BasicDBObject("$regex", regexPattern));
                        }
                    }
                }
                isQueryConstructed = true;
            }
            if (limit != null)
            {
                if (skipFirstFew != null)
                {
                    whereQueryResult = tableInstance.find(whereQuery).limit(limit.intValue()).skip(skipFirstFew.intValue());

                }
                else
                {
                    whereQueryResult = tableInstance.find(whereQuery).limit(limit.intValue());
                }
            }
            else
            {
                if (skipFirstFew != null)
                {
                    whereQueryResult = tableInstance.find(whereQuery).skip(skipFirstFew.intValue());

                }
                else
                {
                    whereQueryResult = tableInstance.find(whereQuery);
                }
            }
            ArrayList<String> findResultsArrayList;
            findResultsArrayList = new ArrayList<>();
            /*
            pushing into vector has an advantage
            vector is synchronized arraylist is not
             */
            while (whereQueryResult.hasNext())
            {
                String result = whereQueryResult.next().toString();
                findResultsArrayList.add(result);
            }
            return findResultsArrayList;
        }
    }

    /*obsolete collection warning due to use of vector*/
    public boolean deleteSpecificRowsWhereBasicQuery(String tableName,
            HashMap<String, Object> whereQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereQueryDetails != null) && (whereQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereQuery = new BasicDBObject();
                for (String keyWhereQuery : whereQueryDetails.keySet())
                {
                    Object valWhereQuery = whereQueryDetails.get(keyWhereQuery);
                    whereQuery.put(keyWhereQuery, valWhereQuery);
                }
                tableInstance.remove(whereQuery);
            }
            else
            {
                //if we do not mention anything in whereQueryDetails
                tableInstance.remove(new BasicDBObject());
            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereNotEqualToQuery(String tableName, HashMap<String, Object> whereNotEqualToQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereNotEqualToQueryDetails != null) && (whereNotEqualToQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereNotEqualToQuery = new BasicDBObject();
                for (String whereNotEqualToQueryKey : whereNotEqualToQueryDetails.keySet())
                {
                    Object whereNotEqualToQueryValue = whereNotEqualToQueryDetails.get(whereNotEqualToQueryKey);
                    whereNotEqualToQuery.put(whereNotEqualToQueryKey, new BasicDBObject("$ne", whereNotEqualToQueryValue));
                }
                tableInstance.remove(whereNotEqualToQuery);
            }
            else
            {
                tableInstance.remove(new BasicDBObject());
            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereInQuery(String tableName, HashMap<String, List> whereInQueryDetails)
    {
        /*
        SELECT column_name(s)
        FROM table_name
        WHERE column_name IN (value1, value2, ...);
         */
 /*
        Suppose, we face a query like
        SELECT COLUMN_NAME1,COLUMN_NAME2 FROM TABLE_NAME WHERE COLUMN_NAME1 
        IN (VALUE11,VALUE12) AND COLUMN_NMAE2 IN (VALUE21, VALUE22)
         */
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereInQueryDetails != null) && (whereInQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereInQuery = new BasicDBObject();
                for (String whereInQueryKey : whereInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereInQueryDetails.get(whereInQueryKey);
                    whereInQuery.put(whereInQueryKey, new BasicDBObject("$in", whereInQueryValueLists));
                }
                tableInstance.remove(whereInQuery);

            }
            else
            {
                tableInstance.remove(new BasicDBObject());

            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereNotInQuery(String tableName, HashMap<String, List> whereNotInQueryDetails)
    {
        /*
        SELECT column_name(s)
        FROM table_name
        WHERE column_name IN (value1, value2, ...);
         */
 /*
        Suppose, we face a query like
        SELECT COLUMN_NAME1,COLUMN_NAME2 FROM TABLE_NAME WHERE COLUMN_NAME1 
        IN (VALUE11,VALUE12) AND COLUMN_NMAE2 IN (VALUE21, VALUE22)
         */
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!(whereNotInQueryDetails == null) || (whereNotInQueryDetails.isEmpty()))
            {
                BasicDBObject whereNotInQuery = new BasicDBObject();
                for (String whereNotInQueryKey : whereNotInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereNotInQueryDetails.get(whereNotInQueryKey);
                    whereNotInQuery.put(whereNotInQueryKey, new BasicDBObject("$nin", whereInQueryValueLists));
                }
                tableInstance.remove(whereNotInQuery);
            }
            else
            {
                tableInstance.remove(new BasicDBObject());
            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereGreaterThanQuery(String tableName, HashMap<String, Object> whereGreaterThanQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!(whereGreaterThanQueryDetails == null) || (whereGreaterThanQueryDetails.isEmpty()))
            {
                BasicDBObject whereGreaterThanQuery = new BasicDBObject();
                for (String whereGreaterThanQueryKey : whereGreaterThanQueryDetails.keySet())
                {
                    Object whereGreaterThanQueryValue = whereGreaterThanQueryDetails.get(whereGreaterThanQueryKey);
                    whereGreaterThanQuery.put(whereGreaterThanQueryKey, new BasicDBObject("$gt", whereGreaterThanQueryValue));
                }
                tableInstance.remove(whereGreaterThanQuery);

            }
            else
            {

                tableInstance.remove(new BasicDBObject());

            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereGreaterThanOrEqualQuery(String tableName, HashMap<String, Object> whereGreaterThanOrEqualQueryDetails)
    {

        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereGreaterThanOrEqualQueryDetails != null) && (whereGreaterThanOrEqualQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereGreaterThanOrEqualQuery = new BasicDBObject();
                for (String whereGreaterThanOrEqualQueryKey : whereGreaterThanOrEqualQueryDetails.keySet())
                {
                    Object whereGreaterThanOrEqualQueryValue = whereGreaterThanOrEqualQueryDetails.get(whereGreaterThanOrEqualQueryKey);
                    whereGreaterThanOrEqualQuery.put(whereGreaterThanOrEqualQueryKey, new BasicDBObject("$gte", whereGreaterThanOrEqualQueryValue));
                }
                tableInstance.remove(whereGreaterThanOrEqualQuery);
            }
            else
            {
                tableInstance.remove(new BasicDBObject());
            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereLessThanQuery(String tableName, HashMap<String, Object> whereLessThanQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereLessThanQueryDetails != null) && (whereLessThanQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereLessThanQuery = new BasicDBObject();
                for (String whereLessThanQueryKey : whereLessThanQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanQueryDetails.get(whereLessThanQueryKey);
                    whereLessThanQuery.put(whereLessThanQueryKey, new BasicDBObject("$lt", whereLessThanQueryValue));
                }
                synchronized (this)
                {
                    tableInstance.remove(whereLessThanQuery);
                }
            }
            else
            {
                synchronized (this)
                {
                    tableInstance.remove(new BasicDBObject());
                }
            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereLessThanOrEqualQuery(String tableName, HashMap<String, Object> whereLessThanOrEqualQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereLessThanOrEqualQueryDetails != null) && (whereLessThanOrEqualQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereLessThanOrEqualQuery = new BasicDBObject();
                for (String whereLessThanOrEqualQueryKey : whereLessThanOrEqualQueryDetails.keySet())
                {
                    Object whereLessThanOrEqualQueryValue = whereLessThanOrEqualQueryDetails.get(whereLessThanOrEqualQueryKey);
                    whereLessThanOrEqualQuery.put(whereLessThanOrEqualQueryKey, new BasicDBObject("$lte", whereLessThanOrEqualQueryValue));
                }
                tableInstance.remove(whereLessThanOrEqualQuery);
            }
            else
            {
                tableInstance.remove(new BasicDBObject());
            }
            return true;
        }
    }

    public boolean deleteSpecificRowsInBetweenExclusiveQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereInBetweenQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereInBetweenQueryDetails != null) && (whereInBetweenQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereInBetweenQuery = new BasicDBObject();
                for (String whereInBetweenQueryFieldName : whereInBetweenQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenQueryFieldRangeValues
                            = whereInBetweenQueryDetails.get(whereInBetweenQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereInBetweenQuery.put(whereInBetweenQueryFieldName,
                                new BasicDBObject("$gt", greaterThanValue).append("$lt", lessThanValue));
                    }
                }
                tableInstance.remove(whereInBetweenQuery);
            }
            else
            {
                tableInstance.remove(new BasicDBList());

            }
            return true;
        }
    }

    public boolean deteteSpecificRowsInBetweenInclusiveQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereInBetweenQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereInBetweenQueryDetails != null) && (whereInBetweenQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereInBetweenQuery = new BasicDBObject();
                for (String whereInBetweenQueryFieldName : whereInBetweenQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenQueryFieldRangeValues
                            = whereInBetweenQueryDetails.get(whereInBetweenQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereInBetweenQuery.put(whereInBetweenQueryFieldName,
                                new BasicDBObject("$gte", greaterThanValue).append("$lte", lessThanValue));
                    }
                }
                tableInstance.remove(whereInBetweenQuery);

            }
            else
            {
                tableInstance.remove(new BasicDBObject());

            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereLikeQueryCaseSensitive(String tableName,
            HashMap<String, Object> whereLikeQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereLikeQuery = new BasicDBObject();
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    Object regexPattern = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    whereLikeQuery.put(whereLikeQueryFieldName,
                            new BasicDBObject("$regex", regexPattern));

                }
                tableInstance.remove(whereLikeQuery);

            }
            else
            {
                tableInstance.remove(new BasicDBObject());

            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereLikeQueryCaseInSensitive(String tableName,
            HashMap<String, Object> whereLikeQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereLikeQuery = new BasicDBObject();
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    Object regexPattern = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    whereLikeQuery.put(whereLikeQueryFieldName,
                            new BasicDBObject("$regex", regexPattern).append("$options", "i"));

                }
                tableInstance.remove(whereLikeQuery);

            }
            else
            {
                tableInstance.remove(new BasicDBObject());

            }
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereLikeQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereLikeQueryDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            BasicDBObject whereLikeQuery = new BasicDBObject();
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    HashMap<Object, Object> regexPatternAndOptions
                            = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    for (Object regexPattern : regexPatternAndOptions.keySet())
                    {
                        String options = (String) regexPatternAndOptions.get(regexPattern);
                        if (options != null)
                        {
                            for (int l = 0; l < options.length(); l++)
                            {
                                switch (options.charAt(l))
                                {
                                    case PatternMatchingOptions.CASE_INSENSITIVE:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.ALLOW_DOT_CHARACTER_TO_MATCH_ALL_CHARACTER:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.INCLUDE_ANCHOR:
                                    {
                                        //do not do anything 
                                        break;
                                    }
                                    default:
                                    {
                                        LOGGER.error("Invalid option is set");
                                        return false;
                                    }
                                }
                            }
                            whereLikeQuery.put(whereLikeQueryFieldName,
                                    new BasicDBObject("$regex", regexPattern).append("$options", options));
                        }
                        else
                        {
                            whereLikeQuery.put(whereLikeQueryFieldName, new BasicDBObject("$regex", regexPattern));
                        }
                    }
                }
            }
            tableInstance.remove(whereLikeQuery);
            return true;
        }
    }

    public boolean deleteSpecificRowsWhereQuery(String tableName,
            HashMap<String, Object> whereBasicQueryDetails,
            HashMap<String, Object> whereGreaterThanOrEqualQueryDetails,
            HashMap<String, Object> whereGreaterThanQueryDetails,
            HashMap<String, Object> whereLessThanQueryDetails,
            HashMap<String, Object> whereLessThanOrEqualQueryDetails,
            HashMap<String, List> whereInQueryDetails,
            HashMap<String, List> whereNotInQueryDetails,
            HashMap<String, Object> whereNotEqualToQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereInBetweenExclusiveQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereInBetweenInclusiveQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereLikeQueryDetails
    )
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            DBObject whereQuery = new BasicDBObject();
            boolean isQueryConstructed = false;
            if (whereBasicQueryDetails != null && whereBasicQueryDetails.isEmpty() != true)
            {
                for (String whereBasicQueryFieldName : whereBasicQueryDetails.keySet())
                {
                    Object whereBasicQueryValue = whereBasicQueryDetails.get(whereBasicQueryFieldName);
                    whereQuery.put(whereBasicQueryFieldName, whereBasicQueryValue);
                }
                isQueryConstructed = true;
            }
            if ((whereNotEqualToQueryDetails != null) && (whereNotEqualToQueryDetails.isEmpty() == false))
            {
                for (String whereNotEqualToQueryKey : whereNotEqualToQueryDetails.keySet())
                {
                    Object whereNotEqualToQueryValue = whereNotEqualToQueryDetails.get(whereNotEqualToQueryKey);
                    whereQuery.put(whereNotEqualToQueryKey, new BasicDBObject("$ne", whereNotEqualToQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereInQueryDetails != null) && (whereInQueryDetails.isEmpty() == false))
            {
                for (String whereInQueryKey : whereInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereInQueryDetails.get(whereInQueryKey);
                    whereQuery.put(whereInQueryKey, new BasicDBObject("$in", whereInQueryValueLists));
                }
                isQueryConstructed = true;
            }
            if ((whereNotInQueryDetails != null) && (whereNotInQueryDetails.isEmpty() == false))
            {
                for (String whereNotInQueryKey : whereNotInQueryDetails.keySet())
                {
                    List whereNotInQueryValueLists = whereNotInQueryDetails.get(whereNotInQueryKey);
                    whereQuery.put(whereNotInQueryKey, new BasicDBObject("$nin", whereNotInQueryValueLists));
                }
                isQueryConstructed = true;
            }
            if ((whereGreaterThanQueryDetails != null) && (whereGreaterThanQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanQueryKey : whereGreaterThanQueryDetails.keySet())
                {
                    Object whereGreaterThanQueryValue = whereGreaterThanQueryDetails.get(whereGreaterThanQueryKey);
                    whereQuery.put(whereGreaterThanQueryKey, new BasicDBObject("$gt", whereGreaterThanQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereGreaterThanOrEqualQueryDetails != null) && (whereGreaterThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanOrEqualQueryKey : whereGreaterThanOrEqualQueryDetails.keySet())
                {
                    Object whereGreaterThanOrEqualQueryValue = whereGreaterThanOrEqualQueryDetails.get(whereGreaterThanOrEqualQueryKey);
                    whereQuery.put(whereGreaterThanOrEqualQueryKey, new BasicDBObject("$gte", whereGreaterThanOrEqualQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereLessThanQueryDetails != null) && (whereLessThanQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanQueryKey : whereLessThanQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanQueryDetails.get(whereLessThanQueryKey);
                    whereQuery.put(whereLessThanQueryKey, new BasicDBObject("$lt", whereLessThanQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereLessThanOrEqualQueryDetails != null) && (whereLessThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanOrEqualQueryKey : whereLessThanOrEqualQueryDetails.keySet())
                {
                    Object whereLessThanOrEqualQueryValue = whereLessThanOrEqualQueryDetails.get(whereLessThanOrEqualQueryKey);
                    whereQuery.put(whereLessThanOrEqualQueryKey, new BasicDBObject("$lte", whereLessThanOrEqualQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereInBetweenExclusiveQueryDetails != null) && (whereInBetweenExclusiveQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenExclusiveQueryFieldName : whereInBetweenExclusiveQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenExclusiveQueryFieldRangeValues
                            = whereInBetweenExclusiveQueryDetails.get(whereInBetweenExclusiveQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenExclusiveQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenExclusiveQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereQuery.put(whereInBetweenExclusiveQueryFieldName,
                                new BasicDBObject("$gt", greaterThanValue).append("$lt", lessThanValue));
                    }
                }
                isQueryConstructed = true;
            }
            if ((whereInBetweenInclusiveQueryDetails != null) && (whereInBetweenInclusiveQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenInclusiveQueryFieldName : whereInBetweenInclusiveQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenInclusiveQueryFieldRangeValues
                            = whereInBetweenInclusiveQueryDetails.get(whereInBetweenInclusiveQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenInclusiveQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenInclusiveQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereQuery.put(whereInBetweenInclusiveQueryFieldName,
                                new BasicDBObject("$gte", greaterThanValue).append("$lte", lessThanValue));
                    }
                }
                isQueryConstructed = true;
            }
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    HashMap<Object, Object> regexPatternAndOptions
                            = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    for (Object regexPattern : regexPatternAndOptions.keySet())
                    {
                        String options = (String) regexPatternAndOptions.get(regexPattern);
                        if (options != null)
                        {
                            for (int l = 0; l < options.length(); l++)
                            {
                                switch (options.charAt(l))
                                {
                                    case PatternMatchingOptions.CASE_INSENSITIVE:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.ALLOW_DOT_CHARACTER_TO_MATCH_ALL_CHARACTER:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.INCLUDE_ANCHOR:
                                    {
                                        //do not do anything 
                                        break;
                                    }
                                    default:
                                    {
                                        LOGGER.error("Invalid option is set");
                                        return false;
                                    }
                                }
                            }
                            whereQuery.put(whereLikeQueryFieldName,
                                    new BasicDBObject("$regex", regexPattern).append("$options", options));
                        }
                        else
                        {
                            whereQuery.put(whereLikeQueryFieldName, new BasicDBObject("$regex", regexPattern));
                        }
                    }
                }
                isQueryConstructed = true;
            }
            tableInstance.remove(whereQuery);
            return true;
        }
    }

    public boolean drop_a_table(String tableName)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
        if (tableInstance == null)
        {
            tableInstance = databaseInstance.getCollection(tableName);
            CollectionNameInstanceMapping.put(tableName, tableInstance);
        }
        DBObject deleteAllDocs = new BasicDBObject();
        tableInstance.remove(deleteAllDocs);
        tableInstance.drop();
        collectionNames.remove(tableName);
        CollectionNameInstanceMapping.remove(tableName);
        LOGGER.info("The table is deleted successfully");
        return true;
        //successfully write it
    }

    //again synchronised is used to ensure that inserttion is done in right table
    /*obsolete collection warning due to use of vector*/
    public boolean updateSpecificRowsWhereBasicQuery(String tableName,
            HashMap<String, Object> whereQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            if (updateDetails == null)
            {
                LOGGER.error("No details of update is found");
                return false;
            }
            if (updateDetails.isEmpty() == true)
            {
                LOGGER.error("No details of update is found");
                return false;
            }
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String keyUpdateQuery : updateDetails.keySet())
            {
                Object valUpdateQuery = updateDetails.get(keyUpdateQuery);
                //LOGGER.debug("The update field name: " + keyUpdateQuery);
                //LOGGER.debug("the update field value: " + valUpdateQuery);
                updateQuery.append("$set", new BasicDBObject().append(keyUpdateQuery, valUpdateQuery));
                //without using set a new document will replace the old one
                /*
                Suppose, the initial dataset is like:
                { "_id" : { "$oid" : "id"} , "hosting" : "hostA" , "type" : "vps" , "clients" : 1000}
                { "_id" : { "$oid" : "id"} , "hosting" : "hostB" , "type" : "dedicated server" , "clients" : 100}
                { "_id" : { "$oid" : "id"} , "hosting" : "hostC" , "type" : "vps" , "clients" : 900}
                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("clients", 110);
                BasicDBObject searchQuery = new BasicDBObject().append("hosting", "hostB");
                collection.update(searchQuery, newDocument);
                It will actually generate the output:
                { "_id" : { "$oid" : "id"} , "hosting" : "hostA" , "type" : "vps" , "clients" : 1000}
                { "_id" : { "$oid" : "id"} , "clients" : 110}
                { "_id" : { "$oid" : "id"} , "hosting" : "hostC" , "type" : "vps" , "clients" : 900}
                 */
            }
            WriteResult updateResult = null;
            BasicDBObject whereQuery = new BasicDBObject();
            if ((whereQueryDetails != null) && (whereQueryDetails.isEmpty() == false))
            {
                for (String keyWhereQuery : whereQueryDetails.keySet())
                {
                    Object valWhereQuery = whereQueryDetails.get(keyWhereQuery);
                    //LOGGER.info("The where query field name: " + keyWhereQuery);
                    //LOGGER.info("the where query field value: " + valWhereQuery);
                    whereQuery.put(keyWhereQuery, valWhereQuery);
                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereQuery, updateQuery);
                //multiple row updatation
                //if we wont use updateMulti it will update single row
            }
            catch (Exception e)
            {
                LOGGER.error("The update could not be successfully done");
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("The update operation might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereNotEqualToQuery(String tableName,
            HashMap<String, Object> whereNotEqualToQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No details of update is found");
                return false;
            }
            DBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            if ((whereNotEqualToQueryDetails != null) && (whereNotEqualToQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereNotEqualToQuery = new BasicDBObject();
                for (String whereNotEqualToQueryKey : whereNotEqualToQueryDetails.keySet())
                {
                    Object whereNotEqualToQueryValue = whereNotEqualToQueryDetails.get(whereNotEqualToQueryKey);
                    whereNotEqualToQuery.put(whereNotEqualToQueryKey, new BasicDBObject("$ne", whereNotEqualToQueryValue));
                }
                try
                {
                    updateResult = tableInstance.updateMulti(whereNotEqualToQuery, updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("The update could not be successful: " + e);
                    return false;
                }
            }
            else
            {
                try
                {
                    updateResult = tableInstance.updateMulti(new BasicDBObject(), updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("The update could not be successful");
                    return false;
                }
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereInQuery(String tableName, HashMap<String, List> whereInQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        /*
        SELECT column_name(s)
        FROM table_name
        WHERE column_name IN (value1, value2, ...);
         */
 /*
        Suppose, we face a query like
        SELECT COLUMN_NAME1,COLUMN_NAME2 FROM TABLE_NAME WHERE COLUMN_NAME1 
        IN (VALUE11,VALUE12) AND COLUMN_NMAE2 IN (VALUE21, VALUE22)
         */
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details is found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                LOGGER.debug("The updateQueryKey " + updateQueryKey);
                LOGGER.debug("The updateQueryValue " + updateQueryValue);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            if ((whereInQueryDetails != null) && (whereInQueryDetails.isEmpty() == false))
            {
                BasicDBObject whereInQuery = new BasicDBObject();
                for (String whereInQueryKey : whereInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereInQueryDetails.get(whereInQueryKey);
                    whereInQuery.put(whereInQueryKey, new BasicDBObject("$in", whereInQueryValueLists));
                }
                try
                {
                    updateResult = tableInstance.updateMulti(whereInQuery, updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("The update is not successful " + e);
                    return false;
                }
            }
            else
            {
                try
                {
                    updateResult = tableInstance.updateMulti(new BasicDBObject(), updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("The update is not successful " + e);
                    return false;
                }

            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereNotInQuery(String tableName,
            HashMap<String, List> whereNotInQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        /*
        SELECT column_name(s)
        FROM table_name
        WHERE column_name IN (value1, value2, ...);
         */
 /*
        Suppose, we face a query like
        SELECT COLUMN_NAME1,COLUMN_NAME2 FROM TABLE_NAME WHERE COLUMN_NAME1 
        IN (VALUE11,VALUE12) AND COLUMN_NMAE2 IN (VALUE21, VALUE22)
         */
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            BasicDBObject whereNotInQuery = new BasicDBObject();
            if ((whereNotInQueryDetails != null) && (whereNotInQueryDetails.isEmpty() == false))
            {
                for (String whereNotInQueryKey : whereNotInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereNotInQueryDetails.get(whereNotInQueryKey);
                    whereNotInQuery.put(whereNotInQueryKey, new BasicDBObject("$nin", whereInQueryValueLists));
                }
                try
                {
                    updateResult = tableInstance.updateMulti(whereNotInQuery, updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("The update was not successful: " + e);
                    return false;
                }
            }
            else
            {
                try
                {
                    updateResult = tableInstance.updateMulti(whereNotInQuery, updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("The update was not successful: " + e);
                    return false;
                }
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereGreaterThanQuery(String tableName,
            HashMap<String, Object> whereGreaterThanQueryDetails,
            HashMap<String, Object> updateDetails)
    {

        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (updateDetails == null || updateDetails.isEmpty())
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            BasicDBObject whereGreaterThanQuery = new BasicDBObject();
            WriteResult updateResult = null;
            if ((whereGreaterThanQueryDetails != null) && (whereGreaterThanQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanQueryKey : whereGreaterThanQueryDetails.keySet())
                {
                    Object whereGreaterThanQueryValue = whereGreaterThanQueryDetails.get(whereGreaterThanQueryKey);
                    whereGreaterThanQuery.put(whereGreaterThanQueryKey, new BasicDBObject("$gt", whereGreaterThanQueryValue));
                }
                try
                {
                    updateResult = tableInstance.updateMulti(whereGreaterThanQuery, updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("Update is unsuccessful " + e);
                    return false;
                }
            }
            else
            {
                try
                {
                    updateResult = tableInstance.updateMulti(whereGreaterThanQuery, updateQuery);
                }
                catch (Exception e)
                {
                    LOGGER.error("Update is unsuccessful " + e);
                    return false;
                }
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereGreaterThanOrEqualQuery(String tableName,
            HashMap<String, Object> whereGreaterThanOrEqualQueryDetails,
            HashMap<String, Object> updateDetails)
    {

        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            BasicDBObject whereGreaterThanOrEqualQuery = new BasicDBObject();
            WriteResult updateResult = null;
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            if ((whereGreaterThanOrEqualQueryDetails != null) && (whereGreaterThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanOrEqualQueryKey : whereGreaterThanOrEqualQueryDetails.keySet())
                {
                    Object whereGreaterThanOrEqualQueryValue = whereGreaterThanOrEqualQueryDetails.get(whereGreaterThanOrEqualQueryKey);
                    whereGreaterThanOrEqualQuery.put(whereGreaterThanOrEqualQueryKey, new BasicDBObject("$gte", whereGreaterThanOrEqualQueryValue));
                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereGreaterThanOrEqualQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update process was unsuccessful " + e);
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereLessThanQuery(String tableName,
            HashMap<String, Object> whereLessThanQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            BasicDBObject whereLessThanQuery = new BasicDBObject();
            WriteResult updateResult = null;
            if ((whereLessThanQueryDetails != null) && (whereLessThanQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanQueryKey : whereLessThanQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanQueryDetails.get(whereLessThanQueryKey);
                    whereLessThanQuery.put(whereLessThanQueryKey, new BasicDBObject("$lt", whereLessThanQueryValue));
                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereLessThanQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update process was unsucceesful " + e);
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereLessThanOrEqualQuery(String tableName,
            HashMap<String, Object> whereLessThanOrEqualQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            BasicDBObject whereLessThanOrEqualQuery = new BasicDBObject();
            if ((whereLessThanOrEqualQueryDetails != null) && (whereLessThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanOrEqualQueryKey : whereLessThanOrEqualQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanOrEqualQueryDetails.get(whereLessThanOrEqualQueryKey);
                    whereLessThanOrEqualQuery.put(whereLessThanOrEqualQueryKey, new BasicDBObject("$lte", whereLessThanQueryValue));
                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereLessThanOrEqualQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update process is unsuccessful");
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsInBetweenExclusiveQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereInBetweenQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            BasicDBObject whereInBetweenQuery = new BasicDBObject();
            WriteResult updateResult = null;
            if ((whereInBetweenQueryDetails != null) && (whereInBetweenQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenQueryFieldName : whereInBetweenQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenQueryFieldRangeValues
                            = whereInBetweenQueryDetails.get(whereInBetweenQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereInBetweenQuery.put(whereInBetweenQueryFieldName,
                                new BasicDBObject("$gt", greaterThanValue).append("$lt", lessThanValue));
                    }
                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereInBetweenQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update was not successful " + e);
                return false;
            }

            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsInBetweenInclusiveQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereInBetweenQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            BasicDBObject whereInBetweenQuery = new BasicDBObject();
            if ((whereInBetweenQueryDetails != null) && (whereInBetweenQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenQueryFieldName : whereInBetweenQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenQueryFieldRangeValues
                            = whereInBetweenQueryDetails.get(whereInBetweenQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereInBetweenQuery.put(whereInBetweenQueryFieldName,
                                new BasicDBObject("$gte", greaterThanValue).append("$lte", lessThanValue));
                    }
                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereInBetweenQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("Update was not unsuccessful " + e);
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereLikeQueryCaseSensitive(String tableName,
            HashMap<String, Object> whereLikeQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            BasicDBObject whereLikeQuery = new BasicDBObject();
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    Object regexPattern = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    whereLikeQuery.put(whereLikeQueryFieldName,
                            new BasicDBObject("$regex", regexPattern));

                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereLikeQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update is unsuccessful " + e);
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereLikeQueryCaseInSensitive(String tableName,
            HashMap<String, Object> whereLikeQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            return false;
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            BasicDBObject whereLikeQuery = new BasicDBObject();
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    Object regexPattern = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    whereLikeQuery.put(whereLikeQueryFieldName,
                            new BasicDBObject("$regex", regexPattern).append("$options", "i"));
                }
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereLikeQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update is unsuccessful");
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereLikeQuery(String tableName,
            HashMap<String, HashMap<Object, Object>> whereLikeQueryDetails,
            HashMap<String, Object> updateDetails)
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            BasicDBObject whereLikeQuery = new BasicDBObject();
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    HashMap<Object, Object> regexPatternAndOptions
                            = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    for (Object regexPattern : regexPatternAndOptions.keySet())
                    {
                        String options = (String) regexPatternAndOptions.get(regexPattern);
                        if (options != null)
                        {
                            for (int l = 0; l < options.length(); l++)
                            {
                                switch (options.charAt(l))
                                {
                                    case PatternMatchingOptions.CASE_INSENSITIVE:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.ALLOW_DOT_CHARACTER_TO_MATCH_ALL_CHARACTER:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.INCLUDE_ANCHOR:
                                    {
                                        //do not do anything 
                                        break;
                                    }
                                    default:
                                    {
                                        LOGGER.error("Invalid option is set");
                                        return false;
                                    }
                                }
                            }
                            whereLikeQuery.put(whereLikeQueryFieldName,
                                    new BasicDBObject("$regex", regexPattern).append("$options", options));
                        }
                        else
                        {
                            whereLikeQuery.put(whereLikeQueryFieldName, new BasicDBObject("$regex", regexPattern));
                        }
                    }
                }
            }

            try
            {
                updateResult = tableInstance.updateMulti(whereLikeQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update is unsuccessful " + e);
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public boolean updateSpecificRowsWhereQuery(String tableName,
            HashMap<String, Object> whereBasicQueryDetails,
            HashMap<String, Object> whereGreaterThanOrEqualQueryDetails,
            HashMap<String, Object> whereGreaterThanQueryDetails,
            HashMap<String, Object> whereLessThanQueryDetails,
            HashMap<String, Object> whereLessThanOrEqualQueryDetails,
            HashMap<String, List> whereInQueryDetails,
            HashMap<String, List> whereNotInQueryDetails,
            HashMap<String, Object> whereNotEqualToQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereInBetweenExclusiveQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereInBetweenInclusiveQueryDetails,
            HashMap<String, HashMap<Object, Object>> whereLikeQueryDetails,
            HashMap<String, Object> updateDetails
    )
    {
        if (tableName == null)
        {
            LOGGER.error("The tableName passed to the function is null");
            return false;
        }
        if (tableName.equals(""))
        {
            LOGGER.error("The tableName passed to the function is an empty string");
            return false;
        }
        //initial sanity checking is done
        if (!collectionNames.contains(tableName))
        {
            LOGGER.error("The table does not exist into database: " + databaseName);
            if (databaseInstance.collectionExists(tableName))
            {
                collectionNames.add(tableName);
            }
            else
            {
                return false;
            }
        }
        synchronized (this)
        {
            DBCollection tableInstance = CollectionNameInstanceMapping.get(tableName);
            if (tableInstance == null)
            {
                tableInstance = databaseInstance.getCollection(tableName);
                CollectionNameInstanceMapping.put(tableName, tableInstance);
            }
            if (!((updateDetails != null) && (updateDetails.isEmpty() == false)))
            {
                LOGGER.error("No update details were found");
                return false;
            }
            BasicDBObject updateQuery = new BasicDBObject();
            for (String updateQueryKey : updateDetails.keySet())
            {
                Object updateQueryValue = updateDetails.get(updateQueryKey);
                updateQuery.put("$set", new BasicDBObject().append(updateQueryKey, updateQueryValue));
            }
            WriteResult updateResult = null;
            DBObject whereQuery = new BasicDBObject();
            boolean isQueryConstructed = false;
            if (whereBasicQueryDetails != null && whereBasicQueryDetails.isEmpty() != true)
            {
                for (String whereBasicQueryFieldName : whereBasicQueryDetails.keySet())
                {
                    Object whereBasicQueryValue = whereBasicQueryDetails.get(whereBasicQueryFieldName);
                    whereQuery.put(whereBasicQueryFieldName, whereBasicQueryValue);
                }
                isQueryConstructed = true;
            }
            if ((whereNotEqualToQueryDetails != null) && (whereNotEqualToQueryDetails.isEmpty() == false))
            {
                for (String whereNotEqualToQueryKey : whereNotEqualToQueryDetails.keySet())
                {
                    Object whereNotEqualToQueryValue = whereNotEqualToQueryDetails.get(whereNotEqualToQueryKey);
                    whereQuery.put(whereNotEqualToQueryKey, new BasicDBObject("$ne", whereNotEqualToQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereInQueryDetails != null) && (whereInQueryDetails.isEmpty() == false))
            {
                for (String whereInQueryKey : whereInQueryDetails.keySet())
                {
                    List whereInQueryValueLists = whereInQueryDetails.get(whereInQueryKey);
                    whereQuery.put(whereInQueryKey, new BasicDBObject("$in", whereInQueryValueLists));
                }
                isQueryConstructed = true;
            }
            if ((whereNotInQueryDetails != null) && (whereNotInQueryDetails.isEmpty() == false))
            {
                for (String whereNotInQueryKey : whereNotInQueryDetails.keySet())
                {
                    List whereNotInQueryValueLists = whereNotInQueryDetails.get(whereNotInQueryKey);
                    whereQuery.put(whereNotInQueryKey, new BasicDBObject("$nin", whereNotInQueryValueLists));
                }
                isQueryConstructed = true;
            }
            if ((whereGreaterThanQueryDetails != null) && (whereGreaterThanQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanQueryKey : whereGreaterThanQueryDetails.keySet())
                {
                    Object whereGreaterThanQueryValue = whereGreaterThanQueryDetails.get(whereGreaterThanQueryKey);
                    whereQuery.put(whereGreaterThanQueryKey, new BasicDBObject("$gt", whereGreaterThanQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereGreaterThanOrEqualQueryDetails != null) && (whereGreaterThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereGreaterThanOrEqualQueryKey : whereGreaterThanOrEqualQueryDetails.keySet())
                {
                    Object whereGreaterThanOrEqualQueryValue = whereGreaterThanOrEqualQueryDetails.get(whereGreaterThanOrEqualQueryKey);
                    whereQuery.put(whereGreaterThanOrEqualQueryKey, new BasicDBObject("$gte", whereGreaterThanOrEqualQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereLessThanQueryDetails != null) && (whereLessThanQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanQueryKey : whereLessThanQueryDetails.keySet())
                {
                    Object whereLessThanQueryValue = whereLessThanQueryDetails.get(whereLessThanQueryKey);
                    whereQuery.put(whereLessThanQueryKey, new BasicDBObject("$lt", whereLessThanQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereLessThanOrEqualQueryDetails != null) && (whereLessThanOrEqualQueryDetails.isEmpty() == false))
            {
                for (String whereLessThanOrEqualQueryKey : whereLessThanOrEqualQueryDetails.keySet())
                {
                    Object whereLessThanOrEqualQueryValue = whereLessThanOrEqualQueryDetails.get(whereLessThanOrEqualQueryKey);
                    whereQuery.put(whereLessThanOrEqualQueryKey, new BasicDBObject("$lte", whereLessThanOrEqualQueryValue));
                }
                isQueryConstructed = true;
            }
            if ((whereInBetweenExclusiveQueryDetails != null) && (whereInBetweenExclusiveQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenExclusiveQueryFieldName : whereInBetweenExclusiveQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenExclusiveQueryFieldRangeValues
                            = whereInBetweenExclusiveQueryDetails.get(whereInBetweenExclusiveQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenExclusiveQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenExclusiveQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereQuery.put(whereInBetweenExclusiveQueryFieldName,
                                new BasicDBObject("$gt", greaterThanValue).append("$lt", lessThanValue));
                    }
                }
                isQueryConstructed = true;
            }
            if ((whereInBetweenInclusiveQueryDetails != null) && (whereInBetweenInclusiveQueryDetails.isEmpty() == false))
            {
                for (String whereInBetweenInclusiveQueryFieldName : whereInBetweenInclusiveQueryDetails.keySet())
                {
                    HashMap<Object, Object> whereInBetweenInclusiveQueryFieldRangeValues
                            = whereInBetweenInclusiveQueryDetails.get(whereInBetweenInclusiveQueryFieldName);
                    for (Object greaterThanValue : whereInBetweenInclusiveQueryFieldRangeValues.keySet())
                    {
                        Object lessThanValue = whereInBetweenInclusiveQueryFieldRangeValues.get(greaterThanValue);
                        LOGGER.info("Range value: " + greaterThanValue + "-" + lessThanValue);
                        whereQuery.put(whereInBetweenInclusiveQueryFieldName,
                                new BasicDBObject("$gte", greaterThanValue).append("$lte", lessThanValue));
                    }
                }
                isQueryConstructed = true;
            }
            if ((whereLikeQueryDetails != null) && (whereLikeQueryDetails.isEmpty() == false))
            {
                for (String whereLikeQueryFieldName : whereLikeQueryDetails.keySet())
                {
                    HashMap<Object, Object> regexPatternAndOptions
                            = whereLikeQueryDetails.get(whereLikeQueryFieldName);
                    for (Object regexPattern : regexPatternAndOptions.keySet())
                    {
                        String options = (String) regexPatternAndOptions.get(regexPattern);
                        if (options != null)
                        {
                            for (int l = 0; l < options.length(); l++)
                            {
                                switch (options.charAt(l))
                                {
                                    case PatternMatchingOptions.CASE_INSENSITIVE:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.ALLOW_DOT_CHARACTER_TO_MATCH_ALL_CHARACTER:
                                    {
                                        //do not do anything
                                        break;
                                    }
                                    case PatternMatchingOptions.INCLUDE_ANCHOR:
                                    {
                                        //do not do anything 
                                        break;
                                    }
                                    default:
                                    {
                                        LOGGER.error("Invalid option is set");
                                        return false;
                                    }
                                }
                            }
                            whereQuery.put(whereLikeQueryFieldName,
                                    new BasicDBObject("$regex", regexPattern).append("$options", options));
                        }
                        else
                        {
                            whereQuery.put(whereLikeQueryFieldName, new BasicDBObject("$regex", regexPattern));
                        }
                    }
                }
                isQueryConstructed = true;
            }
            try
            {
                updateResult = tableInstance.updateMulti(whereQuery, updateQuery);
            }
            catch (Exception e)
            {
                LOGGER.error("The update was unsuccessful");
                return false;
            }
            if (updateResult != null)
            {
                if (updateResult.getLastError().getErrorMessage() != null)
                {
                    LOGGER.error("The update process invokes some error: "
                            + updateResult.getLastError().getErrorMessage());
                    return false;
                }
            }
            else
            {
                LOGGER.error("Update process might not be performed");
                return false;
            }
            return true;
        }
    }

    public void destroyMongoDBHandler()
    {
        if (mongoClientInstance != null)
        {
            mongoClientInstance.close();
            //this is to close the connection
        }
    }
}
