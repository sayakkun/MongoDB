package newmongodbaccessor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
Author : Sayak
Date: 17/04/2017
 */
public class MongoDBConfiguration
{

    private static final Logger LOGGER = LogManager.getLogger(MongoDBConfiguration.class.getName());
    /*
        Why LogManager.getLogger(MongoDBConfiguration.class.getName())
        Suppose, in future, you want to set different log levels for this particular file
     */
    private static MongoDBConfiguration mongoDBConfigurationInstance = null;
    private static final long RELOAD_TIME_DIFF = 10;
    /*minimum difference between two realod*/
    private static final String HOST_IP = "hostIP";
    private static final String HOST_PORT = "hostPort";
    private static final String DATABASE_NAME = "dataBaseName";
    private static final String USER_NAME = "userName";
    private static final String PASSWORD = "passWord";
    private String hostIP;
    private short hostPort;
    private String dataBaseName;
    private String userName;
    private String password;
    private static boolean setHostIP = false;
    private static boolean setHostPort = false;
    private static boolean setDataBaseName = false;
    private static boolean setUserName = false;
    private static boolean setPassword = false;
    private static boolean alreadyLoadedFlag = false;
    private static long lastAccessTime = 0;
    private MongoDBConfiguration()
    {
        //only one instance of MongoDBConfiguration is possible
        hostIP = null;
        hostPort = 0;
        dataBaseName = null;
        userName = null;
        password = null;
    }

    public static MongoDBConfiguration getMongoDBConfigurationInstance()
    {
        if (mongoDBConfigurationInstance == null)
        {
            mongoDBConfigurationInstance = new MongoDBConfiguration();
            return mongoDBConfigurationInstance;
        }
        else
        {
            return mongoDBConfigurationInstance;
        }
    }

    private boolean loadConfig()
    {
        File databaseConfigFileHandler;
        FileReader databaseConfigFileReader = null;
        BufferedReader br = null;
        String fileName = "MongoDBConfiguration.cfg";
        /*
            Now, MongoDBConfiguration.cfg is the configuration filename
         */
        //convention is to handle the configuration file with .cfg extension
        try
        {
            databaseConfigFileHandler = new File(fileName);
            /*System.out.println("The absolute path for the file: "+
                    databaseConfigFileHandler.getAbsolutePath());*/
            if (!databaseConfigFileHandler.exists())
            {
                LOGGER.fatal("The file " + fileName + " does not exist in "
                        + "current path");
                return false;
            }
            databaseConfigFileReader = new FileReader(databaseConfigFileHandler);
            br = new BufferedReader(databaseConfigFileReader);
            String Line;
            String singleLineComment = "//.*";
            //singleLineComment regex is already tested
            String singleLineComment2 = "/\\*.*\\*/";
            //this matching must be checked before checking the text against multiLineCommentBeginning 
            String multiLineCommentBeginning = "/\\*.*";
            String multiLineCommentEnding = ".*\\*/";
            String conflictingComment = "//.*\\*/";
            String configLine = "\\s*([A-Za-z0-9]+)\\s*=\\s*([A-Za-z0-9\\.]+)\\s*";
            String configLine2 = "\\s*([A-Za-z0-9]+)\\s*=\\s*([A-Za-z0-9\\.]+)\\s*//.*";
            String configLine3 = "\\s*(/\\*.*\\*/)*\\s*([A-Za-z0-9]+)\\s*(/\\*.*"
                    + "\\*/)*\\s*=\\s*(/\\*.*\\*/)*\\s*([A-Za-z0-9]+)\\s*(/\\*.*\\*/)*";
            //upto this we allowed the first single line comment pattern at the end
            String noLine = "|\\s+";
            //since, it is matched against a line fetched by BufferedReader 
            //class it won't cause problem
            /*
                Why it is a conflictingCommentPattern since, 
                it can be treated either as a singleLineComment 
                or multiLineCommentEnding
             */
            //System.out.println("All the strings are written");
            Pattern singleLineCommentPat = Pattern.compile(singleLineComment);
            Pattern singleLineComment2Pat = Pattern.compile(singleLineComment2);
            Pattern multiLineCommentBeginningPat = Pattern.compile(multiLineCommentBeginning);
            Pattern conflictingCommentPat = Pattern.compile(conflictingComment);
            Pattern multiLineCommentEndingPat = Pattern.compile(multiLineCommentEnding);
            Pattern configLinePat = Pattern.compile(configLine);
            Pattern configLinePat2 = Pattern.compile(configLine2);
            Pattern configLinePat3 = Pattern.compile(configLine3);
            Pattern noLinePat = Pattern.compile(noLine);
            //System.out.println("All the patterns are written");
            boolean multiLineCommentBeginningFlag = false;
            while (true)
            {
                Line = br.readLine();
                //System.out.println("The line is: "+Line);	
                if (Line == null)
                {
                    break;
                }
                //Now there will be no line terminator character at the end of each line
                Matcher matchSingleLineComment = singleLineCommentPat.matcher(Line);
                Matcher matchSingleLineComment2 = singleLineComment2Pat.matcher(Line);
                Matcher matchMultiLineCommentBeginning = multiLineCommentBeginningPat.matcher(Line);
                Matcher matchMultiLineCommentEnding = multiLineCommentEndingPat.matcher(Line);
                Matcher matchConflictingComment = conflictingCommentPat.matcher(Line);
                Matcher matchConfigLine = configLinePat.matcher(Line);
                Matcher matchConfigLine2 = configLinePat2.matcher(Line);
                Matcher matchConfigLine3 = configLinePat3.matcher(Line);
                Matcher matchNoLine = noLinePat.matcher(Line);
                //System.out.println("All matcher objects are created");
                if (!multiLineCommentBeginningFlag)
                {
                    if (matchSingleLineComment.matches()
                            || matchConflictingComment.matches())
                    {
                        //System.out.println("A single line comment is detected");

                    }
                    else if (matchConfigLine3.matches())
                    {
                        /* System.out.println("First = "
                                + matchConfigLine3.group(1)
                                + " Second = " + matchConfigLine3.group(2)+" Third = "+ matchConfigLine3.group(3)+" Fourth = "+matchConfigLine3.group(4)+" Fifth = "+matchConfigLine3.group(5)+" Sixth = "+matchConfigLine3.group(6));*/
                        if (!this.setAndValidateConfig(matchConfigLine3.group(2),
                                matchConfigLine3.group(5)))
                        {
                            return false;
                        }
                    }

                    else if (matchSingleLineComment2.matches())
                    {
                        //System.out.println("A single line comment is detected");

                    }
                    else if (matchMultiLineCommentBeginning.matches())
                    {
                        //System.out.println("A multiple line comment has began");
                        multiLineCommentBeginningFlag = true;
                    }
                    else if (matchMultiLineCommentEnding.matches())
                    {
                        LOGGER.fatal("Error in "
                                + fileName + " "
                                + "comment line as a multilinecomment "
                                + "closes before it opens");
                        return false;
                    }
                    else if (matchConfigLine.matches())
                    {
                        LOGGER.info("Configuration name= "
                                + matchConfigLine.group(1)
                                + " Configuration value= " + matchConfigLine.group(2));
                        if (!this.setAndValidateConfig(matchConfigLine.group(1),
                                matchConfigLine.group(2)))
                        {
                            return false;
                        }
                        //go to fetch the next line
                    }
                    else if (matchConfigLine2.matches())
                    {
                        LOGGER.info("configuration name= " + matchConfigLine2.group(1) + " Configuration value= " + matchConfigLine2.group(2));
                        if (!this.setAndValidateConfig(matchConfigLine2.group(1), matchConfigLine2.group(2)))
                        {
                            return false;
                        }
                    }
                    else if (matchNoLine.matches())
                    {

                    }
                    else
                    {
                        LOGGER.fatal("Error in " + fileName + " as this "
                                + "does not match with any pattern defined");
                        return false;
                    }

                }
                else
                {
                    if (matchMultiLineCommentEnding.matches()
                            || matchConflictingComment.matches())
                    {
                        //System.out.println("A muliple line comment has ended");
                        multiLineCommentBeginningFlag = false;
                    }
                }
            }
            /*end of while*/
            //System.out.println("End of while");
            if (multiLineCommentBeginningFlag)
            {
                LOGGER.fatal("Error in " + fileName + " as the multiline "
                        + "comment opened, is not closed");
                return false;
            }
            if (setHostIP && setHostPort && setDataBaseName && setUserName
                    && setPassword)
            {
                //if all the absolutely necessary configuration field values are taken
                alreadyLoadedFlag = true;
                return true;
            }
            else
            {
                LOGGER.fatal("Host IP (hostIP), Host Port(hostPort),"
                        + " DataBase Name(dataBaseName), User Name(userName)"
                        + ", password(password) these fields must be configured");
                return false;
            }
        }
        catch (FileNotFoundException e)
        {
            //FileNotFounfException e
            LOGGER.fatal("The file " + fileName + " cannot be opened");
            LOGGER.fatal("Maybe it's a directory. Check again");
            //System.out.println(e);
            return false;
        }
        catch (IOException e)
        {
            LOGGER.fatal("Error in reading from " + fileName + " file");
            LOGGER.fatal(e);
            return false;
        }
        //remember, finally will always be executed since final block bypasses return statement
        //or exception throwing
        finally
        {

            if (databaseConfigFileReader != null)
            {
                try
                {
                    databaseConfigFileReader.close();
                    LOGGER.info("The FileReader handler "
                            + "ServConfigFileReader attached with the file "
                            + fileName + " is successfully closed");
                }
                catch (IOException e)
                {
                    LOGGER.error("The FileReader handler "
                            + "ServConfigFileReader attached with the file "
                            + fileName + " cannot be closed properly");
                }
            }
            if (br != null)
            {
                try
                {
                    br.close();
                    LOGGER.info("The BufferedReader object br is "
                            + "succesfully closed");

                }
                catch (IOException e)
                {
                    LOGGER.error("The BufferedReader object br is not successfully"
                            + "closed");
                    //but we wont return false as we need not to
                    /*The configuration is successfully read or not that does 
                    not depend upon that the configuration file is closed or not
                     */
                }
            }
        }
    }
    private boolean setAndValidateConfig(String field, String value)
    {
        if (field == null)
        {
            LOGGER.fatal("The field does not contain any field name");
            return false;
        }
        if (value == null)
        {
            LOGGER.fatal("The value field does not contain any field value");
            return false;
        }
        switch (field)
        {
            case HOST_IP:
            {
                if (!setHostIP)
                {
                    //there's no case is which alreadyloadedFlag==true
                    //but, but setServIP is false
                    if (this.validateIP(value))
                    {
                        hostIP = value;
                        LOGGER.info("The Database Host IPAddress :"
                                + hostIP + " is set "
                                + "successfully");
                        setHostIP = true;
                        return true;
                    }
                    else
                    {
                        LOGGER.info("The Database Host IPAddress: "
                                + value + " is not valid");
                        return false;
                    }
                }
                else
                {
                    if (!alreadyLoadedFlag)
                    {
                        //a single database configuration file instance unfortunatrly
                        //has multiple line of hostIP instance
                        LOGGER.info("The configuration file already had a"
                                + " configuration line telling about Database host IP, This time"
                                + "it is trying to override the old value");
                    }
                    else
                    {
                        LOGGER.info("The reloadConfig function is called "
                                + " as some fields in DataBase Configuration might"
                                + " be changed");
                    }
                    if (this.validateIP(value))
                    {
                        hostIP = value;
                        LOGGER.info("The old value of "
                                + "host IP is overridden"
                                + "successfully");
                        return true;
                    }
                    else
                    {
                        LOGGER.error("The database host IPAddress: "
                                + value + " is not valid");
                        LOGGER.error("The old value of database Host IPAddress is not overridden"
                                + " successfully");
                        return true;
                    }
                }
            }
            case HOST_PORT:
            {
                short tempHostPort;
                if (!setHostPort)
                {
                    try
                    {
                        tempHostPort = Short.parseShort(value);
                    }
                    catch (NumberFormatException e)
                    {
                        //java.lang.Exception
                        LOGGER.fatal("Either the value corresponding to database "
                                + "host Port "
                                + value + " does not have integer value or"
                                + " the database host Port's value"
                                + "database host Port does not lie in the"
                                + "valid range of port value (0-65535)");
                        return false;
                    }
                    hostPort = tempHostPort;
                    LOGGER.info("The host port "
                            + hostPort + " is set successfully");
                    setHostPort = true;
                    return true;
                }
                else
                {
                    if (!alreadyLoadedFlag)
                    {
                        LOGGER.info("The configuration file already had a configuration"
                                + " line telling about the database host Port, this time it "
                                + "is trying to override the old value");
                    }
                    else
                    {
                        LOGGER.info("The function reloadConfig is called to reoad configuration"
                                + " as some portions of configuration file might be changed");
                    }
                    try
                    {
                        tempHostPort = Short.parseShort(value);
                        hostPort = tempHostPort;
                        LOGGER.info("The database host port " + hostPort
                                + " is overridden successfully");
                    }
                    catch (NumberFormatException e)
                    {
                        //java.lang.Exception
                        //not fatal error since trying to override
                        LOGGER.error("Either the value corresponding to database hostPort "
                                + value + " does not have integer value or"
                                + " The database host Port's value"
                                + "tempHostPort does not lie in the"
                                + "valid range of port value (0-65535)");
                        LOGGER.error("The old value of database host port is not overriden"
                                + " successfully");
                    }
                    return true;
                }
            }
            case DATABASE_NAME:
            {
                if (!setDataBaseName)
                {
                    //if flag is not set
                    dataBaseName = value;
                    //no validation is done
                    LOGGER.info("Database Name mentioned in configuration name: "
                            + dataBaseName);
                    setDataBaseName = true;
                    return true;

                }
                else
                {
                    if (!alreadyLoadedFlag)
                    {
                        LOGGER.info("The configuration file already had a configuration"
                                + " line telling about the database name, this time it "
                                + "is trying to override the old value");
                        //if alreadyLoadedFlag is false
                        /*that means database name is sepcified more than once
                        in the same file
                         */
                    }
                    else
                    {
                        //alreadyLoadedFlag is true
                        /*
                        that means we are reloading it
                         */
                        LOGGER.info("Reloadconfig function is called as some things"
                                + " mignt be changed in DataBaseConfiguration");
                    }
                    dataBaseName = value;
                    LOGGER.info("The new name of dataBase " + dataBaseName
                            + " is overridden successfully");
                    return true;
                }
            }
            case USER_NAME:
            {
                if (!setUserName)
                {
                    userName = value;
                    LOGGER.info("Database user Name mentioned in configuration name: "
                            + userName);
                    setUserName = true;
                    return true;
                }
                else
                {
                    if (!alreadyLoadedFlag)
                    {
                        LOGGER.info("The configuration file already had a configuration"
                                + " line telling about the username, this time it "
                                + "is trying to override the old value");

                    }
                    else
                    {
                        LOGGER.info("The reloadconfig function is called as some "
                                + "portions of the file might be changed");
                    }
                    userName = value;
                    LOGGER.info("The username is overridden successfully");
                    return true;
                }
            }
            case PASSWORD:
            {
                if (!setPassword)
                {
                    password = value;
                    LOGGER.info("Database password mentioned in configuration name: "
                            + password);
                    setPassword = true;
                    return true;
                }
                else
                {
                    if (!alreadyLoadedFlag)
                    {
                        LOGGER.info("The configuration file already had a configuration"
                                + " line telling about the database password, this time it "
                                + "is trying to override the old value");

                    }
                    else
                    {
                        LOGGER.info("The reloadconfig function is called as some "
                                + "portions of the file might be changed");
                    }
                    password = value;
                    LOGGER.info("The password is overridden successfully");
                    return true;
                }
            }
            default:
            {
                /*
                Unwanted field is present in DataBaseConnection.cfg
                 */
                LOGGER.debug("Unspecified field name: " + field);
                LOGGER.fatal("Unspecified field is present in MongoDBConfiguration.cfg");
                return false;
            }
        }
    }

    private boolean validateIP(String ipAddress)
    {
        String ipAddressRegex
                = "(\\d{1}|\\d{2}|\\d{3}).(\\d{1}|\\d{2}|\\d{3}).(\\d{1}|d{2}|\\d{3}).(\\d{1}|\\d{2}|\\d{3})";
        Pattern ipAddressPattern = Pattern.compile(ipAddressRegex);
        Matcher ipAddressMatcher = ipAddressPattern.matcher(ipAddress);
        if (ipAddressMatcher.matches())
        {
            int firstOctet = Integer.parseInt(ipAddressMatcher.group(1));
            int secondOctet = Integer.parseInt(ipAddressMatcher.group(2));
            int thirdOctet = Integer.parseInt(ipAddressMatcher.group(3));
            int fourthOctet = Integer.parseInt(ipAddressMatcher.group(4));
            //System.out.println("First octet: "+firstOctet+" Second octet: "+secondOctet+" Third octet: "+thirdOctet+" Fourth Octet: "+fourthOctet);
            if ((firstOctet >= 0 && firstOctet <= 255) && (secondOctet >= 0
                    && secondOctet <= 255) && (thirdOctet >= 0 && thirdOctet <= 255)
                    && (fourthOctet >= 0 && fourthOctet <= 255))
            {
                //first octet cannot be zero
                LOGGER.info("The IPaddress is a valid address");
                //      System.out.println(""+firstOctet+""+secondOCtet+""+thirdOctet+""+fourthOctet+"\n");
                return true;
            }
            else
            {
                LOGGER.fatal("The IPaddress is not a valid address");
                return false;
            }
        }
        else
        {
            LOGGER.fatal("The IPaddress is not a valid address");
            return false;
        }
        /*for any invalid IP of type like:
                 255.2555.189.232 would not match the regexpattern
         */
        /*for any invalid IP of type like:
        255.2555.189.232 would not match the regexpattern
        192.1.1.1 matches the regex pattern
        255.256.255.255 will be handled by the checkings
         */
    }

    public boolean reloadConfig()
    {
        if (!alreadyLoadedFlag)
        {
            if (this.loadConfig())
            {
                Date currDate = new Date();
                lastAccessTime = currDate.getTime();
                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            Date currDate = new Date();
            long currentAccessTime = currDate.getTime();
            double timeDiff = (currentAccessTime - lastAccessTime) / 1000;
            long timeDiffInSeconds = Math.round(timeDiff);
            if (timeDiffInSeconds > RELOAD_TIME_DIFF)
            {
                //implementation of how you will reload it
                lastAccessTime = currentAccessTime;
                if (this.loadConfig())
                {
                    return true;
                }
                else
                {
                    LOGGER.fatal("The database configuration cannot be reloaded"
                            + " as some of the changes specifications are not valid");
                    return false;
                }
            }

            else
            {
                LOGGER.error("The DataBase Configurations cannot be reloaded as we"
                        + "try to reload configuration file frequently");
                return true;
                //as it cannot be reloaded due to time gap
            }
        }
    }

    public String returnHostIP()
    {
        return hostIP;
    }

    public short returnHostPort()
    {
        return hostPort;
    }

    public String returnDataBaseName()
    {
        return dataBaseName;
    }

    public String returnUserName()
    {
        return userName;
    }

    public String returnPassword()
    {
        return password;
    }
}

