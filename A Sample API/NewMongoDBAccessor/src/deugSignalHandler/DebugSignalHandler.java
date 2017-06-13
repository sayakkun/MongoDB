package debugSignalHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Signal;
import sun.misc.SignalHandler;
import newmongodbaccessor.MongoDBConfiguration;
import newmongodbaccessor.MongoDBHandler;

public class DebugSignalHandler implements SignalHandler
{

    private static final Logger LOGGER = LogManager.getLogger(DebugSignalHandler.class.getName());

    public static void listenTo(String name)
    {
        LOGGER.debug("listen To function is called of DebugSignalHandler");
        Signal signal = new Signal(name);
        Signal.handle(signal, (SignalHandler) new DebugSignalHandler());
    }

    @Override
    public void handle(Signal signal)
    {
        LOGGER.debug("Handle function is called of DebugSignalHandler");
        LOGGER.info("Signal: " + signal);
        String signalName = signal.toString().trim();
        LOGGER.debug("Signal name is :" + signalName);
        if (signal.toString().trim().equals("SIGUSR2"))
        {
            LOGGER.debug("SIGUSR2 signal is sent");
            MongoDBConfiguration mongoDBConfigurationInstance
                    = MongoDBConfiguration.getMongoDBConfigurationInstance();
            if (mongoDBConfigurationInstance.reloadConfig() == false)
            {
                System.exit(0);
            }
            LOGGER.debug("ReloadConfig function is called");
            MongoDBHandler mongoDBHandlerInstance = MongoDBHandler.getMongoDBHandlerInstance();
            if (mongoDBHandlerInstance == null)
            {
                LOGGER.fatal("MongoDBHandler cannot be instantiated");
                System.exit(0);
            }
            if(!mongoDBHandlerInstance.initMongoDBHandler())
            {
                System.exit(0);
            }
        }
    }
}
