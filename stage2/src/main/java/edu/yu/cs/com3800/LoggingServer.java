package edu.yu.cs.com3800;

import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface LoggingServer {

    default Logger initializeLogging(String s, boolean useConsole) throws IOException{
        Logger logger = Logger.getLogger(s);
        //todo disable console
        logger.setUseParentHandlers(useConsole);
        FileHandler fh = new FileHandler(s + ".log");
        fh.setLevel(Level.FINEST);
        logger.addHandler(fh);
        return logger;
    }
}
