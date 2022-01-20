package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;

public interface LoggingServer {



    default Logger initializeLogging(String s, String path, boolean useConsole) throws IOException{
        Logger logger = Logger.getLogger(s);
        logger.setUseParentHandlers(useConsole);
        logger.setLevel(Level.ALL);

        // choose which level of logs goes to console
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.WARNING);
        logger.addHandler(ch);

        // make a designated logs dir
        File f = new File(System.getProperty("user.dir") + path);
        if (!f.exists()) f.mkdir();


        // choose which level of logs goes to .log file
        FileHandler fh = new FileHandler(f.getAbsolutePath() + "/" +  s + ".log");
        fh.setLevel(Level.FINE);
        logger.addHandler(fh);

        return logger;
    }

    default Logger initializeLogging(String s) throws IOException {
        return initializeLogging(s, "/logs", false);
    }

    default Logger initializeLogging(String s, String path) throws IOException {
        return initializeLogging(s, path, false);
    }
}
