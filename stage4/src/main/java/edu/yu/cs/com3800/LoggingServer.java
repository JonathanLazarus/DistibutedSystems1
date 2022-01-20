package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;

public interface LoggingServer {

    default Logger initializeLogging(String s, boolean useConsole) throws IOException{
        Logger logger = Logger.getLogger(s);
        logger.setUseParentHandlers(useConsole);
        logger.setLevel(Level.ALL);

        // choose which level of logs goes to console
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.WARNING);
        logger.addHandler(ch);

        // make a designated logs dir
        File f = new File(System.getProperty("user.dir") + "/logs");
        f.mkdir();


        // choose which level of logs goes to .log file
        FileHandler fh = new FileHandler(f.getAbsolutePath() + "/" + s + ".log");
        fh.setLevel(Level.FINEST);
        logger.addHandler(fh);

        return logger;
    }

    default Logger initializeLogging(String s) throws IOException {
        return initializeLogging(s, false);
    }
}
