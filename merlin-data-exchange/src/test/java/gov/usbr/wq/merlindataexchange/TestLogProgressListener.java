package gov.usbr.wq.merlindataexchange;


import hec.ui.ProgressListener;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public final class TestLogProgressListener implements ProgressListener
{

    private static final Logger LOGGER = Logger.getLogger(TestLogProgressListener.class.getName());
    private int _progress = 0;

    public TestLogProgressListener() throws IOException
    {
        LOGGER.setUseParentHandlers(false);
        Path dir = Paths.get(System.getProperty("user.dir")).resolve("build/tmp").resolve("progressLog.log");
        LOGGER.addHandler(new FileHandler(dir.toString())
        {
            @Override
            public Formatter getFormatter()
            {
                return new Formatter() {
                    @Override
                    public String format(LogRecord record)
                    {
                        return "PROGRESS LOG " + record.getMessage() + "\n";
                    }
                };
            }
        });
        LOGGER.addHandler(new ConsoleHandler()
        {
            @Override
            public Formatter getFormatter()
            {
                return new Formatter() {
                    @Override
                    public String format(LogRecord record)
                    {
                        return "PROGRESS LOG " + record.getMessage() + "\n";
                    }
                };
            }
        });
    }
    @Override
    public void start()
    {

    }

    @Override
    public void start(int i)
    {

    }

    @Override
    public void switchToIndeterminate()
    {

    }

    @Override
    public void setStayOnTop(boolean b)
    {

    }

    @Override
    public void switchToDeterminate(int i)
    {

    }

    @Override
    public void finish()
    {
    }

    @Override
    public void progress(int i)
    {
        _progress = i;
        LOGGER.info("Progress: " + i + "%");
    }

    @Override
    public void progress(String s)
    {
        LOGGER.info(s);
    }

    @Override
    public void progress(String s, MessageType messageType)
    {
        String msgType = "(" + messageType + "): ";
        if(messageType == MessageType.ERROR)
        {
            LOGGER.info("ERROR: " + s);
        }
        else if(messageType == MessageType.WARNING)
        {
            LOGGER.info("WARNING: " + s);
        }
        else if(messageType == MessageType.GENERAL)
        {
            LOGGER.info(msgType + "  " + s);
        }
        else if(messageType == MessageType.IMPORTANT)
        {
            LOGGER.info(msgType + s);
        }
    }

    @Override
    public void progress(String s, int i)
    {

    }

    @Override
    public void progress(String s, MessageType messageType, int i)
    {
        String percentMsg = "(" + i + "%) ";
        if(i < 10)
        {
            percentMsg += "  ";
        }
        else if(i < 100)
        {
            percentMsg += " ";
        }
        progress(percentMsg + s, messageType);
    }

    @Override
    public void incrementProgress(int i)
    {

    }

    public int getProgress()
    {
        return _progress;
    }
}
