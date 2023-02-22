package gov.usbr.wq.merlindataexchange;


import hec.ui.ProgressListener;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

final class TestLogProgressListener implements ProgressListener
{

    private static final Logger LOGGER = Logger.getLogger(TestLogProgressListener.class.getName());
    private int _progress = 0;

    TestLogProgressListener()
    {
        LOGGER.setUseParentHandlers(false);
        LOGGER.addHandler(new ConsoleHandler()
        {
            @Override
            public Formatter getFormatter()
            {
                return new Formatter() {
                    @Override
                    public String format(LogRecord record)
                    {
                        return "PROGRESS LOG: " + record.getMessage() + "\n";
                    }
                };
            }
        });
    }
    @Override
    public void start()
    {
        LOGGER.info("started");
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
        LOGGER.info("Finished!");
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
        if(messageType == MessageType.IMPORTANT)
        {
            LOGGER.info(s);
        }
        if(messageType == MessageType.ERROR)
        {
            LOGGER.info("ERROR: " + s);
        }
    }

    @Override
    public void progress(String s, int i)
    {

    }

    @Override
    public void progress(String s, MessageType messageType, int i)
    {
        progress(s, messageType);
        progress(i);
    }

    @Override
    public void incrementProgress(int i)
    {

    }

    int getProgress()
    {
        return _progress;
    }
}
