package timandes.flume.sinks;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import java.io.File;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSink extends AbstractSink implements Configurable
{
    private static final int _defaultBatchSize = 100;

    /** @var String File path */
    private String _pathTemplate;
    /** @var int Events per batch */
    private int _batchSize = _defaultBatchSize;

    private static final Logger logger = LoggerFactory.getLogger(FileSink.class);

    private OutputStream _outputStream;
    private String _currentPath;

    @Override
    public void configure(Context context)
    {
        this._pathTemplate = context.getString("pathTemplate", "'/var/log/'YYYYMMdd'.log'");
        this._batchSize = context.getInteger("batchSize", this._defaultBatchSize);
    }

    @Override
    public void start()
    {
        logger.debug("FileSink.start()");
    }

    @Override
    public void stop()
    {
        logger.debug("FileSink.stop()");
    }

    @Override
    public Status process() throws EventDeliveryException
    {
        // decide output stream
        String path = this._getOutputFilePath();
        if (path != this._currentPath) {
            try {
                // close file
                if (null != this._outputStream) {
                    this._outputStream.flush();
                    this._outputStream.close();
                    this._outputStream = null;
                }

                this._createParentDirs(path);

                this._outputStream = new BufferedOutputStream(
                        new FileOutputStream(path, true));
            } catch (IOException ioe) {
                logger.error(ioe.getMessage());

                this._outputStream = null;
                this._currentPath = "";

                return Status.BACKOFF;
            } catch (Throwable t) {
                logger.error(t.getMessage());

                if (t instanceof Error)
                    throw (Error)t;

                return Status.BACKOFF;
            }
        }

        Status retval = null;

        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event = null;
        txn.begin();
        try {
            for(int i=0; i<this._batchSize; ++i) {
                event = ch.take();
                if (event == null) {
                    retval = Status.BACKOFF;
                    break;
                } else {
                    this._outputStream.write(event.getBody());
                    this._outputStream.write(0x0a);
    
                    retval = Status.READY;
                }
            }

            this._outputStream.flush();
            txn.commit();
        } catch (Throwable t) {
            logger.error(t.getMessage());

            txn.rollback();
            retval = Status.BACKOFF;

            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }

        return retval;
    }

    /**
     * Create parent dirs of given path
     */
    private void _createParentDirs(String path) throws Exception
    {
        File file = new File(path);
        String dirPath = file.getParent();
        
        File dir = new File(dirPath);
        if (dir.exists())
            return;

        if (!dir.mkdirs())
            throw new Exception("Fail to create parent dirs for path '" + path + "'");
    }

    private String _getOutputFilePath()
    {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat(this._pathTemplate);
        String path = dateFormat.format(now);

        return path;
    }
}
