package timandes.flume.sinks;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSink extends AbstractSink implements Configurable
{
    private String _pathTemplate;
    private static final Logger logger = LoggerFactory.getLogger(FileSink.class);

    @Override
    public void configure(Context context)
    {
        this._pathTemplate = context.getString("pathTemplate", "'/var/log/'YYYYMMdd'.log'");
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
        Status retval = null;

        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            if (event == null) {
                logger.debug("No new event was found");
                retval = Status.BACKOFF;
            } else {
                logger.debug("New events arrived");
                Date now = new Date();
                SimpleDateFormat dateFormat = new SimpleDateFormat(this._pathTemplate);
                String path = dateFormat.format(now);

                logger.debug("path=" + path);
/*
                FileWriter writer = new FileWriter(path, true);
                writer.write(event.getBody());
                writer.close();
*/
                FileOutputStream os = new FileOutputStream(path, true);
                os.write(event.getBody());
                os.write(0x0a);
                os.close();

                retval = Status.READY;
            }

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

}
