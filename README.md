Flume-NG File
=============

File processing plugin for Apache Flume NG.

Configuration of File Sink
--------------------------

    agent1.sinks.filesink1.channel = channel1
    agent1.sinks.filesink1.type = timandes.flume.sinks.FileSink
    agent1.sinks.filesink1.pathTemplate = '/var/log/'yyyyMMdd'.log'
    agent1.sinks.filesink1.batchSize = 100

Configurations
--------------

* pathTemplate

    Template of log file.

* batchSize

    Processing events per batch.
