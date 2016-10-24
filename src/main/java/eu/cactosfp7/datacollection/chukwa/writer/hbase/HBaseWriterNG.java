/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.cactosfp7.datacollection.chukwa.writer.hbase;

import static eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseWriterConstants.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineableWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessor;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessorFactory;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.UnknownRecordTypeException;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Reporter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.log4j.Logger;

public class HBaseWriterNG extends PipelineableWriter {

    private final static Logger log = Logger.getLogger(HBaseWriterNG.class);

    private final boolean reportStats;
    private volatile long dataSize = 0;
    private final Timer statTimer;
    // private OutputCollectorNG output;

    private Reporter reporter; //FIXME: does this really have to be a field?
    private ChukwaConfiguration conf = new ChukwaConfiguration();
    private final String defaultProcessor = getDefaultProcessorName();

    private final HTablePool pool;
    private Configuration hconf;

    private String getDefaultProcessorName() {
        return conf.get(CHUKWA_DEFAULT_DEMUX_PROCESSOR_KEY, CHUKWA_DEFAULT_DEMUX_PROCESSOR_VALUE);
    }

    public HBaseWriterNG() {
        this(true, new ChukwaConfiguration(), HBaseConfiguration.create());
    }

    private HBaseWriterNG(boolean reportStats, ChukwaConfiguration conf, Configuration hconf) {
        this.reportStats = reportStats;
        statTimer = new Timer();
        //FIXME: shouldn't this be started?
		/* HBase Version 0.89.x */
        this.hconf = hconf;
        this.conf = conf;
        pool = new HTablePool(hconf, 60);
    }

    public HBaseWriterNG(ChukwaConfiguration conf, Configuration hconf) {
        this(true, conf, hconf);
    }

    /**
     * close this writer;
     */
    public void close() {
        statTimer.cancel();
    }

    /**
     * init method; only called once
     */
    public void init(Configuration conf) throws WriterException {
        if (reportStats) {
            statTimer.schedule(new StatReportingTask(), 1000, 10 * 1000);
        }
        reporter = new Reporter();
        HBaseSchemaValidator.verifyHbaseSchema(conf, hconf);
    }

    private MapProcessor findProcessorForChunk(Chunk chunk) throws UnknownRecordTypeException {
        String processorClass = conf.get(chunk.getDataType(), defaultProcessor);
        MapProcessor processor = MapProcessorFactory.getProcessor(processorClass);
        return processor;
    }

    private void iterateChunks(List<Chunk> chunks) {
        for (Chunk chunk : chunks) {
            handleChunk(chunk);
            dataSize += chunk.getData().length;
            reporter.clear();
        }
    }

    private void handleChunk(Chunk chunk) {
        try {
            doHandleChunk(chunk);
        } catch (IOException e) {
            log.warn("error when writing data to HBase");
            log.warn(ExceptionUtil.getStackTrace(e));
            e.printStackTrace();
        } catch (UnknownRecordTypeException e) {
            log.error("could not find or instantiate processor for chunk: " + chunk.getDataType());
            log.error(ExceptionUtil.getStackTrace(e));
        }
    }

    private void doHandleChunk(Chunk chunk) throws IOException, UnknownRecordTypeException {
        OutputCollectorNG output = new OutputCollectorNG();
        MapProcessor processor = findProcessorForChunk(chunk);
        Set<String> tableNames = findTablesForProcessor(processor);
        if (tableNames.isEmpty()) {
            return;
        }

        processor.process(new ChukwaArchiveKey(), chunk, output, reporter);
        writeToHBase(tableNames, output);
    }

    private void writeToHBase(Set<String> tables, OutputCollectorNG output) throws IOException {
        for (String table : tables) {
            HTableInterface hbase = pool.getTable(table.getBytes());

//            hbase.delete(output.getDeleteKeyValues(table));
            hbase.put(output.getKeyValues(table));
            pool.putTable(hbase);
        }
    }

    private Set<String> findTablesForProcessor(MapProcessor processor) {
        if (processor.getClass().isAnnotationPresent(Table.class)) {
            Table t = processor.getClass().getAnnotation(Table.class);
            return Collections.singleton(t.name());
        }

        if (processor.getClass().isAnnotationPresent(Tables.class)) {
            Set<String> tableNames = new HashSet<String>();
            Tables tables = processor.getClass().getAnnotation(Tables.class);
            for (Table t : tables.annotations()) {
                tableNames.add(t.name());
            }
            return tableNames;
        }

        return Collections.emptySet();
    }

    @Override
    public synchronized CommitStatus add(List<Chunk> chunks) throws WriterException {
        iterateChunks(chunks);
        // FIXME: how to figure out if data was only partially written //
        return proceedToNext(chunks);
    }

    private CommitStatus proceedToNext(List<Chunk> chunks) throws WriterException {
        CommitStatus rv = ChukwaWriter.COMMIT_OK;
        if (next != null) {
            rv = next.add(chunks); //pass data through
        }
        return rv;
    }

    private class StatReportingTask extends TimerTask {

        private long lastTs = System.currentTimeMillis();
        private long lastDataSize = 0;

        public void run() {
            long time = System.currentTimeMillis();
            long interval = time - lastTs;
            lastTs = time;

            long ds = dataSize;
            long dataRate = 1000 * (ds - lastDataSize) / interval; // bytes/sec
            // refers only to data field, not including http or chukwa headers
            lastDataSize = ds;

            log.info("stat=HBaseWriter|dataRate=" + dataRate);
        }
    };
}
