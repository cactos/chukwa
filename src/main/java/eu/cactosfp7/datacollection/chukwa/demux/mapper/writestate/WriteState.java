/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.writestate;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.writestate.WriteStateConstants.WRITE_STATE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.writestate.WriteStateConstants.WRITE_STATE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.writestate.WriteStateConstants.writeStateReadSpec;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 *
 * @author atsi
 */
@Tables(annotations = {
    @Table(name = CN_TABLE, columnFamily = "meta"),
    @Table(name = CN_HISTORY_TABLE, columnFamily = "meta")
})

public class WriteState extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(WriteState.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "NodeName");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        WriteStateConstants.WriteStateMetricContext ctx = WriteStateConstants.createMapperContext();
        handleWriteState(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("NodeName")).split("[\\t]++");
        if (header.length != WRITE_STATE_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(WRITE_STATE_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + WRITE_STATE_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleWriteState(String[] data, ParallelCollector collector, WriteStateConstants.WriteStateMetricContext ctx) {
        Map<String, AccountingReadSpecification> vmElements = AccountingReadSpecification.fromReadWritemap(writeStateReadSpec);
        for (int i = 1; i < data.length; i++) {
            handleWriteStateSequence(data[i], vmElements, i);
        }
        collector.addData(vmElements).addMappings(WriteStateConstants.getWriteStateMappers(ctx)).addMappings(WriteStateConstants.getWriteStateHistoryMappers(ctx));
    }

    private boolean handleWriteStateSequence(String fsInput, Map<String, AccountingReadSpecification> nodeElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < WRITE_STATE_COLUMN_NAMES.length; i++) {
            String key = WRITE_STATE_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = nodeElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, nodeElements);
            }
        }
        return true;
    }

}
