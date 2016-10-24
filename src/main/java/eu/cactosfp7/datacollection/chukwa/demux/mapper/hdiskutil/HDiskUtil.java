/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hdiskutil;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hdiskutil.HDiskUtilConstants.HDISKUTIL_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hdiskutil.HDiskUtilConstants.HDISKUTIL_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hdiskutil.HDiskUtilConstants.hdiskutilReadSpec;

import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 *
 * @author atsi
 */
@Tables(annotations = {
    @Table(name = CN_TABLE, columnFamily = "storage_util"),
    @Table(name = CN_HISTORY_TABLE, columnFamily = "storage_util"),
    @Table(name = ST_TABLE, columnFamily = "util"),
    @Table(name = ST_HISTORY_TABLE, columnFamily = "util")
})
public class HDiskUtil extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(HDiskUtil.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = "";

        try {
            body = AbstractProcessorConstants.skipStartSequence(recordEntry, "Device:");
        } catch (Exception e) {
            log.warn("Cannot find disk data on " + collector.getSource());
            return;
        }

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        HDiskUtilConstants.HDiskUtilMetricContext ctx = HDiskUtilConstants.createMapperContext();
        handleHDiskUtil(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("Device:")).split("[\\s]++");
        if (header.length != HDISKUTIL_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(HDISKUTIL_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + HDISKUTIL_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleHDiskUtil(String[] data, ParallelCollector collector, HDiskUtilConstants.HDiskUtilMetricContext ctx) {
        Map<String, AccountingReadSpecification> hdiskElements = AccountingReadSpecification.fromReadWritemap(hdiskutilReadSpec);

        for (int i = 1; i < data.length; i++) {
            handleHDiskUtilSequence(data[i], hdiskElements, i);
        }
        if (collector.getSource().contains("storage")) {
            collector.addData(hdiskElements).addMappings(HDiskUtilConstants.getHDiskUtilMappersForStorage(ctx)).addMappings(HDiskUtilConstants.getHistoryHDiskUtilMappersForStorage(ctx));
        } else {
            collector.addData(hdiskElements).addMappings(HDiskUtilConstants.getHDiskUtilMappers(ctx)).addMappings(HDiskUtilConstants.getHistoryHDiskUtilMappers(ctx));
        }
    }

    private boolean handleHDiskUtilSequence(String fsInput, Map<String, AccountingReadSpecification> hdiskUtilElements, int iteration) {
        String[] data = fsInput.split("[\\s]++");
        for (int i = 0; i < HDISKUTIL_COLUMN_NAMES.length; i++) {
            String key = HDISKUTIL_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = hdiskUtilElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, hdiskUtilElements);
            }
        }
        return true;
    }

}
