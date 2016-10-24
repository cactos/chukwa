/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hdisk;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hdisk.HDiskConstants.HDISK_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hdisk.HDiskConstants.HDISK_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hdisk.HDiskConstants.hdiskReadSpec;

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
    @Table(name = CN_TABLE, columnFamily = "storage"),
    @Table(name = CN_HISTORY_TABLE, columnFamily = "storage"),
    @Table(name = ST_TABLE, columnFamily = "spec"),
    @Table(name = ST_HISTORY_TABLE, columnFamily = "spec")
})
public class HDisk extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(HDisk.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "disk_name");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        HDiskConstants.HDiskMetricContext ctx = HDiskConstants.createMapperContext();
        handleHDisk(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("disk_name")).split("[\\t]++");
        if (header.length != HDISK_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(HDISK_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + HDISK_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleHDisk(String[] data, ParallelCollector collector, HDiskConstants.HDiskMetricContext ctx) {
        Map<String, AccountingReadSpecification> hdiskElements = AccountingReadSpecification.fromReadWritemap(hdiskReadSpec);

        for (int i = 1; i < data.length; i++) {
            handleHDiskSequence(data[i], hdiskElements, i);
        }
        if (collector.getSource().contains("storage")) {
            collector.addData(hdiskElements).addMappings(HDiskConstants.getHDiskMappersForStorage(ctx)).addMappings(HDiskConstants.getHistoryHDiskMappersForStorage(ctx));
        } else {
            collector.addData(hdiskElements).addMappings(HDiskConstants.getHDiskMappers(ctx)).addMappings(HDiskConstants.getHistoryHDiskMappers(ctx));
        }
    }

    private boolean handleHDiskSequence(String fsInput, Map<String, AccountingReadSpecification> hdiskElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < HDISK_COLUMN_NAMES.length; i++) {
            String key = HDISK_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = hdiskElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, hdiskElements);
            }
        }
        return true;
    }

}
