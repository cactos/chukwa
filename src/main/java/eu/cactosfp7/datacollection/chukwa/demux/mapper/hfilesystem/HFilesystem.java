/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hfilesystem;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hfilesystem.HFilesystemConstants.HFILESYSTEM_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hfilesystem.HFilesystemConstants.HFILESYSTEM_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hfilesystem.HFilesystemConstants.hFilesystemReadSpec;

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
    @Table(name = CN_TABLE, columnFamily = "filesystem"),
    @Table(name = CN_HISTORY_TABLE, columnFamily = "filesystem"),
    @Table(name = ST_TABLE, columnFamily = "filesystem"),
    @Table(name = ST_HISTORY_TABLE, columnFamily = "filesystem")
})
public class HFilesystem extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(HFilesystem.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "mount");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        HFilesystemConstants.HFilesystemMetricContext ctx = HFilesystemConstants.createMapperContext();
        handleHFilesystem(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("mount")).split("[\\t]++");
        if (header.length != HFILESYSTEM_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(HFILESYSTEM_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + HFILESYSTEM_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleHFilesystem(String[] data, ParallelCollector collector, HFilesystemConstants.HFilesystemMetricContext ctx) {
        Map<String, AccountingReadSpecification> hFilesystemElements = AccountingReadSpecification.fromReadWritemap(hFilesystemReadSpec);

        for (int i = 1; i < data.length; i++) {
            handleHFilesystemSequence(data[i], hFilesystemElements, i);
        }
        if (collector.getSource().contains("storage")) {
            collector.addData(hFilesystemElements).addMappings(HFilesystemConstants.getHFilesystemMappersForStorage(ctx)).addMappings(HFilesystemConstants.getHistoryHFilesystemMappersForStorage(ctx));
        } else {
            collector.addData(hFilesystemElements).addMappings(HFilesystemConstants.getHFilesystemMappers(ctx)).addMappings(HFilesystemConstants.getHistoryHFilesystemMappers(ctx));
        }
    }

    private boolean handleHFilesystemSequence(String fsInput, Map<String, AccountingReadSpecification> hdiskElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < HFILESYSTEM_COLUMN_NAMES.length; i++) {
            String key = HFILESYSTEM_COLUMN_NAMES[i];
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
