/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hutil;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hutil.HUtilConstants.HUTIL_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hutil.HUtilConstants.HUTIL_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hutil.HUtilConstants.hUtilReadSpec;
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
    @Table(name = CN_TABLE, columnFamily = "hardware_util"),
    @Table(name = CN_TABLE, columnFamily = "network_util"),
    @Table(name = CN_HISTORY_TABLE, columnFamily = "hardware_util"),
    @Table(name = CN_HISTORY_TABLE, columnFamily = "network_util")

})
public class HUtil extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(HUtil.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "cpu_usr");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        HUtilConstants.HUtilMetricContext ctx = HUtilConstants.createMapperContext();
        handleHUtil(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("cpu_usr")).split("[\\t]++");
        if (header.length != HUTIL_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(HUTIL_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + HUTIL_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleHUtil(String[] data, ParallelCollector collector, HUtilConstants.HUtilMetricContext ctx) {
        Map<String, AccountingReadSpecification> hUtilElements = AccountingReadSpecification.fromReadWritemap(hUtilReadSpec);

        for (int i = 1; i < data.length; i++) {
            handleHUtilSequence(data[i], hUtilElements, i);
        }
        collector.addData(hUtilElements).addMappings(HUtilConstants.getHUtilMappers(ctx)).addMappings(HUtilConstants.getHistoryHUtilMappers(ctx));
    }

    private boolean handleHUtilSequence(String fsInput, Map<String, AccountingReadSpecification> hUtilElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < HUTIL_COLUMN_NAMES.length; i++) {
            String key = HUTIL_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = hUtilElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, hUtilElements);
            }
        }
        return true;
    }

}
