/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hpowerutil;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hpowerutil.HPowerUtilConstants.POWER_UTIL_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hpowerutil.HPowerUtilConstants.POWER_UTIL_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hpowerutil.HPowerUtilConstants.hPowerUtilReadSpec;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 *
 * @author atsi
 */
@Annotation.Tables(annotations = {
    @Annotation.Table(name = CN_TABLE, columnFamily = "power_util"),
    @Annotation.Table(name = CN_HISTORY_TABLE, columnFamily = "power_util")
})
public class HPowerUtil extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(HPowerUtil.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "consumption");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        HPowerUtilConstants.PowerMetricContext ctx = HPowerUtilConstants.createMapperContext();
        handleHPowerUtil(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("consumption")).split("[\\t]++");
        if (header.length != POWER_UTIL_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(POWER_UTIL_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + POWER_UTIL_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleHPowerUtil(String[] data, ParallelCollector collector, HPowerUtilConstants.PowerMetricContext ctx) {
        Map<String, AccountingReadSpecification> powerUtilElements = AccountingReadSpecification.fromReadWritemap(hPowerUtilReadSpec);

        for (int i = 1; i < data.length; i++) {
            handleHPowerUtilSequence(data[i], powerUtilElements, i);
        }
        collector.addData(powerUtilElements).addMappings(HPowerUtilConstants.getPowerUtilMappers(ctx)).addMappings(HPowerUtilConstants.getHistoryPowerUtilMappers(ctx));
    }

    private boolean handleHPowerUtilSequence(String fsInput, Map<String, AccountingReadSpecification> powerUtilElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < POWER_UTIL_COLUMN_NAMES.length; i++) {
            String key = POWER_UTIL_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = powerUtilElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, powerUtilElements);
            }
        }
        return true;
    }

}
