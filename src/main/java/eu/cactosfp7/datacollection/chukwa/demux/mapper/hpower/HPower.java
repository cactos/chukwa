/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hpower;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hpower.HPowerConstants.POWER_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hpower.HPowerConstants.POWER_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.hpower.HPowerConstants.hPowerReadSpec;
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
    @Annotation.Table(name = CN_TABLE, columnFamily = "power"),
    @Annotation.Table(name = CN_HISTORY_TABLE, columnFamily = "power")
})
public class HPower extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(HPower.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "serial");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        HPowerConstants.PowerMetricContext ctx = HPowerConstants.createMapperContext();
        handleHPower(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("serial")).split("[\\t]++");
        if (header.length != POWER_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(POWER_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + POWER_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleHPower(String[] data, ParallelCollector collector, HPowerConstants.PowerMetricContext ctx) {
        Map<String, AccountingReadSpecification> powerElements = AccountingReadSpecification.fromReadWritemap(hPowerReadSpec);

        for (int i = 1; i < data.length; i++) {
            handleHPowerSequence(data[i], powerElements, i);
        }
        collector.addData(powerElements).addMappings(HPowerConstants.getPowerMappers(ctx)).addMappings(HPowerConstants.getHistoryPowerMappers(ctx));
    }

    private boolean handleHPowerSequence(String fsInput, Map<String, AccountingReadSpecification> POWERElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < POWER_COLUMN_NAMES.length; i++) {
            String key = POWER_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = POWERElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, POWERElements);
            }
        }
        return true;
    }

}
