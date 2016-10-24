/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.appinstance;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.appinstance.AppInstanceDeployedConstants.APP_INSTANCE_DEPLOYED_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.appinstance.AppInstanceDeployedConstants.APP_INSTANCE_DEPLOYED_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.appinstance.AppInstanceDeployedConstants.appInstanceDeployedReadSpec;
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
@Tables(annotations={
		@Table(name=VM_TABLE,columnFamily="meta"),
                @Table(name=VM_HISTORY_TABLE, columnFamily="meta")
})

public class AppInstanceDeployed extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(AppInstanceDeployed.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "VMID");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        AppInstanceDeployedConstants.AppInstanceDeployedMetricContext ctx = AppInstanceDeployedConstants.createMapperContext();
        handleAppInstanceDeployed(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("VMID")).split("[\\t]++");
        if (header.length != APP_INSTANCE_DEPLOYED_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(APP_INSTANCE_DEPLOYED_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + APP_INSTANCE_DEPLOYED_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleAppInstanceDeployed(String[] data, ParallelCollector collector, AppInstanceDeployedConstants.AppInstanceDeployedMetricContext ctx) {
        Map<String, AccountingReadSpecification> appInstanceDeployedElements = AccountingReadSpecification.fromReadWritemap(appInstanceDeployedReadSpec);
        for (int i = 1; i < data.length; i++) {
            handleAppInstanceDeployedSequence(data[i], appInstanceDeployedElements, i);
        }
        collector.addData(appInstanceDeployedElements).addMappings(AppInstanceDeployedConstants.getAppInstanceDeployedMappers(ctx)).addMappings(AppInstanceDeployedConstants.getAppInstanceDeployedHistoryMappers(ctx));
    }

    private boolean handleAppInstanceDeployedSequence(String fsInput, Map<String, AccountingReadSpecification> appInstanceDeployedElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < APP_INSTANCE_DEPLOYED_COLUMN_NAMES.length; i++) {
            String key = APP_INSTANCE_DEPLOYED_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = appInstanceDeployedElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, appInstanceDeployedElements);
            }
        }
        return true;
    }

}
