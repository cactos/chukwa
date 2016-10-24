/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.vm;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vm.VMConstants.VM_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vm.VMConstants.VM_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vm.VMConstants.vmReadSpec;
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
    @Table(name = CN_TABLE, columnFamily = "vms"),
    @Table(name = CN_HISTORY_TABLE, columnFamily = "vms"),
    @Table(name = VM_TABLE, columnFamily = "meta"),
    @Table(name = VM_HISTORY_TABLE, columnFamily = "meta")
})

public class VM extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(VM.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "vm_name");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        VMConstants.VMMetricContext ctx = VMConstants.createMapperContext();
        handleVM(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("vm_name")).split("[\\t]++");
        if (header.length != VM_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(VM_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + VM_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleVM(String[] data, ParallelCollector collector, VMConstants.VMMetricContext ctx) {
        Map<String, AccountingReadSpecification> vmElements = AccountingReadSpecification.fromReadWritemap(vmReadSpec);
        for (int i = 1; i < data.length; i++) {
            handleVMSequence(data[i], vmElements, i);
        }
        collector.addData(vmElements).addMappings(VMConstants.getVMMappers(ctx)).addMappings(VMConstants.getVMHistoryMappers(ctx));
    }

    private boolean handleVMSequence(String fsInput, Map<String, AccountingReadSpecification> vmElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < VM_COLUMN_NAMES.length; i++) {
            String key = VM_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = vmElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, vmElements);
            }
        }
        return true;
    }

}
