/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.kvmtop;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.kvmtop.KvmTopConstants.KVMTOP_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.kvmtop.KvmTopConstants.KVMTOP_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.kvmtop.KvmTopConstants.kvmtopReadSpec;
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
    @Table(name = VM_TABLE, columnFamily = "hardware"),
    @Table(name = VM_TABLE, columnFamily = "storage"),
    @Table(name = VM_TABLE, columnFamily = "network"),
    @Table(name = VM_HISTORY_TABLE, columnFamily = "hardware"),
    @Table(name = VM_HISTORY_TABLE, columnFamily = "storage"),
    @Table(name = VM_HISTORY_TABLE, columnFamily = "network")
})
public class KvmTop extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(KvmTop.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "vmname");

        String[] lines = body.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        KvmTopConstants.KvmTopMetricContext ctx = KvmTopConstants.createMapperContext();
        handleKvmTop(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("vmname")).split("[\\t]++");
        if (header.length != KVMTOP_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(KVMTOP_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + KVMTOP_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleKvmTop(String[] data, ParallelCollector collector, KvmTopConstants.KvmTopMetricContext ctx) {
        Map<String, AccountingReadSpecification> kvmtopElements = AccountingReadSpecification.fromReadWritemap(kvmtopReadSpec);

        for (int i = 1; i < data.length; i++) {
            handleKvmTopSequence(data[i], kvmtopElements, i);
        }
        collector.addData(kvmtopElements).addMappings(KvmTopConstants.getKvmTopMappers(ctx)).addMappings(KvmTopConstants.getHistoryKvmTopMappers(ctx));
    }

    private boolean handleKvmTopSequence(String fsInput, Map<String, AccountingReadSpecification> kvmtopElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < KVMTOP_COLUMN_NAMES.length; i++) {
            String key = KVMTOP_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = kvmtopElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, kvmtopElements);
            }
        }
        return true;
    }

}
