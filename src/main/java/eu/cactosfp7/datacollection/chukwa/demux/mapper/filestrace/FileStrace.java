/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.filestrace;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_APP_HISTORY_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.filestrace.FileStraceConstants.FILESTRACE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.filestrace.FileStraceConstants.FILESTRACE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.filestrace.FileStraceConstants.filestraceReadSpec;
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
    @Table(name = VM_APP_HISTORY_TABLE, columnFamily = "file")
})

public class FileStrace extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(FileStrace.class);

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "MolproState");

        String[] lines = body.split("\n");
//        if (!verifyHeaderElements(lines[0])) {
//            return;
//        }
        FileStraceConstants.FileStraceMetricContext ctx = FileStraceConstants.createMapperContext();
        handleFileStrace(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("MolproState")).split("[\\t]++");
        if (header.length != FILESTRACE_HEADER_SPLIT_COLUMNS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(FILESTRACE_HEADER_SPLIT_COLUMNS, header, 0);
        if (i > -1) {
            log.warn("host header element at index " + i + " does not match: " + FILESTRACE_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleFileStrace(String[] data, ParallelCollector collector, FileStraceConstants.FileStraceMetricContext ctx) {
        Map<String, AccountingReadSpecification> filestraceElements = AccountingReadSpecification.fromReadWritemap(filestraceReadSpec);
        AbstractProcessorConstants.addSingleValue("vm_id", this.chunk.getTag("vmUUID"), filestraceElements);
        for (int i = 1; i < data.length; i++) {
            handleFileStraceSequence(data[i], filestraceElements, i);
        }
        collector.addData(filestraceElements).addMappings(FileStraceConstants.getFileStraceMappers(ctx));
    }

    private boolean handleFileStraceSequence(String fsInput, Map<String, AccountingReadSpecification> filestraceElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < FILESTRACE_COLUMN_NAMES.length; i++) {
            String key = FILESTRACE_COLUMN_NAMES[i];
            String value = data[i];
            AccountingReadSpecification spec = filestraceElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex("MolproState", iteration, value, filestraceElements);
            }
        }
        return true;
    }

}
