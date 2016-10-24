/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.visor;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_APP_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.visor.VisorConstants.VisorMetricContext;
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
    @Table(name = VM_TABLE, columnFamily = "app"),
    @Table(name = VM_APP_HISTORY_TABLE, columnFamily = "app")
})

public class Visor extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(Visor.class);
    private VisorConstants constants = null;

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String[] lines = recordEntry.split("\n");
        if (!verifyHeaderElements(lines[0])) {
            return;
        }
        VisorMetricContext ctx = constants.createMapperContext();
        handleVisor(lines, collector, ctx);
    }

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("VMID")).split("[\\t]++");
        constants = new VisorConstants(header[0], header[1], header[2], header[3]);
        return true;
    }

    private void handleVisor(String[] data, ParallelCollector collector, VisorMetricContext ctx) {
        Map<String, AccountingReadSpecification> visorElements = AccountingReadSpecification.fromReadWritemap(constants.visorReadSpec);
        for (int i = 1; i < data.length; i++) {
            handleVisorSequence(data[i], visorElements, i);
        }
        collector.addData(visorElements).addMappings(constants.getVMAppHistoryMappers(ctx)).addMappings(constants.getVMMappers(ctx));
    }

    private boolean handleVisorSequence(String fsInput, Map<String, AccountingReadSpecification> vmElements, int iteration) {
        String[] data = fsInput.split("[\\t]++");
        for (int i = 0; i < constants.VISOR_COLUMN_NAMES.length; i++) {
            String key = constants.VISOR_COLUMN_NAMES[i];
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
