/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.ipmi;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.INDEX_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SYSTEM_METRICS;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.ipmi.IpmiConstants.IPMI_HOST_ELEMENTS;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ipmi.IpmiConstants.IpmiMetricContext;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.ipmi.IpmiConstants.ipmiReadSpec;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

@Annotation.Tables(annotations = {
    @Annotation.Table(name = SYSTEM_METRICS, columnFamily = "Ipmi"),
    @Annotation.Table(name = INDEX_TABLE, columnFamily = "Ipmi")
})
public class Ipmi extends AbstractProcessorNG {

    static Logger log = Logger.getLogger(Ipmi.class);

    private boolean verifyHeaderElements(String headerLine) {
        String[] header = headerLine.substring(headerLine.indexOf("Current Power")).split(",");
        if (header.length != IPMI_HOST_ELEMENTS.length) {
            log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
            return false;
        }
        int i = AbstractProcessorConstants.compareArrays(IPMI_HOST_ELEMENTS, header, 0);
        if (i < 0) {
            log.warn("host header element at index " + i + " does not match: " + IPMI_HOST_ELEMENTS[i] + " vs " + header[i]);
            return false;
        }
        return true;
    }

    private void handleConsumptions(String ipmiInput, ParallelCollector collector, IpmiMetricContext imc) {
        Map<String, AccountingReadSpecification> ipmiElements = AccountingReadSpecification.fromReadWritemap(ipmiReadSpec);

        String[] data = ipmiInput.split(",");
        for (int i = 0; i < IPMI_HOST_ELEMENTS.length; i++) {
            String key = IPMI_HOST_ELEMENTS[i];
            String value = data[i];
            AccountingReadSpecification spec = ipmiElements.get(key);
            if (spec == null) {
                log.info("ignoring non-specified key: " + key);
            } else {
                AbstractProcessorConstants.addSingleValueAtIndex(key, i, value, ipmiElements);
            }
        }

        collector.addData(ipmiElements).addMappings(IpmiConstants.getIpmiMappers(imc));
    }

    @Override
    protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {
        AbstractProcessorConstants.setTimestamp(recordEntry, collector);
        String body = AbstractProcessorConstants.skipStartSequence(recordEntry, "Current Power");
        String[] lines = body.split("\n");
        StringBuilder headerBuilder = new StringBuilder();
        StringBuilder dataBuilder = new StringBuilder();
        String[] values = null;
        String[] results = null;
        for (String line : lines) {
            values = null;
            if (!line.isEmpty()) {
                values = line.split(":");
            }
            if (values == null) {
                continue;
            }
            headerBuilder.append(values[0].trim()).append(",");
            results = values[1].trim().split("\\s");
            dataBuilder.append(results[0].trim()).append(",");
        }
        String headers = headerBuilder.substring(0, headerBuilder.length() - 1);
        String data = dataBuilder.substring(0, dataBuilder.length() - 1);
        if (!verifyHeaderElements(headers)) {
            return;
        }
        IpmiMetricContext imc = IpmiConstants.createMapperContext();
        handleConsumptions(data, collector, imc);
    }

}
