/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.nodecpuwio;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseMappingSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.PlainValueRowGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.SplittingRowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.TimestampedRowGenerator;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author atsi
 */
public class HardwareConstants {

    public static final String CPU_CORES = "cpu_cores";
    public static final String NODE_NAME = "node_name";
    public static final String CPU_USR = "cpu_wio";

    static final String[] HARDWARE_HEADER_SPLIT_COLUMNS = {
        CPU_CORES, NODE_NAME, CPU_USR
    };

    static final String[] HARDWARE_COLUMN_NAMES = {
        CPU_CORES, NODE_NAME, CPU_USR
    };

    static final HardwareMetricContext createMapperContext() {
        return new HardwareMetricContext();
    }

    final static class HardwareMetricContext {

    	 final RowKeyGenerator multiplex_time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, NODE_NAME);
         final RowKeyGenerator multiplex = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, NODE_NAME);
    }

    static final Map<String, SourceReadSpecification> hardwareReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(CPU_CORES, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(NODE_NAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(CPU_USR, String.class, ComputeType.ONLY_PARTS)}, hardwareReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_CORES,CN_TABLE, "hardware", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_USR, CN_TABLE, "hardware_util", ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_CORES, CN_HISTORY_TABLE, "hardware", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_USR, CN_HISTORY_TABLE, "hardware_util", ctx.multiplex_time));
    }
}
