/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.h;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseMappingSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.PlainValueRowGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.TimestampedRowGenerator;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author atsi
 */
public class HardwareConstants {

    public static final String CPU_ARCH = "cpu_arch";
    public static final String CPU_CORES = "cpu_cores";
    public static final String CPU_FREQ = "cpu_freq";
    public static final String MEM_FREQ = "mem_freq";
    public static final String MEM_SIZE = "mem_size";
    public static final String NETW_SPEED = "netw_speed";

    static final String[] HARDWARE_HEADER_SPLIT_COLUMNS = {
        CPU_ARCH, CPU_CORES, CPU_FREQ, MEM_FREQ, MEM_SIZE, NETW_SPEED
    };

    static final String[] HARDWARE_COLUMN_NAMES = {
        CPU_ARCH, CPU_CORES, CPU_FREQ, MEM_FREQ, MEM_SIZE, NETW_SPEED
    };

    static final HardwareMetricContext createMapperContext() {
        return new HardwareMetricContext();
    }

    final static class HardwareMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> hardwareReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(CPU_ARCH, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(CPU_CORES, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(CPU_FREQ, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(MEM_FREQ, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(MEM_SIZE, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(NETW_SPEED, String.class, ComputeType.IS_SINGLETON)}, hardwareReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_ARCH, CN_TABLE, "hardware", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_CORES, CN_TABLE, "hardware", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_FREQ, CN_TABLE, "hardware", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_FREQ, CN_TABLE, "hardware", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_SIZE, CN_TABLE, "hardware", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NETW_SPEED, CN_TABLE, "network", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_ARCH, CN_HISTORY_TABLE, "hardware", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_CORES, CN_HISTORY_TABLE, "hardware", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_FREQ, CN_HISTORY_TABLE, "hardware", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_FREQ, CN_HISTORY_TABLE, "hardware", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_SIZE, CN_HISTORY_TABLE, "hardware", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NETW_SPEED, CN_HISTORY_TABLE, "network", ctx.time));
    }
}
