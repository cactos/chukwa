/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hutil;

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
public class HUtilConstants {

    public static final String CPU_USR = "cpu_usr";
    public static final String CPU_SYS = "cpu_sys";
    public static final String CPU_WIO = "cpu_wio";
    public static final String MEM_FREE = "mem_free";
    public static final String MEM_CACHE = "mem_cache";
    public static final String MEM_BUFF = "mem_buff";
    public static final String MEM_SWPD = "mem_swpd";
    public static final String NET_THROUGH = "net_through";

    static final String[] HUTIL_HEADER_SPLIT_COLUMNS = {
        CPU_USR,
        CPU_SYS,
        CPU_WIO,
        MEM_FREE,
        MEM_CACHE,
        MEM_BUFF,
        MEM_SWPD,
        NET_THROUGH
    };

    static final String[] HUTIL_COLUMN_NAMES = {
        CPU_USR,
        CPU_SYS,
        CPU_WIO,
        MEM_FREE,
        MEM_CACHE,
        MEM_BUFF,
        MEM_SWPD,
        NET_THROUGH
    };

    static final HUtilMetricContext createMapperContext() {
        return new HUtilMetricContext();
    }

    final static class HUtilMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> hUtilReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(CPU_USR, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(CPU_SYS, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(CPU_WIO, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(MEM_FREE, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(MEM_CACHE, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(MEM_BUFF, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(MEM_SWPD, String.class, ComputeType.IS_SINGLETON),
            new SourceReadSpecification(NET_THROUGH, String.class, ComputeType.IS_SINGLETON)
        }, hUtilReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHUtilMappers(HUtilMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_USR, CN_TABLE, "hardware_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_SYS, CN_TABLE, "hardware_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_WIO, CN_TABLE, "hardware_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_FREE, CN_TABLE, "hardware_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_CACHE, CN_TABLE, "hardware_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_BUFF, CN_TABLE, "hardware_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_SWPD, CN_TABLE, "hardware_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_THROUGH, CN_TABLE, "network_util", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHUtilMappers(HUtilMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_USR, CN_HISTORY_TABLE, "hardware_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_SYS, CN_HISTORY_TABLE, "hardware_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_WIO, CN_HISTORY_TABLE, "hardware_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_FREE, CN_HISTORY_TABLE, "hardware_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_CACHE, CN_HISTORY_TABLE, "hardware_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_BUFF, CN_HISTORY_TABLE, "hardware_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_SWPD, CN_HISTORY_TABLE, "hardware_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_THROUGH, CN_HISTORY_TABLE, "network_util", ctx.time)
                );
    }
}
