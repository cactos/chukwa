/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hdiskutil;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_TABLE;
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
public class HDiskUtilConstants {

    public static final String DEVICE = "Device:";
    public static final String TPS = "tps";
    public static final String KB_READ_P_SEC = "kB_read/s";
    public static final String KB_WRITE_P_SEC = "kB_wrtn/s";
    public static final String KB_READ = "kB_read";
    public static final String KB_WRITE = "kB_wrtn";
    public static final String MOUNTPOINT = "Mountpoint";

    static final String[] HDISKUTIL_HEADER_SPLIT_COLUMNS = {
        DEVICE,
        TPS,
        KB_READ_P_SEC,
        KB_WRITE_P_SEC,
        KB_READ,
        KB_WRITE,
        MOUNTPOINT
    };

    static final String[] HDISKUTIL_COLUMN_NAMES = {
        DEVICE,
        TPS,
        KB_READ_P_SEC,
        KB_WRITE_P_SEC,
        KB_READ,
        KB_WRITE,
        MOUNTPOINT
    };

    static final HDiskUtilMetricContext createMapperContext() {
        return new HDiskUtilMetricContext();
    }

    final static class HDiskUtilMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> hdiskutilReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(DEVICE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(TPS, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(KB_READ_P_SEC, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(KB_WRITE_P_SEC, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(KB_READ, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(KB_WRITE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(MOUNTPOINT, String.class, ComputeType.ONLY_PARTS)
        }, hdiskutilReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHDiskUtilMappersForStorage(HDiskUtilMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DEVICE, ST_TABLE, "util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TPS, ST_TABLE, "util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ_P_SEC, ST_TABLE, "util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE_P_SEC, ST_TABLE, "util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ, ST_TABLE, "util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE, ST_TABLE, "util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNTPOINT, ST_TABLE, "util", ctx.plain)
                );
    }
    static MapperCollection<HBaseMappingSpecification> getHDiskUtilMappers(HDiskUtilMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DEVICE, CN_TABLE, "storage_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TPS, CN_TABLE, "storage_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ_P_SEC, CN_TABLE, "storage_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE_P_SEC, CN_TABLE, "storage_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ, CN_TABLE, "storage_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE, CN_TABLE, "storage_util", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNTPOINT, CN_TABLE, "storage_util", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHDiskUtilMappersForStorage(HDiskUtilMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DEVICE, ST_HISTORY_TABLE, "util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TPS, ST_HISTORY_TABLE, "util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ_P_SEC, ST_HISTORY_TABLE, "util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE_P_SEC, ST_HISTORY_TABLE, "util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ, ST_HISTORY_TABLE, "util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE, ST_HISTORY_TABLE, "util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNTPOINT, ST_HISTORY_TABLE, "util", ctx.time)
                );
    }
    static MapperCollection<HBaseMappingSpecification> getHistoryHDiskUtilMappers(HDiskUtilMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DEVICE, CN_HISTORY_TABLE, "storage_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TPS, CN_HISTORY_TABLE, "storage_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ_P_SEC, CN_HISTORY_TABLE, "storage_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE_P_SEC, CN_HISTORY_TABLE, "storage_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_READ, CN_HISTORY_TABLE, "storage_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(KB_WRITE, CN_HISTORY_TABLE, "storage_util", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNTPOINT, CN_HISTORY_TABLE, "storage_util", ctx.time)
                );
    }
}
