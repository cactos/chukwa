/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hdisk;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.ST_HISTORY_TABLE;
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
public class HDiskConstants {

    public static final String DISK_NAME = "disk_name";
    public static final String DISK_PARENT = "disk_parent";
    public static final String DISK_TYPE = "disk_type";
    public static final String DISK_SIZE = "disk_size";
    public static final String DISK_DEVICE = "disk_device";
    public static final String DISK_MOUNT = "disk_mount";

    static final String[] HDISK_HEADER_SPLIT_COLUMNS = {
        DISK_NAME,
        DISK_PARENT,
        DISK_TYPE,
        DISK_SIZE,
        DISK_DEVICE,
        DISK_MOUNT
    };

    static final String[] HDISK_COLUMN_NAMES = {
        DISK_NAME,
        DISK_PARENT,
        DISK_TYPE,
        DISK_SIZE,
        DISK_DEVICE,
        DISK_MOUNT
    };

    static final HDiskMetricContext createMapperContext() {
        return new HDiskMetricContext();
    }

    final static class HDiskMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> hdiskReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(DISK_NAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_PARENT, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_TYPE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_SIZE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_DEVICE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_MOUNT, String.class, ComputeType.ONLY_PARTS)}, hdiskReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHDiskMappers(HDiskMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_NAME, CN_TABLE, "storage", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_PARENT, CN_TABLE, "storage", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_TYPE, CN_TABLE, "storage", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_SIZE, CN_TABLE, "storage", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_DEVICE, CN_TABLE, "storage", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_MOUNT, CN_TABLE, "storage", ctx.plain)
                );
    }
    
        static MapperCollection<HBaseMappingSpecification> getHDiskMappersForStorage(HDiskMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_NAME, ST_TABLE, "spec", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_PARENT, ST_TABLE, "spec", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_TYPE, ST_TABLE, "spec", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_SIZE, ST_TABLE, "spec", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_DEVICE, ST_TABLE, "spec", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_MOUNT, ST_TABLE, "spec", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHDiskMappers(HDiskMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_NAME, CN_HISTORY_TABLE, "storage", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_PARENT, CN_HISTORY_TABLE, "storage", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_TYPE, CN_HISTORY_TABLE, "storage", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_SIZE, CN_HISTORY_TABLE, "storage", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_DEVICE, CN_HISTORY_TABLE, "storage", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_MOUNT, CN_HISTORY_TABLE, "storage", ctx.time)
                );
    }
    
        static MapperCollection<HBaseMappingSpecification> getHistoryHDiskMappersForStorage(HDiskMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_NAME, ST_HISTORY_TABLE, "spec", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_PARENT, ST_HISTORY_TABLE, "spec", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_TYPE, ST_HISTORY_TABLE, "spec", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_SIZE, ST_HISTORY_TABLE, "spec", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_DEVICE, ST_HISTORY_TABLE, "spec", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_MOUNT, ST_HISTORY_TABLE, "spec", ctx.time)
                );
    }
}
