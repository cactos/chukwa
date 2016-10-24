/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hfilesystem;

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
public class HFilesystemConstants {

    public static final String MOUNT = "mount";
    public static final String TYPE = "type";
    public static final String AVAILABLE = "available";
    public static final String USED = "used";
    public static final String READBANDMAX = "readbandmax";
    public static final String WRITEBANDMAX = "writebandmax";

    static final String[] HFILESYSTEM_HEADER_SPLIT_COLUMNS = {
        MOUNT,
        TYPE,
        AVAILABLE,
        USED,
        READBANDMAX,
        WRITEBANDMAX
    };

    static final String[] HFILESYSTEM_COLUMN_NAMES = {
        MOUNT,
        TYPE,
        AVAILABLE,
        USED,
        READBANDMAX,
        WRITEBANDMAX
    };

    static final HFilesystemMetricContext createMapperContext() {
        return new HFilesystemMetricContext();
    }

    final static class HFilesystemMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> hFilesystemReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(MOUNT, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(TYPE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(AVAILABLE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(USED, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(READBANDMAX, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(WRITEBANDMAX, String.class, ComputeType.ONLY_PARTS)}, hFilesystemReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHFilesystemMappers(HFilesystemMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNT, CN_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TYPE, CN_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(AVAILABLE, CN_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(USED, CN_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(READBANDMAX, CN_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(WRITEBANDMAX, CN_TABLE, "filesystem", ctx.plain)
                );
    }
    static MapperCollection<HBaseMappingSpecification> getHFilesystemMappersForStorage(HFilesystemMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNT, ST_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TYPE, ST_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(AVAILABLE, ST_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(USED, ST_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(READBANDMAX, ST_TABLE, "filesystem", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(WRITEBANDMAX, ST_TABLE, "filesystem", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHFilesystemMappers(HFilesystemMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNT, CN_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TYPE, CN_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(AVAILABLE, CN_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(USED, CN_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(READBANDMAX, CN_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(WRITEBANDMAX, CN_HISTORY_TABLE, "filesystem", ctx.time));
    }
    static MapperCollection<HBaseMappingSpecification> getHistoryHFilesystemMappersForStorage(HFilesystemMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOUNT, ST_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TYPE, ST_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(AVAILABLE, ST_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(USED, ST_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(READBANDMAX, ST_HISTORY_TABLE, "filesystem", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(WRITEBANDMAX, ST_HISTORY_TABLE, "filesystem", ctx.time));
    }
}
