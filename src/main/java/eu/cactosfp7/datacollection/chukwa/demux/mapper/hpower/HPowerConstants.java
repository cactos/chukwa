/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hpower;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
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
public class HPowerConstants {

    public static final String POWER_SERIAL = "serial";
    public static final String POWER_CAPACITY = "capacity";

    static final String[] POWER_HEADER_SPLIT_COLUMNS = {
        POWER_SERIAL, POWER_CAPACITY
    };

    static final String[] POWER_COLUMN_NAMES = {
        POWER_SERIAL, POWER_CAPACITY
    };

    static final PowerMetricContext createMapperContext() {
        return new PowerMetricContext();
    }

    final static class PowerMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> hPowerReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(POWER_SERIAL, String.class, SourceReadSpecification.ComputeType.ONLY_PARTS),
            new SourceReadSpecification(POWER_CAPACITY, String.class, SourceReadSpecification.ComputeType.ONLY_PARTS)}, hPowerReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getPowerMappers(PowerMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_SERIAL, CN_TABLE, "power", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_CAPACITY, CN_TABLE, "power", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryPowerMappers(PowerMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_SERIAL, CN_HISTORY_TABLE, "power", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_CAPACITY, CN_HISTORY_TABLE, "power", ctx.time));
    }
}
