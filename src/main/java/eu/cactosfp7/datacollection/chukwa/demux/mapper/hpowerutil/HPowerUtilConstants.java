/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.hpowerutil;

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
public class HPowerUtilConstants {

    public static final String POWER_CONSUMPTION = "consumption";

    static final String[] POWER_UTIL_HEADER_SPLIT_COLUMNS = {
        POWER_CONSUMPTION
    };

    static final String[] POWER_UTIL_COLUMN_NAMES = {
        POWER_CONSUMPTION
    };

    static final PowerMetricContext createMapperContext() {
        return new PowerMetricContext();
    }

    final static class PowerMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> hPowerUtilReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(POWER_CONSUMPTION, String.class, SourceReadSpecification.ComputeType.IS_SINGLETON)}, hPowerUtilReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getPowerUtilMappers(PowerMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_CONSUMPTION, CN_TABLE, "power_util", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryPowerUtilMappers(PowerMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_CONSUMPTION ,CN_HISTORY_TABLE, "power_util", ctx.time));
    }
}
