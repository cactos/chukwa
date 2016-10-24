/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.ipmi;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.INDEX_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SYSTEM_METRICS;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseMappingSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.PlainValueRowGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.TimestampedRowGenerator;
import java.util.HashMap;
import java.util.Map;

public class IpmiConstants {

    public static final String CURRENT_POWER = "Current Power";
    public static final String MINIMUM_POWER = "Minumum Power";
    public static final String MAXIMUM_POWER = "Maximum Power";
    public static final String AVERAGE_POWER = "Average Power";
    public static final String POWER_STATISTICS_REPORTING_PERIOD = "Power Statistics Reporting Period";
    public static final String POWER_GLOBAL_ADMINISTRATIVE_STATE = "Power Global Administrative State";
    public static final String POWER_MEASUREMENTS_STATE = "Power Measurements State";
    public static final String CURRENT_INLET_TEMPERATURE = "Current Inlet Temperature";
    public static final String MINIMUM_INLET_TEMPERATURE = "Minimum Inlet Temperature";
    public static final String MAXIMUM_INLET_TEMPERATURE = "Maximum Inlet Temperature";
    public static final String AVERAGE_INLET_TEMPERATURE = "Average Inlet Temperature";
    public static final String INLET_TEMP_STAT_PERIOD = "Inlet Temperature Statistics Reporting Period";
    public static final String INLET_TEMP_ADMIN_STATE = "Inlet Temperature Global Administrative State";
    public static final String INLET_TEMP_MEASUREMENTS_STATE = "Inlet Temperature Measurements State";

    static final String[] IPMI_HOST_ELEMENTS = {
        CURRENT_POWER, MINIMUM_POWER, MAXIMUM_POWER, AVERAGE_POWER, POWER_STATISTICS_REPORTING_PERIOD, POWER_GLOBAL_ADMINISTRATIVE_STATE,
        POWER_MEASUREMENTS_STATE, CURRENT_INLET_TEMPERATURE, MINIMUM_INLET_TEMPERATURE, MAXIMUM_INLET_TEMPERATURE, AVERAGE_INLET_TEMPERATURE,
        INLET_TEMP_STAT_PERIOD, INLET_TEMP_ADMIN_STATE, INLET_TEMP_MEASUREMENTS_STATE
    };

    static final IpmiMetricContext createMapperContext() {
        return new IpmiMetricContext();
    }

    final static class IpmiMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
    }

    static final Map<String, SourceReadSpecification> ipmiReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(
                new SourceReadSpecification[]{
                    new SourceReadSpecification(CURRENT_POWER, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(MINIMUM_POWER, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(MAXIMUM_POWER, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(AVERAGE_POWER, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(POWER_STATISTICS_REPORTING_PERIOD, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(POWER_GLOBAL_ADMINISTRATIVE_STATE, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(POWER_MEASUREMENTS_STATE, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(CURRENT_INLET_TEMPERATURE, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(MINIMUM_INLET_TEMPERATURE, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(MAXIMUM_INLET_TEMPERATURE, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(AVERAGE_INLET_TEMPERATURE, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(INLET_TEMP_STAT_PERIOD, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(INLET_TEMP_ADMIN_STATE, String.class, ComputeType.IS_SINGLETON),
                    new SourceReadSpecification(INLET_TEMP_MEASUREMENTS_STATE, String.class, ComputeType.IS_SINGLETON)
                }, ipmiReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getIpmiMappers(IpmiMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CURRENT_POWER, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MINIMUM_POWER, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MAXIMUM_POWER, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(AVERAGE_POWER, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_STATISTICS_REPORTING_PERIOD, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_GLOBAL_ADMINISTRATIVE_STATE, INDEX_TABLE, "ipmi", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(POWER_MEASUREMENTS_STATE, INDEX_TABLE, "ipmi", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CURRENT_INLET_TEMPERATURE, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MINIMUM_INLET_TEMPERATURE, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MAXIMUM_INLET_TEMPERATURE, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(AVERAGE_INLET_TEMPERATURE, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(INLET_TEMP_STAT_PERIOD, SYSTEM_METRICS, "ipmi", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(INLET_TEMP_ADMIN_STATE, INDEX_TABLE, "ipmi", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(INLET_TEMP_MEASUREMENTS_STATE, INDEX_TABLE, "ipmi", ctx.plain));
    }
}
