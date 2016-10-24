/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.visor;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_APP_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
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
public class VisorConstants {

    private final String VM_ID;
    private final String METRIC_NAME;
    private final String VISOR_TIMESTAMP;
    private final String TAGS;
    public final String[] VISOR_HEADER_SPLIT_COLUMNS;
    public final String[] VISOR_COLUMN_NAMES;
    final Map<String, SourceReadSpecification> visorReadSpec = new HashMap<String, SourceReadSpecification>();

    public VisorConstants(String vmId, String metricName, String visorTimestamp, String tags) {
        this.VM_ID = vmId;
        this.METRIC_NAME = metricName;
        this.VISOR_TIMESTAMP = visorTimestamp;
        this.TAGS = tags;
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(VM_ID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(METRIC_NAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VISOR_TIMESTAMP, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(TAGS, String.class, ComputeType.ONLY_PARTS)
        }, visorReadSpec);
        this.VISOR_HEADER_SPLIT_COLUMNS = new String[]{
            VM_ID, METRIC_NAME, VISOR_TIMESTAMP, TAGS
        };

        this.VISOR_COLUMN_NAMES = new String[]{
            VM_ID, METRIC_NAME, VISOR_TIMESTAMP, TAGS
        };
    }

    final VisorMetricContext createMapperContext() {
        return new VisorMetricContext();
    }

    final class VisorMetricContext {

        final RowKeyGenerator time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, VM_ID);
        final RowKeyGenerator plain = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, VM_ID);
    }

    MapperCollection<HBaseMappingSpecification> getVMMappers(VisorMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_TABLE, "app", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(METRIC_NAME, VM_TABLE, "app", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VISOR_TIMESTAMP, VM_TABLE, "app", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TAGS, VM_TABLE, "app", ctx.plain)
                );
    }

    MapperCollection<HBaseMappingSpecification> getVMAppHistoryMappers(VisorMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_APP_HISTORY_TABLE, "app", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(METRIC_NAME, VM_APP_HISTORY_TABLE, "app", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VISOR_TIMESTAMP, VM_APP_HISTORY_TABLE, "app", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TAGS, VM_APP_HISTORY_TABLE, "app", ctx.time)
                );
    }

}
