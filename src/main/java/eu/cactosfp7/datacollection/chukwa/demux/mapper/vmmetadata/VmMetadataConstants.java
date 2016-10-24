/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.vmmetadata;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_HISTORY_TABLE;
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
public class VmMetadataConstants {

    public static final String VM_ID = "VMID";
    public static final String APPLICATION_TYPE = "applicationType";
    public static final String APPLICATION_TYPE_INSTANCE = "applicationTypeInstance";
    public static final String APPLICATION_COMPONENT = "applicationComponent";
    public static final String APPLICATION_COMPONENT_INSTANCE = "applicationComponentInstance";

    static final String[] VM_METADATA_HEADER_SPLIT_COLUMNS = {
        VM_ID, APPLICATION_TYPE, APPLICATION_TYPE_INSTANCE, APPLICATION_COMPONENT, APPLICATION_COMPONENT_INSTANCE
    };

    static final String[] VM_METADATA_COLUMN_NAMES = {
        VM_ID, APPLICATION_TYPE, APPLICATION_TYPE_INSTANCE, APPLICATION_COMPONENT, APPLICATION_COMPONENT_INSTANCE
    };

    static final VmMetadataMetricContext createMapperContext() {
        return new VmMetadataMetricContext();
    }

    final static class VmMetadataMetricContext {

        final RowKeyGenerator time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, VM_ID);
        final RowKeyGenerator plain = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, VM_ID);
    }

    static final Map<String, SourceReadSpecification> vmMetadataReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(VM_ID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(APPLICATION_TYPE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(APPLICATION_TYPE_INSTANCE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(APPLICATION_COMPONENT, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(APPLICATION_COMPONENT_INSTANCE, String.class, ComputeType.ONLY_PARTS)
        }, vmMetadataReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getVmMetadataMappers(VmMetadataMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_TYPE, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_TYPE_INSTANCE, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_COMPONENT, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_COMPONENT_INSTANCE, VM_TABLE, "meta", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getVMHistoryMappers(VmMetadataMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_TYPE, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_TYPE_INSTANCE, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_COMPONENT, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPLICATION_COMPONENT_INSTANCE, VM_HISTORY_TABLE, "meta", ctx.time)
                );
    }
}
