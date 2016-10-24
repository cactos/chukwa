/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.vm;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;
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
public class VMConstants {

    public static final String VM_NAME = "vm_name";
    public static final String VM_UUID = "vm_uuid";
    public static final String VM_IMAGE_UUID = "vm_image_uuid";
    public static final String VM_TENANT_UUID = "vm_tenant_uuid";
    public static final String VM_STATE = "vm_state";

    static final String[] VM_HEADER_SPLIT_COLUMNS = {
        VM_NAME, VM_UUID, VM_IMAGE_UUID, VM_TENANT_UUID, VM_STATE
    };

    static final String[] VM_COLUMN_NAMES = {
        VM_NAME, VM_UUID, VM_IMAGE_UUID, VM_TENANT_UUID, VM_STATE
    };

    static final VMMetricContext createMapperContext() {
        return new VMMetricContext();
    }

    final static class VMMetricContext {

        final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
        final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
        
        final RowKeyGenerator mod_plain = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, VM_UUID);
        final RowKeyGenerator mod_time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, VM_UUID);
    }

    static final Map<String, SourceReadSpecification> vmReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(VM_NAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VM_UUID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VM_IMAGE_UUID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VM_TENANT_UUID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VM_STATE, String.class, ComputeType.ONLY_PARTS)}, vmReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getVMMappers(VMMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_NAME, CN_TABLE, "vms", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_UUID, CN_TABLE, "vms", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_IMAGE_UUID, CN_TABLE, "vms", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_TENANT_UUID, CN_TABLE, "vms", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_STATE, VM_TABLE, "meta", ctx.mod_plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getVMHistoryMappers(VMMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_NAME, CN_HISTORY_TABLE, "vms", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_UUID, CN_HISTORY_TABLE, "vms", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_IMAGE_UUID, CN_HISTORY_TABLE, "vms", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_TENANT_UUID, CN_HISTORY_TABLE, "vms", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_STATE, VM_HISTORY_TABLE, "meta", ctx.mod_time)
                );
    }
}
