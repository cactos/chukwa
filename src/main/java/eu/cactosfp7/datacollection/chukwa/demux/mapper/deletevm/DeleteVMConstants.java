/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.deletevm;

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
public class DeleteVMConstants {

    public static final String VM_ID = "VMID";
    public static final String IS_DELETED = "isDeleted";

    static final String[] DELETE_VM_HEADER_SPLIT_COLUMNS = {
        VM_ID, IS_DELETED
    };

    static final String[] DELETE_VM_COLUMN_NAMES = {
        VM_ID, IS_DELETED
    };

    static final DeleteVMMetricContext createMapperContext() {
        return new DeleteVMMetricContext();
    }

    final static class DeleteVMMetricContext {

        final RowKeyGenerator time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, VM_ID);
        final RowKeyGenerator plain = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, VM_ID);
    }

    static final Map<String, SourceReadSpecification> deleteVMReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(VM_ID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(IS_DELETED, String.class, ComputeType.ONLY_PARTS)
        }, deleteVMReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getDeleteVMMappers(DeleteVMMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(IS_DELETED, VM_TABLE, "meta", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getDeleteVMHistoryMappers(DeleteVMMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(IS_DELETED, VM_HISTORY_TABLE, "meta", ctx.time)
                );
    }
}
