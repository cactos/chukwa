/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.vmsigar;

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
 * @author CACTOS
 */
public class VMSigarConstants {

    public static final String VMNAME = "vmname";
    public static final String UUID = "UUID";
    public static final String VCORES = "CpuCS";
    public static final String VCORES_USAGE = "CpuVM";
    public static final String RAM_USED = "ram-used";
    public static final String RAM_TOTAL = "ram-total";
    public static final String NETWORK = "network";
    public static final String DISK_USED = "disk-used";
    public static final String DISK_TOTAL = "disk-total";
    public static final String DISK_READ = "disk-read";
    public static final String DISK_WRITE = "disk-write";
    public static final String VM_STATUS = "vm_state";

    static final String[] KVMTOP_HEADER_SPLIT_COLUMNS = {
        VMNAME,
        UUID,
        VCORES,
        VCORES_USAGE,
        RAM_USED,
        RAM_TOTAL,
        NETWORK,
        DISK_USED,
        DISK_TOTAL,
        DISK_READ,
        DISK_WRITE,
        VM_STATUS
    };

    static final String[] KVMTOP_COLUMN_NAMES = {
        VMNAME,
        UUID,
        VCORES,
        VCORES_USAGE,
        RAM_USED,
        RAM_TOTAL,
        NETWORK,
        DISK_USED,
        DISK_TOTAL,
        DISK_READ,
        DISK_WRITE,
        VM_STATUS
    };

    static final KvmTopMetricContext createMapperContext() {
        return new KvmTopMetricContext();
    }

    final static class KvmTopMetricContext {

        final RowKeyGenerator multiplex_time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, UUID);
        final RowKeyGenerator multiplex = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, UUID);
    }

    static final Map<String, SourceReadSpecification> kvmtopReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(VMNAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(UUID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VCORES, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VCORES_USAGE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(RAM_USED, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(RAM_TOTAL, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(NETWORK, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_USED, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_TOTAL, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_READ, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(DISK_WRITE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VM_STATUS, String.class, ComputeType.ONLY_PARTS)
        }, kvmtopReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getKvmTopMappers(KvmTopMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMNAME, VM_TABLE, "hardware", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(UUID, VM_TABLE, "hardware", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VCORES, VM_TABLE, "hardware", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VCORES_USAGE, VM_TABLE, "hardware", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(RAM_USED, VM_TABLE, "hardware", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(RAM_TOTAL, VM_TABLE, "hardware", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NETWORK, VM_TABLE, "network", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_USED, VM_TABLE, "storage", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_TOTAL, VM_TABLE, "storage", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_READ, VM_TABLE, "storage", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_WRITE, VM_TABLE, "storage", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_STATUS, VM_TABLE, "meta", ctx.multiplex)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryKvmTopMappers(KvmTopMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMNAME, VM_HISTORY_TABLE, "hardware", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(UUID, VM_HISTORY_TABLE, "hardware", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VCORES, VM_HISTORY_TABLE, "hardware", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VCORES_USAGE, VM_HISTORY_TABLE, "hardware", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(RAM_USED, VM_HISTORY_TABLE, "hardware", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(RAM_TOTAL, VM_HISTORY_TABLE, "hardware", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NETWORK, VM_HISTORY_TABLE, "network", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_USED, VM_HISTORY_TABLE, "storage", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_TOTAL, VM_HISTORY_TABLE, "storage", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_READ, VM_HISTORY_TABLE, "storage", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DISK_WRITE, VM_HISTORY_TABLE, "storage", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_STATUS, VM_HISTORY_TABLE, "meta", ctx.multiplex_time));
    }
}
