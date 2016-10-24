/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
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
public class VMPlacementConstants {

    public static final String VM_IMAGE_UUID = "vm_image_uuid";
    public static final String VM_NAME = "vm_name";
    public static final String VM_STATE = "vm_state";
    public static final String VM_UUID = "vm_uuid";
    public static final String NODE_NAME = "node_name";
    public static final String VM_NUMBER = "VM_number";
    public static final String VM_SNAPSHOT_STATE = "vm_state";
    public static final String VM_SNAPSHOT_RAM = "ram-total";
    public static final String VM_SNAPSHOT_CORES ="CpuCS";
    public static final String VM_HOST_NAME = "host_name";

    static final String[] HARDWARE_HEADER_SPLIT_COLUMNS = {
        VM_IMAGE_UUID, VM_NAME, VM_STATE, VM_UUID, NODE_NAME, VM_NUMBER, VM_SNAPSHOT_STATE, VM_SNAPSHOT_RAM, VM_SNAPSHOT_CORES, VM_HOST_NAME
    };

    static final String[] HARDWARE_COLUMN_NAMES = {
        VM_IMAGE_UUID, VM_NAME, VM_STATE, VM_UUID, NODE_NAME, VM_NUMBER, VM_SNAPSHOT_STATE, VM_SNAPSHOT_RAM, VM_SNAPSHOT_CORES, VM_HOST_NAME
    };

    static HardwareMetricContext createMapperContext(String vMkey) {
        return new HardwareMetricContext(vMkey);
    }

    static class HardwareMetricContext {
    	 final RowKeyGenerator multiplex_time;
         final RowKeyGenerator multiplex; 
         
         HardwareMetricContext(String prefix) {
        	 multiplex_time = new TimestampedRowGenerator(generateSpecificReadKey(prefix, NODE_NAME));
        	 multiplex = new PlainValueRowGenerator(generateSpecificReadKey(prefix, NODE_NAME));
        	// multiplex_time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, generateSpecificReadKey(prefix, NODE_NAME));
        	// multiplex = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, generateSpecificReadKey(prefix, NODE_NAME));
         }
    }

    static Map<String, SourceReadSpecification> generatePrefixedHardwareReadSpec(String prefix) {
    	Map<String, SourceReadSpecification> hardwareReadSpec = new HashMap<String, SourceReadSpecification>();
    	AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_IMAGE_UUID), String.class, ComputeType.ONLY_PARTS),
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_NAME), String.class, ComputeType.ONLY_PARTS),
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_STATE), String.class, ComputeType.ONLY_PARTS),
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_UUID), String.class, ComputeType.ONLY_PARTS),
                new SourceReadSpecification(generateSpecificReadKey(prefix, NODE_NAME), String.class, ComputeType.IS_SINGLETON),
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_NUMBER), String.class, ComputeType.IS_SINGLETON),}, hardwareReadSpec);
    	return hardwareReadSpec;
    }
    
    static Map<String, SourceReadSpecification> generatePrefixedVMReadSpec(String prefix) {
    	Map<String, SourceReadSpecification> VMReadSpec = new HashMap<String, SourceReadSpecification>();
    	AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_SNAPSHOT_STATE), String.class, ComputeType.IS_SINGLETON),
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_SNAPSHOT_RAM), String.class, ComputeType.IS_SINGLETON),
                new SourceReadSpecification(generateSpecificReadKey(prefix, NODE_NAME), String.class, ComputeType.IS_SINGLETON),
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_SNAPSHOT_CORES), String.class, ComputeType.IS_SINGLETON),
                new SourceReadSpecification(generateSpecificReadKey(prefix, VM_HOST_NAME), String.class, ComputeType.IS_SINGLETON),}, VMReadSpec);
    	return VMReadSpec;
    }

    static String generateSpecificReadKey(String prefix, String keyname) {
    	return prefix + "-" + keyname;
    	//return keyname;
    }
    
//    void refresh()
//    {
//    	Log.info("initialised");
//        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
//            new SourceReadSpecification(VM_IMAGE_UUID, String.class, ComputeType.ONLY_PARTS),
//            new SourceReadSpecification(VM_NAME, String.class, ComputeType.ONLY_PARTS),
//            new SourceReadSpecification(VM_STATE, String.class, ComputeType.ONLY_PARTS),
//            new SourceReadSpecification(VM_UUID, String.class, ComputeType.ONLY_PARTS),
//            new SourceReadSpecification(NODE_NAME, String.class, ComputeType.ONLY_PARTS),
//            new SourceReadSpecification(VM_NUMBER, String.class, ComputeType.ONLY_PARTS),}, hardwareReadSpec);
//    }
    
    static MapperCollection<HBaseMappingSpecification> getHardwareMappers(String prefix, HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_IMAGE_UUID), CN_TABLE, "vms", VM_IMAGE_UUID, ctx.multiplex)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_NAME),CN_TABLE, "vms", VM_NAME, ctx.multiplex)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_STATE), CN_TABLE, "vms", VM_STATE, ctx.multiplex)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_UUID), CN_TABLE, "vms", VM_UUID, ctx.multiplex)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_NUMBER), CN_TABLE, "vms", VM_NUMBER, ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHardwareMappers(String prefix, HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_IMAGE_UUID), CN_HISTORY_TABLE, "vms", VM_IMAGE_UUID, ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_NAME), CN_HISTORY_TABLE, "vms", VM_NAME, ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_STATE), CN_HISTORY_TABLE, "vms", VM_STATE, ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_UUID), CN_HISTORY_TABLE, "vms", VM_UUID, ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_NUMBER), CN_HISTORY_TABLE, "vms", VM_NUMBER, ctx.multiplex_time));
    }
    
    static MapperCollection<HBaseMappingSpecification> getVMSnapshotMappers(String prefix, HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_SNAPSHOT_STATE), VM_TABLE, "meta", VM_SNAPSHOT_STATE, ctx.multiplex)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_SNAPSHOT_RAM), VM_TABLE, "hardware", VM_SNAPSHOT_RAM, ctx.multiplex)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_SNAPSHOT_CORES), VM_TABLE, "hardware", VM_SNAPSHOT_CORES, ctx.multiplex)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_HOST_NAME), VM_TABLE, "meta", VM_HOST_NAME, ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getVMHistoryMappers(String prefix, HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_SNAPSHOT_STATE), VM_HISTORY_TABLE, "meta", VM_SNAPSHOT_STATE, ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_SNAPSHOT_RAM), VM_HISTORY_TABLE, "hardware", VM_SNAPSHOT_RAM, ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_SNAPSHOT_CORES), VM_HISTORY_TABLE, "hardware", VM_SNAPSHOT_CORES, ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, VM_HOST_NAME), VM_HISTORY_TABLE, "meta", VM_HOST_NAME, ctx.multiplex_time));
    }
}
