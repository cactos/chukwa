/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.storage;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.Hardware;
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
public class HardwareConstants {

    public static final String AVAILABLE = "available";
    public static final String NODE_NAME = "node_name";


    static final String[] HARDWARE_HEADER_SPLIT_COLUMNS = {
    		AVAILABLE, NODE_NAME
    };

    static final String[] HARDWARE_COLUMN_NAMES = {
    		AVAILABLE, NODE_NAME
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
         }
    }

    static Map<String, SourceReadSpecification> generatePrefixedHardwareReadSpec(String prefix) {
    	Map<String, SourceReadSpecification> hardwareReadSpec = new HashMap<String, SourceReadSpecification>();
    	AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
                new SourceReadSpecification(generateSpecificReadKey(prefix, AVAILABLE), String.class, ComputeType.ONLY_PARTS),
                new SourceReadSpecification(generateSpecificReadKey(prefix, NODE_NAME), String.class, ComputeType.IS_SINGLETON),}, hardwareReadSpec);
    	return hardwareReadSpec;
    }

    static String generateSpecificReadKey(String prefix, String keyname) {
    	return prefix + "-" + keyname;
    	//return keyname;
    }
    
    static MapperCollection<HBaseMappingSpecification> getHardwareMappers(String prefix, HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, AVAILABLE), CN_TABLE, "storage", AVAILABLE, ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHardwareMappers(String prefix, HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.arbitraryQuantifierMapping(generateSpecificReadKey(prefix, AVAILABLE), CN_HISTORY_TABLE, "storage", AVAILABLE, ctx.multiplex_time));
    }
}
