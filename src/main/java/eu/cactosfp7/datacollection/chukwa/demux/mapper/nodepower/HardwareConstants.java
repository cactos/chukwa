/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.nodepower;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseMappingSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.PlainValueRowGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.SplittingRowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.TimestampedRowGenerator;
import java.util.HashMap;
import java.util.Map;


public class HardwareConstants {

    public static final String NODE_NAME = "node_name";
    public static final String CONSUMPTION = "consumption";
    public static final String SERIAL = "serial";
    public static final String CAPACITY = "capacity";
    
    static final String[] HARDWARE_HEADER_SPLIT_COLUMNS = {
       NODE_NAME, CONSUMPTION, SERIAL, CAPACITY
    };

    static final String[] HARDWARE_COLUMN_NAMES = {
       NODE_NAME, CONSUMPTION, SERIAL, CAPACITY
    };

    static final HardwareMetricContext createMapperContext() {
        return new HardwareMetricContext();
    }

//    final static class HardwareMetricContext {
//
//    	 final RowKeyGenerator multiplex_time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, NODE_NAME);
//         final RowKeyGenerator multiplex = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, NODE_NAME);
//    }
    
    final static class HardwareMetricContext {

        final RowKeyGenerator multiplex_time = new TimestampedRowGenerator(NODE_NAME);
        final RowKeyGenerator multiplex = new PlainValueRowGenerator(NODE_NAME);
    }

    static final Map<String, SourceReadSpecification> hardwareReadSpec = new HashMap<String, SourceReadSpecification>();
    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
    		new SourceReadSpecification(SERIAL, String.class, SourceReadSpecification.ComputeType.ONLY_PARTS),
            new SourceReadSpecification(CAPACITY, String.class, SourceReadSpecification.ComputeType.ONLY_PARTS)
            }, hardwareReadSpec);
    }
    
    static final Map<String, SourceReadSpecification> nodeReadSpec = new HashMap<String, SourceReadSpecification>();
    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
    		new SourceReadSpecification(NODE_NAME, String.class, SourceReadSpecification.ComputeType.IS_SINGLETON),
            new SourceReadSpecification(CONSUMPTION, String.class, SourceReadSpecification.ComputeType.IS_SINGLETON)
            }, nodeReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CAPACITY, CN_TABLE, "power", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(SERIAL, CN_TABLE, "power", ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CAPACITY, CN_HISTORY_TABLE, "power", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(SERIAL, CN_HISTORY_TABLE, "power", ctx.multiplex_time));
    }
    
    static MapperCollection<HBaseMappingSpecification> getNodeMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CONSUMPTION, CN_TABLE, "power_util", ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryNodeMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CONSUMPTION, CN_HISTORY_TABLE, "power_util", ctx.multiplex_time));
          
    }
}


