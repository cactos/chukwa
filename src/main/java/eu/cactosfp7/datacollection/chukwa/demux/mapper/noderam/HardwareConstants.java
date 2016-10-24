/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.noderam;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
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
public class HardwareConstants {

    public static final String MEM_FREE = "mem_free";
    public static final String MEM_SIZE = "mem_size";
    public static final String MEM_BUFF = "mem_buff";
    public static final String MEM_CACHE = "mem_cache";
    public static final String MEM_SWPD = "mem_swpd";
    public static final String NODE_NAME = "node_name";

    static final String[] HARDWARE_HEADER_SPLIT_COLUMNS = {
    	MEM_FREE, MEM_SIZE, MEM_BUFF, MEM_CACHE, MEM_SWPD, NODE_NAME 
    };

    static final String[] HARDWARE_COLUMN_NAMES = {
    	MEM_FREE, MEM_SIZE, MEM_BUFF, MEM_CACHE, MEM_SWPD, NODE_NAME
    };

    static final HardwareMetricContext createMapperContext() {
        return new HardwareMetricContext();
    }

    final static class HardwareMetricContext {

    	 final RowKeyGenerator multiplex_time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, NODE_NAME);
         final RowKeyGenerator multiplex = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, NODE_NAME);
    }

    static final Map<String, SourceReadSpecification> hardwareReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
        		new SourceReadSpecification(MEM_SIZE, String.class, ComputeType.ONLY_PARTS),
        		new SourceReadSpecification(MEM_FREE, String.class, ComputeType.ONLY_PARTS),
        		new SourceReadSpecification(MEM_BUFF, String.class, ComputeType.ONLY_PARTS),
        		new SourceReadSpecification(MEM_CACHE, String.class, ComputeType.ONLY_PARTS),
        		new SourceReadSpecification(MEM_SWPD, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(NODE_NAME, String.class, ComputeType.ONLY_PARTS)}, hardwareReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_FREE, CN_TABLE, "hardware_util", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_BUFF, CN_TABLE, "hardware_util", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_CACHE, CN_TABLE, "hardware_util", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_SWPD, CN_TABLE, "hardware_util", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_SIZE, CN_TABLE, "hardware", ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_FREE, CN_HISTORY_TABLE, "hardware_util", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_BUFF, CN_HISTORY_TABLE, "hardware_util", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_SIZE, CN_HISTORY_TABLE, "hardware", ctx.multiplex_time));
    }
}
