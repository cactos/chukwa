/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.nodenet;

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

    public static final String NET_THROUGH = "net_through";
    public static final String NET_SPEED = "netw_speed";
    public static final String NODE_NAME = "node_name";

    static final String[] HARDWARE_HEADER_SPLIT_COLUMNS = {
    	NET_THROUGH, NET_SPEED, NODE_NAME 
    };

    static final String[] HARDWARE_COLUMN_NAMES = {
    	NET_THROUGH, NET_SPEED, NODE_NAME
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
            new SourceReadSpecification(NET_THROUGH, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(NET_SPEED, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(NODE_NAME, String.class, ComputeType.ONLY_PARTS)}, hardwareReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_THROUGH, CN_TABLE, "network_util", ctx.multiplex)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_SPEED, CN_TABLE, "network", ctx.multiplex));
    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHardwareMappers(HardwareMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_THROUGH, CN_HISTORY_TABLE, "network_util", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_SPEED, CN_HISTORY_TABLE, "network", ctx.multiplex_time));
    }
}
