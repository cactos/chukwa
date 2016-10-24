/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.writestate;

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
public class WriteStateConstants {

    public static final String NODE_NAME = "NodeName";
    public static final String STATE = "State";

    static final String[] WRITE_STATE_HEADER_SPLIT_COLUMNS = {
        NODE_NAME, STATE
    };

    static final String[] WRITE_STATE_COLUMN_NAMES = {
        NODE_NAME, STATE
    };

    static final WriteStateMetricContext createMapperContext() {
        return new WriteStateMetricContext();
    }

    final static class WriteStateMetricContext {

        final RowKeyGenerator time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, NODE_NAME);
        final RowKeyGenerator plain = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, NODE_NAME);
    }

    static final Map<String, SourceReadSpecification> writeStateReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(NODE_NAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(STATE, String.class, ComputeType.ONLY_PARTS)
        }, writeStateReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getWriteStateMappers(WriteStateMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NODE_NAME, CN_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(STATE, CN_TABLE, "meta", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getWriteStateHistoryMappers(WriteStateMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NODE_NAME, CN_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(STATE, CN_HISTORY_TABLE, "meta", ctx.time)
                );
    }
}
