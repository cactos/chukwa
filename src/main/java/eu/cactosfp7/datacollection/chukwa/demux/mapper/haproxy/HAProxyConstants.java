package eu.cactosfp7.datacollection.chukwa.demux.mapper.haproxy;

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
public class HAProxyConstants {
	
    public static final String APP_COMP_NAME = "app_comp_name";
    public static final String UUID = "UUID";
    public static final String RATE_2XX = "rate_2xx";
    public static final String TOT_REQ_RATE = "tot_req_rate";
    public static final String VM_STATUS = "vm_status";
    public static final String VMAPPHISTORY = "VMAppHistory";

    static final String[] HAPROXY_HEADER_SPLIT_COLUMNS = {
    	APP_COMP_NAME,
        UUID,
        RATE_2XX,
        TOT_REQ_RATE,
        VM_STATUS
    };

    static final String[] HAPROXY_COLUMN_NAMES = {
        	APP_COMP_NAME,
            UUID,
            RATE_2XX,
            TOT_REQ_RATE,
            VM_STATUS
    };

    static final HAProxyMetricContext createMapperContext() {
        return new HAProxyMetricContext();
    }

    final static class HAProxyMetricContext {

    	final RowKeyGenerator multiplex_time = new TimestampedRowGenerator(UUID);
    	final RowKeyGenerator multiplex = new PlainValueRowGenerator(UUID);
//        final RowKeyGenerator multiplex_time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, UUID);
//        final RowKeyGenerator multiplex = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, UUID);
    }


    static final Map<String, SourceReadSpecification> haproxyReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(APP_COMP_NAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(UUID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(RATE_2XX, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(TOT_REQ_RATE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VM_STATUS, String.class, ComputeType.ONLY_PARTS)
        }, haproxyReadSpec);
    }

//    static MapperCollection<HBaseMappingSpecification> getHAProxyMappers(HAProxyMetricContext ctx) {
//        return new MapperCollection<HBaseMappingSpecification>().
//                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APP_COMP_NAME, VM_TABLE, "app", ctx.multiplex)).
//                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(UUID, VM_TABLE, "app", ctx.multiplex)).
//                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(RATE_2XX, VM_TABLE, "app", ctx.multiplex)).
//                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TOT_REQ_RATE, VM_TABLE, "app", ctx.multiplex)).
//                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_STATUS, VM_TABLE, "app", ctx.multiplex)
//                );
//    }

    static MapperCollection<HBaseMappingSpecification> getHistoryHAProxyMappers(HAProxyMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APP_COMP_NAME, VMAPPHISTORY, "app", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(UUID, VMAPPHISTORY, "app", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(RATE_2XX, VMAPPHISTORY, "app", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TOT_REQ_RATE, VMAPPHISTORY, "app", ctx.multiplex_time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_STATUS, VMAPPHISTORY, "app", ctx.multiplex_time));
    }
}
