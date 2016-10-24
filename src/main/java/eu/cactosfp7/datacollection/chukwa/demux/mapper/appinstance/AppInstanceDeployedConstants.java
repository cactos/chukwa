/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.appinstance;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.deletevm.*;
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
public class AppInstanceDeployedConstants {

    public static final String VM_ID = "VMID";
    public static final String COMPONENT = "Component";
    public static final String APP_NAME = "AppName";
    public static final String APPINSTANCE = "AppInstance";

    static final String[] APP_INSTANCE_DEPLOYED_HEADER_SPLIT_COLUMNS = {
        VM_ID, COMPONENT, APP_NAME, APPINSTANCE
    };

    static final String[]  APP_INSTANCE_DEPLOYED_COLUMN_NAMES = {
        VM_ID, COMPONENT, APP_NAME, APPINSTANCE
    };

    static final AppInstanceDeployedMetricContext createMapperContext() {
        return new AppInstanceDeployedMetricContext();
    }

    final static class AppInstanceDeployedMetricContext {

        final RowKeyGenerator time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, VM_ID);
        final RowKeyGenerator plain = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, VM_ID);
    }

    static final Map<String, SourceReadSpecification> appInstanceDeployedReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{
            new SourceReadSpecification(VM_ID, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(COMPONENT, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(APP_NAME, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(APPINSTANCE, String.class, ComputeType.ONLY_PARTS)
        }, appInstanceDeployedReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getAppInstanceDeployedMappers(AppInstanceDeployedMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(COMPONENT, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APP_NAME, VM_TABLE, "meta", ctx.plain)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPINSTANCE, VM_TABLE, "meta", ctx.plain)
                );
    }

    static MapperCollection<HBaseMappingSpecification> getAppInstanceDeployedHistoryMappers(AppInstanceDeployedMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VM_ID, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(COMPONENT, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APP_NAME, VM_HISTORY_TABLE, "meta", ctx.time)).
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(APPINSTANCE, VM_HISTORY_TABLE, "meta", ctx.time)
                );
    }
}
