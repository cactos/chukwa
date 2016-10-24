/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.filestrace;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_APP_HISTORY_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseMappingSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.SplittingRowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.TimestampedRowGenerator;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author atsi
 */
public class FileStraceConstants {

    public static final String MOLPROSTATE = "MolproState";
//    public static final String FILENAME = "Filename";
//    public static final String READ = "Read [Byte]";
//    public static final String WRITE = "Written [Byte]";
    public static final String VM_ID = "vm_id";

    static final String[] FILESTRACE_HEADER_SPLIT_COLUMNS = {
        MOLPROSTATE,
        //        FILENAME,
        //             READ, WRITE
//        VM_ID
    };

    static final String[] FILESTRACE_COLUMN_NAMES = {
        MOLPROSTATE,
        //        FILENAME,
        //        READ, WRITE
//        VM_ID
    };

    static final FileStraceMetricContext createMapperContext() {
        return new FileStraceMetricContext();
    }

    final static class FileStraceMetricContext {

        final RowKeyGenerator time = new SplittingRowKeyGenerator<TimestampedRowGenerator>(TimestampedRowGenerator.class, VM_ID);
    }

    static final Map<String, SourceReadSpecification> filestraceReadSpec = new HashMap<String, SourceReadSpecification>();

    static {
        AbstractProcessorConstants.addToMap(new SourceReadSpecification[]{ 
            new SourceReadSpecification(MOLPROSTATE, String.class, ComputeType.ONLY_PARTS),
            new SourceReadSpecification(VM_ID, String.class, ComputeType.ONLY_PARTS),
//            new SourceReadSpecification(FILENAME, String.class, ComputeType.ONLY_PARTS),
        //            new SourceReadSpecification(READ, String.class, ComputeType.ONLY_PARTS),
        //            new SourceReadSpecification(WRITE, String.class, ComputeType.ONLY_PARTS)
        }, filestraceReadSpec);
    }

    static MapperCollection<HBaseMappingSpecification> getFileStraceMappers(FileStraceMetricContext ctx) {
        return new MapperCollection<HBaseMappingSpecification>().
                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MOLPROSTATE, VM_APP_HISTORY_TABLE, "file", ctx.time)
                //                ).
                //                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(READ, VM_APP_HISTORY_TABLE, "file", ctx.time)).
                //                addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(WRITE, VM_APP_HISTORY_TABLE, "file", ctx.time)
                );
    }
}
