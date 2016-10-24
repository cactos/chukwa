package eu.cactosfp7.datacollection.chukwa.demux.mapper.df;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.INDEX_TABLE; 
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SYSTEM_METRICS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SOURCE;

import java.util.HashMap;
import java.util.Map;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseMappingSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.PlainValueRowGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.TimestampedRowGenerator;

public class DfConstants {
	
	public static final String FILESYSTEM = "Filesystem";
	public static final String ONE_K_BLOCKS = "1K-blocks";
	public static final String FS_USED = "Used";
	public static final String FS_AVAIL = "Available";
	public static final String FS_USE_PERC = "Use%";
	public static final String FS_MOUNT_ON = "Mounted on";
	
	static final String[] DF_HEADER_SPLIT_COLUMNS = { 
		FILESYSTEM, ONE_K_BLOCKS, FS_USED, FS_AVAIL, FS_USE_PERC, "Mounted", "on"
	};
	
	static final String[]  DF_COLUMN_NAMES = { 
		FILESYSTEM, ONE_K_BLOCKS, FS_USED, FS_AVAIL, FS_USE_PERC, FS_MOUNT_ON
	};
	
	static final DfMetricContext createMapperContext() {
		return new DfMetricContext();
	}
	
	final static class DfMetricContext {
		final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
		final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
	}
	
	static final Map<String, SourceReadSpecification> dfReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification(FILESYSTEM, double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(ONE_K_BLOCKS, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(FS_USED, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(FS_AVAIL, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(FS_USE_PERC, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(FS_MOUNT_ON, String.class, ComputeType.ONLY_PARTS),
				}, dfReadSpec); 
	}
	
	static MapperCollection<HBaseMappingSpecification> getDfMappers(DfMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(FILESYSTEM, INDEX_TABLE, "df", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(ONE_K_BLOCKS, INDEX_TABLE, "df", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(FS_USED, SYSTEM_METRICS, "df", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(FS_AVAIL, SYSTEM_METRICS, "df", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(FS_USE_PERC, SYSTEM_METRICS, "df", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(FS_MOUNT_ON, SYSTEM_METRICS, "df", ctx.time));
	}
}
