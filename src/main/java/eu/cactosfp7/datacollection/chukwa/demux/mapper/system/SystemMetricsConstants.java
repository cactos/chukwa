package eu.cactosfp7.datacollection.chukwa.demux.mapper.system;

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
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.*;

final class SystemMetricsConstants {
	
	static final String[] ALL_FAMILIES = {
		"disk", "cpu", "memory", "system", "location"
	};
	
	static final SystemMetricContext createMapperContext() {
		return new SystemMetricContext();
	}
	
	final static class SystemMetricContext {
		final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
		final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
	}
	
	/**
 	 * DISK
	 * disk:Type.Z			=> static?
	 * disk:TypeName.Z		=> 
	 * disk:SysTypeName.Z	=> static ?
	 * disk:Reads.Z			=> load; one per mounted file system
	 * disk:ReadBytes.Z		=> load; one per mounted file system
	 * disk:Writes.Z		=> load;
	 * disk:WriteBytes.Z	=> load;
	 * disk:Options.Z		=> static?
	 * disk:DirName.Z		=> static?
	 * disk:DevName.Z		=> static?
	 * disk:Flags.Z			=> static?
	 */
	static final Map<String, SourceReadSpecification> diskReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification("disks", double.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("Type", String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("TypeName", String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("SysTypeName", String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("Options", String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("DirName", String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("DevName", String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("Flags", String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("ReadBytes", long.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("Reads", long.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("WriteBytes", long.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("Writes", long.class, ComputeType.ONLY_PARTS),
				}, diskReadSpec);
	}
	
	static MapperCollection<HBaseMappingSpecification> getDiskMappers(SystemMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("disks", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Type", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TypeName", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("SysTypeName", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Options", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("DirName", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("DevName", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Flags", INDEX_TABLE, "disk", ctx.plain)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("ReadBytes", SYSTEM_METRICS, "disk", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Reads", SYSTEM_METRICS, "disk", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("WriteBytes", SYSTEM_METRICS, "disk", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Writes", SYSTEM_METRICS, "disk", ctx.time));
	}
	
	/** CPU
	 * cpu:CacheSize.X		=> static; one per core;
	 * cpu:CoresPerSocket.X	=> static; one per core;
	 * cpu:TotalSockets.	=> static; one per core;
	 * cpu:TotalCores.X		=> static; one per core;
	 * cpu:Model.X			=> static; one per core;
	 * cpu:Mhz.X			=> static?; one per core;
	 * cpu:wait.X			=> load; one per core;
	 * cpu:sys.X			=> load; one per core;
	 * cpu:user.X			=> load; one per core;
	 * cpu:irq.X			=> load; one per core;
	 * cpu:idle.X			=> load; one per core;
	 * cpu:combined.X		=> load; one per core;
	 * cpu:vendor.X			=> static; one per core;
	 * cpu:csource
	 * cpu:ctags
	 * cpu:app
	 */ 
	static final Map<String, SourceReadSpecification> cpuReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification("cpus", double.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("user", double.class, ComputeType.AVG),
						new SourceReadSpecification("sys", double.class, ComputeType.AVG),
						new SourceReadSpecification("idle", double.class, ComputeType.AVG),
						new SourceReadSpecification("combined", double.class, ComputeType.AVG),
                                                new SourceReadSpecification("nice", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("wait", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("CacheSize", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("CoresPerSocket", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("TotalSockets", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("TotalCores", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("Model", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("Mhz", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("irq", double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification("Vendor", double.class, ComputeType.ONLY_PARTS)
				}, cpuReadSpec);
	}
	
	static MapperCollection<HBaseMappingSpecification> getCpuMappers(SystemMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("user", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("sys", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("idle", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("combined", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("nice", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("wait", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("irq", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("cpus", INDEX_TABLE, "cpu", ctx.plain)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("CacheSize", INDEX_TABLE, "cpu", ctx.plain)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("CoresPerSocket", INDEX_TABLE, "cpu", ctx.plain)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TotalSockets", INDEX_TABLE, "cpu", ctx.plain)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TotalCores", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Model", INDEX_TABLE, "cpu", ctx.plain)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Mhz", SYSTEM_METRICS, "cpu", ctx.time)).
				addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Vendor", INDEX_TABLE, "cpu", ctx.plain));
	}
	
	/** NETWORK
	 * network:TxPackets.Y		=> load; one per ???
	 * network:RxPackets.Y		=> load; one per ???
	 * 
	 * network:TxOverruns.Y		=> load; one per ???
	 * network:RxOverruns.Y		=> load; one per ???
	 * 
	 * network:TxErrors.Y 		=> load; one per ???
	 * network:RxErrors.Y		=> load; one per ???
	 * 
	 * network:TxDropped.Y		=> load; one per ???
	 * network:RxDropped.Y		=> load; one per ???
	 * 
	 * network:TxBytes.Y		=> load; one per ???
	 * network:RxBytes.Y		=> load; one per ???
	 * 
	 * network:TxCollisions.Y	=> load; one per ???
	 * network:TxCarrier.Y 		=> load; one per ???
	 * network:RxFrame.Y		=> load; one per ???
	 */
	static final Map<String, SourceReadSpecification> networkReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification("nics", double.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("RxBytes", double.class, ComputeType.SUM),
						new SourceReadSpecification("TxBytes", double.class, ComputeType.SUM),
						
						new SourceReadSpecification("TxErrors", double.class, ComputeType.SUM),
						new SourceReadSpecification("RxErrors", double.class, ComputeType.SUM),
					
						new SourceReadSpecification("RxPackets", double.class, ComputeType.SUM),
						new SourceReadSpecification("TxPackets", double.class, ComputeType.SUM),
						
						new SourceReadSpecification("RxOverruns", double.class, ComputeType.SUM),
						new SourceReadSpecification("TxOverruns", double.class, ComputeType.SUM),
						
						new SourceReadSpecification("TxDropped", double.class, ComputeType.SUM),
						new SourceReadSpecification("RxDropped", double.class, ComputeType.SUM),
						
						new SourceReadSpecification("RxFrame", double.class, ComputeType.SUM),
						new SourceReadSpecification("TxCarrier", double.class, ComputeType.SUM),
						new SourceReadSpecification("TxCollisions", double.class, ComputeType.SUM),
		}, networkReadSpec);
	}

	static MapperCollection<HBaseMappingSpecification> getNetworkMappers(SystemMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("nics", INDEX_TABLE, "network", ctx.plain)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("RxBytes", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TxBytes", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("RxErrors", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TxErrors", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("RxPackets", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TxPackets", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("RxDropped", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TxDropped", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TxCollisions", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("RxFrame", SYSTEM_METRICS, "network", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("TxCarrier", SYSTEM_METRICS, "network", ctx.time));
					//FIXME: add others as well
	}
	
	static final Map<String, SourceReadSpecification> hwStructureReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification("cluster", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("rack", String.class, ComputeType.IS_SINGLETON),
						
				}, hwStructureReadSpec );
	}
	static MapperCollection<HBaseMappingSpecification> getHwStructureMappers(SystemMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("cluster", INDEX_TABLE, "location", ctx.plain)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("rack", INDEX_TABLE, "location", ctx.plain));
	}
	

	/**
	 * SYSTEM
	 * system:LoadAverage.{.5,1,5}	=> load
	 * system:Uptime				=> load
	 */
	static final Map<String, SourceReadSpecification> systemReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification("Uptime", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("LoadAverage.1", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("LoadAverage.5", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("LoadAverage.15", String.class, ComputeType.IS_SINGLETON),
				}, systemReadSpec );
	}
	
	static MapperCollection<HBaseMappingSpecification> getSystemMappers(SystemMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Uptime", SYSTEM_METRICS, "system", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("LoadAverage.1", SYSTEM_METRICS, "system", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("LoadAverage.5", SYSTEM_METRICS, "system", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("LoadAverage.15", SYSTEM_METRICS, "system", ctx.time));
	}
 
	/**
	 * MEMORY
	 * memory:ActualFree	=> load
	 * memory:ActualUsed	=> load
	 * memory:Free			=> load
	 * memory:FreePercent	=> load
	 * memory:Ram			=> static
	 * memory:Total			=> static
	 * memory:Used			=> load
	 * memory:UsedPercent	=> load 
	 */
	static final Map<String, SourceReadSpecification> memoryReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification("ActualFree", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("ActualUsed", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("Free", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("FreePercent", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("Ram", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("Total", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("Used", String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification("UsedPercent", String.class, ComputeType.IS_SINGLETON),
				}, memoryReadSpec );
	}
	
	static MapperCollection<HBaseMappingSpecification> getMemoryMappers(SystemMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("ActualFree", SYSTEM_METRICS, "memory", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("ActualUsed", SYSTEM_METRICS, "memory", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Free", SYSTEM_METRICS, "memory", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("FreePercent", SYSTEM_METRICS, "memory", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Used", SYSTEM_METRICS, "memory", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("UsedPercent", SYSTEM_METRICS, "memory", ctx.time)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Ram", INDEX_TABLE, "memory", ctx.plain)).
						addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping("Total", SYSTEM_METRICS, "memory", ctx.time));
	}
	 
	private SystemMetricsConstants() {
		// no instances of this class
	}
}
