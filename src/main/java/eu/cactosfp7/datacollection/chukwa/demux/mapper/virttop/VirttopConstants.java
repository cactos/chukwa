package eu.cactosfp7.datacollection.chukwa.demux.mapper.virttop;

import java.util.HashMap; 
import java.util.Map;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification.ComputeType;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseMappingSpecification;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.PlainValueRowGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.RowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.SplittingRowKeyGenerator;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.TimestampedRowGenerator;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.MapperCollection;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.SourceReadSpecification;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.*;

final class VirttopConstants {
	/**
 	 * VIRT-TOP
	 *  0 Hostname	
	 *  1 Time
	 *  2 Arch
	 *  3 Physical CPUs
	 *  4 Count
     *  5 Running
	 *  6 Blocked
	 *  7 Paused 
	 *  8 Shutdown
	 *  9 Shutoff
	 * 10 Crashed	
	 * 11 Active
	 * 12 Inactive 
	 * 13 %CPU
	 * 14 Total hardware memory (KB)
	 * 15 Total memory (KB)
	 * 16 Total guest memory (KB)
	 * 17 Total CPU time (ns)
	 * 18 Domain ID
	 * 19 Domain name
	 * 20 CPU (ns)
	 * 21 %CPU
	 * 22 Mem (bytes)
	 * 23 %Mem
	 * 24 Block RDRQ
	 * 25 Block WRRQ
	 * 26 Net RXBY
	 * 27 Net TXBY
	 */
	public static final String HOSTNAME = "Hostname";
	public static final String TIME = "Time";
	public static final String ARCH = "Arch";
	public static final String PHYS_CPUS = "Physical CPUs";
	public static final String VMS_COUNT = "Count";
	public static final String VMS_RUNNING = "Running";
	public static final String VMS_BLOCKED = "Blocked";
	public static final String VMS_PAUSED = "Paused";
	public static final String VMS_SHUTDOWN = "Shutdown";
	public static final String VMS_SHUTOFF = "Shutoff";
	public static final String VMS_CRASHED = "Crashed";
	public static final String VMS_ACTIVE = "Active";
	public static final String VMS_INACTIVE = "Inactive";
	public static final String CPU_UTIL_PERC = "%CPU";
	public static final String HARDWARE_MEMORY = "Total hardware memory (KB)";
	public static final String TOTAL_MEMORY = "Total memory (KB)";
	public static final String TOTAL_GUEST_MEMORY = "Total guest memory (KB)";
	public static final String TOTAL_CPU_TIME = "Total CPU time (ns)";
	public static final String DOMAIN_ID = "Domain ID";
	public static final String DOMAIN_NAME = "Domain name";
	public static final String VMS_CPU_NS = "CPU (ns)"; 
        public static final String VMS_MAP_CPU_PERC = "VM%CPU"; 
	public static final String VMS_CPU_PERC = "%CPU";
	public static final String MEM_BYTES = "Mem (bytes)";
	public static final String MEM_PERC = "%Mem";
	public static final String BLOCK_RDRQ = "Block RDRQ";
	public static final String BLOCK_WRRQ = "Block WRRQ";
	public static final String NET_RXBY = "Net RXBY";
	public static final String NET_TXBY = "Net TXBY";
	
	static final String[] HOST_ELEMENTS = {
		HOSTNAME,TIME, ARCH, PHYS_CPUS, VMS_COUNT, VMS_RUNNING, VMS_BLOCKED, VMS_PAUSED, VMS_SHUTDOWN, 
		VMS_SHUTOFF, VMS_CRASHED, VMS_ACTIVE, VMS_INACTIVE, CPU_UTIL_PERC, HARDWARE_MEMORY, 
		TOTAL_MEMORY, TOTAL_GUEST_MEMORY, TOTAL_CPU_TIME,	
	};
	
	static final String[] VM_ELEMENTS = {
		DOMAIN_ID, DOMAIN_NAME, VMS_CPU_NS, VMS_CPU_PERC, MEM_BYTES, 
		MEM_PERC, BLOCK_RDRQ, BLOCK_WRRQ, NET_RXBY, NET_TXBY
	};
        
        static final String[] VM_MAP_ELEMENTS = {
		DOMAIN_ID, DOMAIN_NAME, VMS_CPU_NS, VMS_MAP_CPU_PERC, MEM_BYTES, 
		MEM_PERC, BLOCK_RDRQ, BLOCK_WRRQ, NET_RXBY, NET_TXBY
	};
	
	static final VirttopMetricContext createMapperContext() {
		return new VirttopMetricContext();
	}
	
	final static class VirttopMetricContext {
		final RowKeyGenerator time = new TimestampedRowGenerator(SOURCE);
		final RowKeyGenerator plain = new PlainValueRowGenerator(SOURCE);
		final RowKeyGenerator multiplex = new SplittingRowKeyGenerator<PlainValueRowGenerator>(PlainValueRowGenerator.class, DOMAIN_NAME);
	}
	
	static final Map<String, SourceReadSpecification> virtTopVmReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification(DOMAIN_ID, double.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(DOMAIN_NAME, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(VMS_CPU_NS, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(VMS_MAP_CPU_PERC, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(MEM_BYTES, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(MEM_PERC, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(BLOCK_RDRQ, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(BLOCK_WRRQ, String.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(NET_RXBY, long.class, ComputeType.ONLY_PARTS),
						new SourceReadSpecification(NET_TXBY, long.class, ComputeType.ONLY_PARTS),
				}, virtTopVmReadSpec); 
	}
	
	static MapperCollection<HBaseMappingSpecification> getVirtTopVmMappers(VirttopMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
//					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DOMAIN_ID, INDEX_TABLE, "virttop", ctx.multiplex)).
//					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DOMAIN_NAME, INDEX_TABLE, "virttop", ctx.multiplex)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DOMAIN_ID, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(DOMAIN_NAME, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_CPU_NS, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_MAP_CPU_PERC, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_BYTES, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(MEM_PERC, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(BLOCK_RDRQ, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(BLOCK_WRRQ, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_RXBY, SYSTEM_METRICS, "virttop", ctx.time)).
					addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(NET_TXBY, SYSTEM_METRICS, "virttop", ctx.time));
	}
	
	static final Map<String, SourceReadSpecification> virtTopHostReadSpec = new HashMap<String, SourceReadSpecification>();
	static { 
		AbstractProcessorConstants.addToMap(
				new SourceReadSpecification[] {
						new SourceReadSpecification(HOSTNAME, double.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(TIME, double.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(ARCH, String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(PHYS_CPUS, String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_COUNT, String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_RUNNING, String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_BLOCKED, String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_PAUSED, String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_SHUTDOWN, String.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_SHUTOFF, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_CRASHED, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_ACTIVE, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(VMS_INACTIVE, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(CPU_UTIL_PERC, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(HARDWARE_MEMORY, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(TOTAL_MEMORY, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(TOTAL_GUEST_MEMORY, long.class, ComputeType.IS_SINGLETON),
						new SourceReadSpecification(TOTAL_CPU_TIME, long.class, ComputeType.IS_SINGLETON),
				}, virtTopHostReadSpec); 
	}
	
	static MapperCollection<HBaseMappingSpecification> getVirtTopHostMapping(VirttopMetricContext ctx) {
		return new MapperCollection<HBaseMappingSpecification>().
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(HOSTNAME, INDEX_TABLE, "virttop", ctx.plain)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(ARCH, INDEX_TABLE, "virttop", ctx.plain)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(PHYS_CPUS, INDEX_TABLE, "virttop", ctx.plain)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_COUNT, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_RUNNING, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_BLOCKED, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_PAUSED, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_SHUTDOWN, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_SHUTOFF, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_CRASHED, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_ACTIVE, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(VMS_INACTIVE, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(CPU_UTIL_PERC, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(HARDWARE_MEMORY, INDEX_TABLE, "virttop", ctx.plain)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TOTAL_MEMORY,  SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TOTAL_GUEST_MEMORY, SYSTEM_METRICS, "virttop", ctx.time)).
							addMapping(HBaseMappingSpecification.readKeyAsQuantifierMapping(TOTAL_CPU_TIME, SYSTEM_METRICS, "virttop", ctx.time));
	}
		 
	private VirttopConstants() {
		// no instances of this class
	}
}
