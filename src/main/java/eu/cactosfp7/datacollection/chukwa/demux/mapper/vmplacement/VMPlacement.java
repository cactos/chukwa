/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.generatePrefixedHardwareReadSpec;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.generatePrefixedVMReadSpec;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.generateSpecificReadKey;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_IMAGE_UUID;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_NAME;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_STATE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_UUID;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.NODE_NAME;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_NUMBER;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_SNAPSHOT_CORES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_SNAPSHOT_RAM;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_SNAPSHOT_STATE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants.VM_HOST_NAME;
import java.util.Locale;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.opencsv.CSVReader;

import java.io.IOException;

/**
 *
 * @author CACTOS
 */
@Tables(annotations = { @Table(name = CN_TABLE, columnFamily = "vms"), 
		@Table(name = CN_HISTORY_TABLE, columnFamily = "vms"), @Table(name = VM_TABLE, columnFamily = "meta"), 
		@Table(name = VM_HISTORY_TABLE, columnFamily = "meta")})
public class VMPlacement extends AbstractProcessorNG {

	static Logger log = Logger.getLogger(VMPlacement.class);
	private SimpleDateFormat sdf = null;
	
	public VMPlacement() {
		// Sample of the Date format
		// Fri Sep 11 06:30:07 UTC 2015
		// Mon Dec 14 06:30:24 UTC 2015
		sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {
		AbstractProcessorConstants.setTimestamp(recordEntry, collector);
		handleHardware(recordEntry, collector);
	}

	private ListMultimap<String, VMData> parseInputFileToMultiMap(String recordEntry) throws IOException, ParseException {
		CSVReader reader = new CSVReader(new StringReader(recordEntry));
		String[] nextLine;
		Date d;
		// Group the VMs into nodes
		ListMultimap<String, VMData> NodeVMmultiMap = ArrayListMultimap.create();
		try {
			while ((nextLine = reader.readNext()) != null) {
				 // log.info("Cell number:"+nextLine.length);
				if (nextLine[0].equals("Date")) {
					NodeVMmultiMap.clear();
				}
				if (!nextLine[0].equals("Date") && nextLine.length==13) {
					d = sdf.parse(nextLine[0]);
					VMData currentVM = new VMData(nextLine[2], nextLine[8], nextLine[1], nextLine[10], d.getTime(), Integer.valueOf(nextLine[4]), Integer.valueOf(nextLine[3]));
					if (StringUtils.isNotBlank(nextLine[7])) {
						NodeVMmultiMap.put(nextLine[7], currentVM);
//						log.info(nextLine[7]);
					}
					if (nextLine[8].equals("STOPPED")){
						NodeVMmultiMap.put("STOPPED", currentVM);
					}
//					 log.info(nextLine[7]);
				}
//				 log.info(nextLine.length);
			}
		}finally {
			reader.close();
		}
		return NodeVMmultiMap;
	}
	
	private void initialiseFCOhypervisors(ListMultimap<String, VMData> nodeVMmultiMap, ParallelCollector collector) {
		// initialise FCO node hypervisors to pass kvm status checkpoint (not included in FCOPlacement.csv)
		//If no VMs are in a node, the node will not appear on the vms column of CNSnapshot and this will cause an error when validating on the Model Updater
		//The following bypasses the previous problem by adding "virtual agents" on all the nodes that have hypervisors (by listing the nodes that would have an agent on the hypervisor level as in OpenStack)
		String FCO_nodes[]={"10.157.128.30", "10.157.128.31", "10.157.128.32", "10.158.128.11", "10.158.128.12", "10.158.128.13", "10.158.128.14", "10.158.128.15", "10.158.128.16", "10.158.128.17", "10.158.128.18"};
		for(String hypervisor:FCO_nodes)
		{
			if (!nodeVMmultiMap.containsKey(hypervisor))
			{
				log.info("empty hypervisor");
				Collection<VMData> VMList = Collections.EMPTY_LIST;
				addVmData(hypervisor, VMList, collector);
			}
		}
	}

	private void addVmData(String VMkey, Collection<VMData> VMList, ParallelCollector collector) {
		log.info("collecting with prefix: " + VMkey);
		Map<String, AccountingReadSpecification> hardwareElements = AccountingReadSpecification.fromReadWritemap(generatePrefixedHardwareReadSpec(VMkey));
		VMPlacementConstants.HardwareMetricContext ctx = VMPlacementConstants.createMapperContext(VMkey);
		AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(VMkey, NODE_NAME), VMkey, hardwareElements);
		int counter = 0;
		for (VMData VMiterator : VMList) {
			//log.info(VMiterator.IPNAME+" "+VMiterator.UUID+" "+VMiterator.IMAGE_UUID+" "+VMkey);
			Map<String, AccountingReadSpecification> vmsnapshotElements = AccountingReadSpecification.fromReadWritemap(generatePrefixedVMReadSpec(VMiterator.UUID));
			VMPlacementConstants.HardwareMetricContext ctx2 = VMPlacementConstants.createMapperContext(VMiterator.UUID);
			AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(VMiterator.UUID, NODE_NAME), VMiterator.UUID, vmsnapshotElements);
			AbstractProcessorConstants.addSingleValueAtIndex(generateSpecificReadKey(VMkey, VM_IMAGE_UUID), counter, VMiterator.IMAGE_UUID, hardwareElements);
			AbstractProcessorConstants.addSingleValueAtIndex(generateSpecificReadKey(VMkey, VM_NAME), counter, VMiterator.IPNAME, hardwareElements);
			AbstractProcessorConstants.addSingleValueAtIndex(generateSpecificReadKey(VMkey, VM_STATE), counter, VMiterator.STATE, hardwareElements);
			AbstractProcessorConstants.addSingleValueAtIndex(generateSpecificReadKey(VMkey, VM_UUID), counter, VMiterator.UUID, hardwareElements);
			AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(VMiterator.UUID, VM_SNAPSHOT_STATE), VMiterator.STATE, vmsnapshotElements);
			AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(VMiterator.UUID, VM_SNAPSHOT_RAM), Integer.toString(VMiterator.RAM)+"MB", vmsnapshotElements);
			AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(VMiterator.UUID, VM_SNAPSHOT_CORES), VMiterator.CORES, vmsnapshotElements);
			AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(VMiterator.UUID, VM_HOST_NAME), VMkey, vmsnapshotElements);
			if (StringUtils.isNotBlank(VMkey.toString()) && !VMkey.equals("null")) {
//				log.info("vmsnapshotElements: " + VMiterator.CORES);
				collector.addData(vmsnapshotElements).addMappings(VMPlacementConstants.getVMSnapshotMappers(VMiterator.UUID, ctx2)).addMappings(VMPlacementConstants.getVMHistoryMappers(VMiterator.UUID, ctx2));
			}
			counter++;
		}
		if (StringUtils.isNotBlank(VMkey.toString()) && !VMkey.equals("null") && !VMkey.equals("STOPPED")) {
			AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(VMkey, VM_NUMBER), counter, hardwareElements);
			log.info("hardwareElements: " + hardwareElements.keySet());
			collector.addData(hardwareElements).addMappings(VMPlacementConstants.getHardwareMappers(VMkey, ctx)).addMappings(VMPlacementConstants.getHistoryHardwareMappers(VMkey, ctx));
		}
	}
	
	private void handleHardware(String recordEntry, ParallelCollector collector) {
		try {
			log.info("VMfile");
//			log.info(recordEntry);
			ListMultimap<String, VMData> NodeVMmultiMap = parseInputFileToMultiMap(recordEntry);
			log.info("multimap keys: " + NodeVMmultiMap.keys());
			NodeVMmultiMap.entries();
			// Place VMs per node in the table
			for (Map.Entry<String, Collection<VMData>> e: NodeVMmultiMap.asMap().entrySet()) {
				addVmData(e.getKey(), e.getValue(), collector);
			}
			initialiseFCOhypervisors(NodeVMmultiMap, collector);
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
		}
	}

}