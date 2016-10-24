/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.storage;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement.VMPlacementConstants;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.HARDWARE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.HARDWARE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.generatePrefixedHardwareReadSpec;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.generateSpecificReadKey;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.NODE_NAME;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.AVAILABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.generatePrefixedHardwareReadSpec;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.storage.HardwareConstants.generateSpecificReadKey;

import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.opencsv.CSVReader;

import java.io.File;
import java.io.StringReader;

/**
 *
 * @author CACTOS
 */
@Tables(annotations = { @Table(name = CN_TABLE, columnFamily = "storage"), @Table(name = CN_HISTORY_TABLE, columnFamily = "storage") })
public class Hardware extends AbstractProcessorNG {

	static Logger log = Logger.getLogger(Hardware.class);

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {
		AbstractProcessorConstants.setTimestamp(recordEntry, collector);
		handleHardware(recordEntry, collector);
	}

	private void handleHardware(String recordEntry, ParallelCollector collector) {
		try{
		String filename = new File(this.chunk.getStreamName()).getName();
		filename = FilenameUtils.removeExtension(filename);
		Map<String, AccountingReadSpecification> hardwareElements = AccountingReadSpecification.fromReadWritemap(generatePrefixedHardwareReadSpec(filename));
		HardwareConstants.HardwareMetricContext ctx = HardwareConstants.createMapperContext(filename);
		
		AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(filename, NODE_NAME), filename, hardwareElements);
		
		
//		Map<String, AccountingReadSpecification> hardwareElements = AccountingReadSpecification.fromReadWritemap(hardwareReadSpec);
//		try {
//			String filename = new File(this.chunk.getStreamName()).getName();
//			filename = FilenameUtils.removeExtension(filename);
//			AbstractProcessorConstants.addSingleValue("node_name", filename, hardwareElements);
//			CSVReader reader = new CSVReader(new StringReader(recordEntry));
////			String[] rowValues = recordEntry.trim().split("[\\r\\n]+");
//			int totalCores = 0;//rowValues.length-1;
//			Double used_bytes=0.0;
//			String [] nextLine;
//			while ((nextLine = reader.readNext()) != null){
//				if(!nextLine[0].equals("DATE")){
//					used_bytes+=Double.valueOf(nextLine[1]);
////					net_through+=Double.valueOf(nextLine[2]);
//				}
//			}
//			AbstractProcessorConstants.addSingleValue("net_through", used_bytes, hardwareElements);
	//	AbstractProcessorConstants.addSingleValue(generateSpecificReadKey(keyrow, NODE_NAME), 0, hardwareElements);
		collector.addData(hardwareElements).addMappings(HardwareConstants.getHardwareMappers(filename, ctx)).addMappings(HardwareConstants.getHistoryHardwareMappers(filename, ctx));
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
		}}
	

	private static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

}
