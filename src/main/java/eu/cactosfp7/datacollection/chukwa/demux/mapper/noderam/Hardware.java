/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.noderam;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.noderam.HardwareConstants.HARDWARE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.noderam.HardwareConstants.HARDWARE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.noderam.HardwareConstants.hardwareReadSpec;
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
@Tables(annotations = { @Table(name = CN_TABLE, columnFamily = "hardware_util"), @Table(name = CN_HISTORY_TABLE, columnFamily = "hardware_util") })
public class Hardware extends AbstractProcessorNG {

	static Logger log = Logger.getLogger(Hardware.class);

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

		AbstractProcessorConstants.setTimestamp(recordEntry, collector);
		HardwareConstants.HardwareMetricContext ctx = HardwareConstants.createMapperContext();
		handleHardware(recordEntry, collector, ctx);
	}

	private void handleHardware(String recordEntry, ParallelCollector collector, HardwareConstants.HardwareMetricContext ctx) {
		Map<String, AccountingReadSpecification> hardwareElements = AccountingReadSpecification.fromReadWritemap(hardwareReadSpec);
		try {
			String filename = new File(this.chunk.getStreamName()).getName();
			filename = FilenameUtils.removeExtension(filename);
			AbstractProcessorConstants.addSingleValue("node_name", filename, hardwareElements);
			log.info("RAM-"+filename+recordEntry);
			CSVReader reader = new CSVReader(new StringReader(recordEntry));
//			String[] rowValues = recordEntry.trim().split("[\\r\\n]+");
			int totalCores = 0;//rowValues.length-1;
			long memory_free=0;
			long memory_total=0;
			long memory_buff=0;
			long memory_cache=0;
			long memory_swpd=0;
			String [] nextLine;
			while ((nextLine = reader.readNext()) != null){
				if(!nextLine[0].equals("DATE") && nextLine.length==6){
					memory_total=Long.valueOf(nextLine[1]);
					memory_free=Long.valueOf(nextLine[2]);
					memory_buff=Long.valueOf(nextLine[3]);
					memory_cache=Long.valueOf(nextLine[4]);
					memory_swpd=Long.valueOf(nextLine[5]);
					log.info("RAM-"+filename+": "+nextLine[1]+" "+nextLine[2]);
				}
			}
			if(memory_total!=0)
			{
				AbstractProcessorConstants.addSingleValue("mem_free", Double.valueOf(String.valueOf(memory_free/1024)), hardwareElements);
				AbstractProcessorConstants.addSingleValue("mem_size", Double.valueOf(String.valueOf((memory_total/1048576))), hardwareElements);
				AbstractProcessorConstants.addSingleValue("mem_buff", memory_buff, hardwareElements);
				AbstractProcessorConstants.addSingleValue("mem_swpd", memory_swpd, hardwareElements);
				AbstractProcessorConstants.addSingleValue("mem_cache", memory_cache, hardwareElements);
				collector.addData(hardwareElements).addMappings(HardwareConstants.getHardwareMappers(ctx)).addMappings(HardwareConstants.getHistoryHardwareMappers(ctx));
			}
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
		}
	}

	private static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

}
