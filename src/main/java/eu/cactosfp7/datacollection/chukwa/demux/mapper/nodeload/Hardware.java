/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.nodeload;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodeload.HardwareConstants.HARDWARE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodeload.HardwareConstants.HARDWARE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodeload.HardwareConstants.hardwareReadSpec;
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
			log.info("NODELOAD-"+filename+recordEntry);
			CSVReader reader = new CSVReader(new StringReader(recordEntry));
//			String[] rowValues = recordEntry.trim().split("[\\r\\n]+");
			int totalCores = 0;//rowValues.length-1;
			double nodeload=-1;
			String [] nextLine;
			while ((nextLine = reader.readNext()) != null){
				if(!nextLine[0].equals("LOAD") && nextLine.length==4){
					nodeload=Double.valueOf(nextLine[1]);
					nodeload=nodeload/(Integer.valueOf(nextLine[2]));
					log.info("NODELOAD-"+filename+": "+nextLine[1]+" "+nextLine[2]);
				}
			}
			if(nodeload>=0)
			{
				AbstractProcessorConstants.addSingleValue("cpu_usr", nodeload, hardwareElements);
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
