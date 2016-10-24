/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.nodenet;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodenet.HardwareConstants.HARDWARE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodenet.HardwareConstants.HARDWARE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodenet.HardwareConstants.hardwareReadSpec;
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
@Tables(annotations = { @Table(name = CN_TABLE, columnFamily = "network_util"), @Table(name = CN_HISTORY_TABLE, columnFamily = "network_util") })
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
			CSVReader reader = new CSVReader(new StringReader(recordEntry));
			Double net_through=0.0;
			String [] nextLine;
			while ((nextLine = reader.readNext()) != null){
				if(!nextLine[0].equals("DATE") && nextLine.length==3){
					net_through=0.0;
					net_through+=Double.valueOf(nextLine[1]);
					net_through+=Double.valueOf(nextLine[2]);
				}
			}
			AbstractProcessorConstants.addSingleValue("net_through", net_through.intValue(), hardwareElements);
			AbstractProcessorConstants.addSingleValue("netw_speed", "1000Mb/s", hardwareElements);
			collector.addData(hardwareElements).addMappings(HardwareConstants.getHardwareMappers(ctx)).addMappings(HardwareConstants.getHistoryHardwareMappers(ctx));
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