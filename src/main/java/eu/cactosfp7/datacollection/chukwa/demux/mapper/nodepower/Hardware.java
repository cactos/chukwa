/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.nodepower;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodepower.HardwareConstants.HARDWARE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodepower.HardwareConstants.HARDWARE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodepower.HardwareConstants.hardwareReadSpec;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodepower.HardwareConstants.nodeReadSpec;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
@Tables(annotations = { @Table(name = CN_TABLE, columnFamily = "power"), @Table(name = CN_TABLE, columnFamily = "power_util"), @Table(name = CN_HISTORY_TABLE, columnFamily = "power"),
		@Table(name = CN_HISTORY_TABLE, columnFamily = "power_util") })
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
		Map<String, AccountingReadSpecification> nodeElements = AccountingReadSpecification.fromReadWritemap(nodeReadSpec);
		try {
			log.info(recordEntry);
			String filename = new File(this.chunk.getStreamName()).getName();
			filename = FilenameUtils.removeExtension(filename);
			String consumption = "NaN";
			String serial = filename.replace(".", "");
			String PWRalloc = "NaN";
			Pattern p = Pattern.compile("\\bcfgServerActualPowerConsumption=(.*) AC");
			Pattern p2 = Pattern.compile("\\bNo power data available");
			Pattern p3 = Pattern.compile("\\bcfgServerPowerAllocation=(.*) AC");
			Matcher m2 = p2.matcher(recordEntry);
			while (m2.find()) {
				consumption = "notFound";
				PWRalloc = "notFound";
				log.info(consumption);
			}
			if (consumption.equals("NaN")) {
				CSVReader reader = new CSVReader(new StringReader(recordEntry));
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					log.info(nextLine[0]);
					Matcher m = p.matcher(nextLine[0]);
					while (m.find()) {
						consumption = m.group(1);
						log.info(consumption);
					}
					Matcher m3 = p3.matcher(nextLine[0]);
					while (m3.find()) {
						PWRalloc = m3.group(1);
					}
				}
			}
			log.info("SERIAL:"+serial);
			if (!consumption.equals("NaN") && !consumption.equals("notFound")) {
				AbstractProcessorConstants.addSingleValue("node_name",filename, nodeElements);
				AbstractProcessorConstants.addSingleValue("consumption",consumption, nodeElements);
				AbstractProcessorConstants.addSingleValueAtIndex("capacity", 1, (Integer.parseInt(PWRalloc))/2, hardwareElements);
				AbstractProcessorConstants.addSingleValueAtIndex("capacity", 2, (Integer.parseInt(PWRalloc))/2, hardwareElements);
				AbstractProcessorConstants.addSingleValueAtIndex("serial", 1, (serial+"0"), hardwareElements);
				AbstractProcessorConstants.addSingleValueAtIndex("serial", 2, (serial+"1"), hardwareElements);
				collector.addData(hardwareElements).addMappings(HardwareConstants.getHardwareMappers(ctx)).addMappings(HardwareConstants.getHistoryHardwareMappers(ctx));
				collector.addData(nodeElements).addMappings(HardwareConstants.getNodeMappers(ctx)).addMappings(HardwareConstants.getHistoryNodeMappers(ctx));
			} else if (consumption.equals("notFound")) {
				AbstractProcessorConstants.addSingleValue("node_name",filename, nodeElements);
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

