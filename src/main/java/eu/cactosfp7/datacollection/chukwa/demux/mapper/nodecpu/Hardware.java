/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.nodecpu;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.CN_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.nodecpu.HardwareConstants;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodecpu.HardwareConstants.HARDWARE_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodecpu.HardwareConstants.HARDWARE_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.nodecpu.HardwareConstants.hardwareReadSpec;
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
@Tables(annotations = { @Table(name = CN_TABLE, columnFamily = "hardware"), @Table(name = CN_HISTORY_TABLE, columnFamily = "hardware") })
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
			// String[] rowValues = recordEntry.trim().split("[\\r\\n]+");
			int totalCores = 0;// rowValues.length;
			// record.add("TotalCores.0", Integer.toString(totalCores));
			Double cpu_freq = 0.0;
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				if (!nextLine[0].equals("DATE") && nextLine.length == 4) {
					// log.info("freq:"+filename+" "+nextLine[0]+"
					// "+nextLine[1]+" "+nextLine[2]);
					if (Integer.valueOf(nextLine[1]) == 0) {
						totalCores = 0;
					}
					cpu_freq = Double.valueOf(nextLine[3]);
					totalCores++;
				}
			}
			// for (int i = 0; i < totalCores; i++) {
			// String[] CellValues = rowValues[i].trim().split("\\s*,\\s*");
			//// log.info("CellValues.length"+CellValues.length);
			// if (CellValues.length == 4) {
			// if (isInteger(CellValues[1])) {
			// // record.add(("user." + CellValues[1]), CellValues[2]);
			// // record.add(("Mhz." + CellValues[1]), CellValues[3]);
			// cpu_freq=Double.valueOf(CellValues[3]);
			// }
			// }
			// }
			// AbstractProcessorConstants.addSingleValue("cpu_cores",
			// totalCores, hardwareElements);
			log.info("cpu_freq=" + cpu_freq);
			if (totalCores != 0 && cpu_freq>0) {
				AbstractProcessorConstants.addSingleValue("cpu_freq", cpu_freq, hardwareElements);
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
