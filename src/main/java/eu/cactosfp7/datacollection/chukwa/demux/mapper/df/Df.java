/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.cactosfp7.datacollection.chukwa.demux.mapper.df;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.df.DfConstants.DF_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.df.DfConstants.DF_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.df.DfConstants.dfReadSpec;

import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.INDEX_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SYSTEM_METRICS;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.df.DfConstants.DfMetricContext;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation;

/**
 * Df:Avail.Z		=> load; one per mounted fs
 * Df:Filesystem.Z	=> static
 * Df:Mounted on.Z 	=> static
 * Df:Size.Z		=> static
 * Df:Use%.Z		=> load
 * Df:Used.Z		=> load
 */

@Annotation.Tables(annotations={
		@Table(name=SYSTEM_METRICS,columnFamily="Df"),
                @Table(name=INDEX_TABLE, columnFamily="Df")
})
public class Df extends AbstractProcessorNG {
	
	static Logger log = Logger.getLogger(AbstractProcessorNG.class);

	public Df() {
		// empty constructor // 
	}
	
	private boolean verifyHeaderElements(String headerLine) {
		String[] header = headerLine.substring(headerLine.indexOf("Filesystem")).split("[\\s]++");
		if(header.length != DF_HEADER_SPLIT_COLUMNS.length) {
			log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
			return false;
		} 
		int i = AbstractProcessorConstants.compareArrays(DF_HEADER_SPLIT_COLUMNS, header, 0);
		if(i < 0) {
			log.warn("host header element at index " + i + " does not match: " + DF_HEADER_SPLIT_COLUMNS[i] + " vs " + header[i]);		  
			return false;
		}
		return true;
	}
	
	private boolean handleFilesystemSequence(String fsInput, Map<String, AccountingReadSpecification> vmElements, int iteration) {
		String[] data = fsInput.split("[\\s]++");
		for(int i = 0; i < DF_COLUMN_NAMES.length; i++) {
			String key = DF_COLUMN_NAMES[i];
			String value = data[i];
			AccountingReadSpecification spec = vmElements.get(key);
			if(spec == null) {
				log.info("ignoring non-specified key: " + key);
			} else {
				AbstractProcessorConstants.addSingleValueAtIndex(key, iteration, value, vmElements);
			}
		}
		return true;
	}
	
	private void handleFileSystems(String[] data, ParallelCollector collector, DfMetricContext ctx) {
		Map<String, AccountingReadSpecification> dfElements = AccountingReadSpecification.fromReadWritemap(dfReadSpec);

                for (int i = 1; i < data.length; i++) {
                     handleFilesystemSequence(data[i], dfElements, i);
               }
		collector.addData(dfElements).addMappings(DfConstants.getDfMappers(ctx));
	}

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

		AbstractProcessorConstants.setTimestamp(recordEntry, collector);
		String body = AbstractProcessorConstants.skipStartSequence(recordEntry,"Filesystem");      
		
		String[] lines = body.split("\n");
		if(!verifyHeaderElements(lines[0])){
			return;
		}
		DfMetricContext ctx = DfConstants.createMapperContext();
		handleFileSystems(lines, collector, ctx);
	}
}
