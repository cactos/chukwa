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

package eu.cactosfp7.datacollection.chukwa.demux.mapper.virttop;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.virttop.VirttopConstants.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;

import com.opencsv.CSVReader;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.INDEX_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.SYSTEM_METRICS;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.virttop.VirttopConstants.VirttopMetricContext;

@Tables(annotations={
		@Table(name=SYSTEM_METRICS,columnFamily="VirtTop"),
                @Table(name=INDEX_TABLE, columnFamily="VirtTop")
})
public class VirtTop extends AbstractProcessorNG {
	static Logger log = Logger.getLogger(VirtTop.class);
	public final String reduceType = "SystemMetrics";
	public final String recordType =  "VirtTop";//this.getClass().getName();

	// private static final String regex = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) (.*?) (.*?): ";
	// private static final Pattern p = Pattern.compile(regex);
	// private Matcher matcher;

	public VirtTop() {
		// empty constructor //    
	}

	public String getDataType() {
		return recordType;
	}

	private boolean verifyHeaderElements(String[] header) {
		if(header.length != HOST_ELEMENTS.length + VM_ELEMENTS.length) {
			log.warn("header length from data source does not match headers in parser. Update needed? " + Arrays.toString(header));
			return false;
		} 
		int i = AbstractProcessorConstants.compareArrays(HOST_ELEMENTS, header, 0);
		if(i > -1) {
			log.warn("host header element at index " + i + " does not match: " + HOST_ELEMENTS[i] + " vs " + header[i]);		  
			return false;
		}
		final int offset = HOST_ELEMENTS.length;
		i = AbstractProcessorConstants.compareArrays(VM_ELEMENTS, header, offset);
		if(i > -1) {
			log.warn("vm header element at index " + (i + offset)  + " does not match: " + HOST_ELEMENTS[i] + " vs " + header[i+offset]);		  
			return false;
		}
		return true;
	}

	private void handleDataLine(String[] data, ParallelCollector collector, VirttopMetricContext ctx) {
		handleHostElements(data, collector, ctx);
		handleVmElements(data, collector, ctx);
	}

	private boolean handleVmElementSequence(String[] data, Map<String, AccountingReadSpecification> vmElements, int iteration) {
		final int offset = HOST_ELEMENTS.length + iteration * VM_ELEMENTS.length;
		if(offset >= data.length) return false;
		
		for(int i = 0; i < VM_MAP_ELEMENTS.length; i++) {
			String key = VM_MAP_ELEMENTS[i];
			String value = data[i + offset];
			AccountingReadSpecification spec = vmElements.get(key);
			if(spec == null) {
				log.info("ignoring non-specified key: " + key);
			} else {
				AbstractProcessorConstants.addSingleValueAtIndex(key, i, value, vmElements);
			}
		}
		return true;
	}
	
	private void handleVmElements(String[] data, ParallelCollector collector, VirttopMetricContext ctx) {
		Map<String, AccountingReadSpecification> vmElements = AccountingReadSpecification.fromReadWritemap(virtTopVmReadSpec);
		int iteration = 0;
		while(handleVmElementSequence(data, vmElements, iteration)) { iteration++; }
		collector.addData(vmElements).addMappings(VirttopConstants.getVirtTopVmMappers(ctx));
	}

	private void handleHostElements(String[] data, ParallelCollector collector, VirttopMetricContext ctx) {
		Map<String, AccountingReadSpecification> hostElements = AccountingReadSpecification.fromReadWritemap(virtTopHostReadSpec);
		for(int i = 0; i < HOST_ELEMENTS.length; i++) {
			String key = HOST_ELEMENTS[i];
			String value = data[i];
			AccountingReadSpecification spec = hostElements.get(key);
			if(spec == null) {
				log.info("ignoring non-specified key: " + key);
			} else {
				AbstractProcessorConstants.addSingleValueAtIndex(key, i, value, hostElements);
			}
		}
		collector.addData(hostElements).addMappings(VirttopConstants.getVirtTopHostMapping(ctx));
	}

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {

		AbstractProcessorConstants.setTimestamp(recordEntry, collector);
		String body = AbstractProcessorConstants.skipStartSequence(recordEntry,"Hostname");      

		CSVReader reader = new CSVReader(new StringReader(body));
		VirttopMetricContext ctx = VirttopConstants.createMapperContext();
		
		try {
			if(! verifyHeaderElements(reader.readNext()) ) {
				return;
			}
			// always skip first line, as it is incomplete //
			reader.readNext();
			handleDataLine(reader.readNext(), collector, ctx);
		} catch (IOException e) {
			log.warn("I/O Exception when parsing VirtTop data", e);
		}catch (Throwable e) {
			log.warn("Other exception when parsing VirtTop data", e);
		} finally {
			reader.close();
		}
	}
}
