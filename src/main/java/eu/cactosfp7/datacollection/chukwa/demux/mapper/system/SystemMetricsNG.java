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

/**
 * Demux parser for system metrics data collected through
 * org.apache.hadoop.chukwa.datacollection.adaptor.sigar.SystemMetrics.
 */
package eu.cactosfp7.datacollection.chukwa.demux.mapper.system;

import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.*; 

import java.io.IOException; 
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.system.SystemMetricsConstants.*;

@Tables(annotations={
		@Table(name=SYSTEM_METRICS,columnFamily="cpu"),
		@Table(name=SYSTEM_METRICS,columnFamily="system"),
		@Table(name=SYSTEM_METRICS,columnFamily="memory"),
		@Table(name=SYSTEM_METRICS,columnFamily="network"),
		@Table(name=SYSTEM_METRICS,columnFamily="disk"),
		@Table(name=INDEX_TABLE, columnFamily="cpu")
})
public class SystemMetricsNG extends AbstractProcessorNG {
	private static Logger log = Logger.getLogger(AbstractProcessorNG.class);

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {
		log.info(recordEntry);
		JSONObject json = (JSONObject) JSONValue.parse(recordEntry);
		
		long timestamp = ((Long)json.get("timestamp")).longValue();
		collector.setTimestamp(timestamp);
		SystemMetricContext ctx = SystemMetricsConstants.createMapperContext();
		
		cpuNG(json, collector, ctx);
		system(json, collector, ctx);
		memory(json, collector, ctx);
		networkNG(json, collector, ctx);
		diskNG(json, collector, ctx);
		cluster(collector, ctx);

		// setTimestamps(ALL_FAMILIES, collector, timestamp, ctx);
	}
	/*
	private void setTimestamps(String[] families, ParallelCollector collector, long timestamp, SystemMetricContext ctx) {
		Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(allOtherReadSpec);
		AbstractProcessorConstants.addSingleValue(TIMESTAMP, Long.toString(timestamp), elements);
		for(String fam : families) {
			collector.addData(elements).addMappings(getFilledAllOtherMapping(fam, ctx.plain));
		}
	}*/
	
	private void iterateOverJsonArray(JSONArray list, Map<String, AccountingReadSpecification> map) throws IOException{
		for(int i = 0;i < list.size(); i++) {
			iterateOverJsonObject((JSONObject) list.get(i), i, map);
		}
	}

	@SuppressWarnings("unchecked")
	private void iterateOverJsonObject(JSONObject json, int i, Map<String, AccountingReadSpecification> map) {
		for(String key : (Set<String>) json.keySet()) {
			AbstractProcessorConstants.addAndCount(key, i, (Map<String,?>) json, map);
		}
	}
	
	private void cpuNG(JSONObject json, ParallelCollector collection, SystemMetricContext ctx) throws IOException {
		JSONArray cpuList = (JSONArray) json.get("cpu");
		Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(cpuReadSpec);
		AbstractProcessorConstants.addSingleValue("cpus", cpuList.size(), elements);
		iterateOverJsonArray(cpuList, elements);
		collection.addData(elements).addMappings(SystemMetricsConstants.getCpuMappers(ctx));
	}
	
	private void networkNG(JSONObject json, ParallelCollector collection, SystemMetricContext ctx) throws IOException {
		JSONArray netList = (JSONArray) json.get("network");
		Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(networkReadSpec);
		AbstractProcessorConstants.addSingleValue("nics", netList.size(), elements);
		iterateOverJsonArray(netList, elements);
		collection.addData(elements).addMappings(SystemMetricsConstants.getNetworkMappers(ctx));
	}

	private void diskNG(JSONObject json, ParallelCollector collection, SystemMetricContext ctx) throws IOException {
		JSONArray diskList = (JSONArray) json.get("disk");
		Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(diskReadSpec);
		AbstractProcessorConstants.addSingleValue("disks", diskList.size(), elements);
		iterateOverJsonArray(diskList, elements);
		collection.addData(elements).addMappings(SystemMetricsConstants.getDiskMappers(ctx));
	}

	private void cluster(ParallelCollector collection, SystemMetricContext ctx) throws IOException {
		Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(hwStructureReadSpec);
		AbstractProcessorConstants.addSingleValue("cluster", getChunkTag("cluster"), elements);
		AbstractProcessorConstants.addSingleValue("rack", getChunkTag("rack"), elements);
		collection.addData(elements).addMappings(SystemMetricsConstants.getHwStructureMappers(ctx));
	}

	private void system(JSONObject json, ParallelCollector collection, SystemMetricContext ctx) throws IOException {
		Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(systemReadSpec);
		AbstractProcessorConstants.addSingleValue("Uptime", json.get("uptime").toString(), elements);
		JSONArray loadavg = (JSONArray) json.get("loadavg");
		AbstractProcessorConstants.addSingleValue("LoadAverage.1", loadavg.get(0).toString(), elements);
		AbstractProcessorConstants.addSingleValue("LoadAverage.5", loadavg.get(1).toString(), elements);
		AbstractProcessorConstants.addSingleValue("LoadAverage.15", loadavg.get(2).toString(), elements);
		collection.addData(elements).addMappings(SystemMetricsConstants.getSystemMappers(ctx));
	}

	private void memory(JSONObject json, ParallelCollector collection, SystemMetricContext ctx) throws IOException {
		Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(memoryReadSpec);
		JSONObject memory = (JSONObject) json.get("memory");
		iterateOverJsonObject(memory, 0, elements);
		collection.addData(elements).addMappings(SystemMetricsConstants.getMemoryMappers(ctx));
	}
}
