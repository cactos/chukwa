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

package eu.cactosfp7.datacollection.chukwa.demux.mapper.vmsigar;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmsigar.VMSigarConstants.KVMTOP_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmsigar.VMSigarConstants.KVMTOP_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.vmsigar.VMSigarConstants.kvmtopReadSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author CACTOS
 */
@Tables(annotations = { @Table(name = VM_TABLE, columnFamily = "hardware"), @Table(name = VM_TABLE, columnFamily = "storage"), @Table(name = VM_TABLE, columnFamily = "network"),
		@Table(name = VM_HISTORY_TABLE, columnFamily = "hardware"), @Table(name = VM_HISTORY_TABLE, columnFamily = "storage"), @Table(name = VM_HISTORY_TABLE, columnFamily = "network") })
public class VMSigar extends AbstractProcessorNG {

	static Logger log = Logger.getLogger(VMSigar.class);

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {
//		log.info(recordEntry);
		JSONObject json = (JSONObject) JSONValue.parse(recordEntry);

		long timestamp = ((Long) json.get("timestamp")).longValue();
		collector.setTimestamp(timestamp);
		VMSigarConstants.KvmTopMetricContext ctx = VMSigarConstants.createMapperContext();

		hardware(json, collector, ctx);
	}

	private void iterateOverJsonArray(JSONArray list, Map<String, AccountingReadSpecification> map) throws IOException {
		for (int i = 0; i < list.size(); i++) {
			iterateOverJsonObject((JSONObject) list.get(i), i, map);
		}
	}

	@SuppressWarnings("unchecked")
	private void iterateOverJsonObject(JSONObject json, int i, Map<String, AccountingReadSpecification> map) {
		for (String key : (Set<String>) json.keySet()) {
			AbstractProcessorConstants.addAndCount(key, i, (Map<String, ?>) json, map);
		}
	}

	private void hardware(JSONObject json, ParallelCollector collection, VMSigarConstants.KvmTopMetricContext ctx) throws IOException {
		try {
			JSONArray cpuList = (JSONArray) json.get("cpu");
			Double combined = 0.0;
			Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(kvmtopReadSpec);
			for (int i = 0; i < cpuList.size(); i++) {
				JSONObject cpu = (JSONObject) cpuList.get(i);
				String tt=cpu.get("user").toString();
				String tt2=cpu.get("sys").toString();
//				log.info(tt);
				combined = combined + Double.parseDouble(tt)+Double.parseDouble(tt2);
			}
			combined = combined / cpuList.size();

			JSONObject memory = (JSONObject) json.get("memory");
			Double ram_total = 0.0;
			Double ram_used = 0.0;
			ram_total = Double.parseDouble(memory.get("Ram").toString());
			ram_used = Double.parseDouble(memory.get("UsedPercent").toString());

			JSONArray networkList = (JSONArray) json.get("network");
			Double network = 0.0;
			String Hostname = "not found";
			String UUID = "not found";
			for (int i = 0; i < networkList.size(); i++) {
				JSONObject net = (JSONObject) networkList.get(i);
				network = network + Double.parseDouble(net.get("TxBytes").toString());
				network = network + Double.parseDouble(net.get("RxBytes").toString());
				Hostname = net.get("Hostname").toString();
				if (net.containsKey("UUID")) {
					UUID = net.get("UUID").toString();
				}
			}

			JSONArray diskList = (JSONArray) json.get("disk");
			Double disk_read = 0.0;
			Double disk_write = 0.0;
			Double disk_total = 0.0;
			Double disk_used = 0.0;
			for (int i = 0; i < diskList.size(); i++) {
				JSONObject net = (JSONObject) diskList.get(i);
				disk_read = disk_read + Double.parseDouble(net.get("ReadBytes").toString());
				disk_write = disk_write + Double.parseDouble(net.get("WriteBytes").toString());
				disk_total = disk_total + Double.parseDouble(net.get("Total").toString());
				disk_used = disk_used + Double.parseDouble(net.get("Used").toString());
			}

			AbstractProcessorConstants.addSingleValue("CpuCS", cpuList.size(), elements);
			AbstractProcessorConstants.addSingleValue("CpuVM", Double.toString(combined*100.0) + "%", elements);
			AbstractProcessorConstants.addSingleValue("UUID", UUID, elements);
			AbstractProcessorConstants.addSingleValue("ram-total", ram_total.longValue() + "MB", elements);
			AbstractProcessorConstants.addSingleValue("ram-used", ram_used.toString() + "%", elements);
			AbstractProcessorConstants.addSingleValue("vmname", Hostname, elements);
			AbstractProcessorConstants.addSingleValue("network", Double.toString(network / 1048576.0) + "MB/s", elements);
			AbstractProcessorConstants.addSingleValue("disk-read", (disk_read / 1048576) + "MB/s", elements);
			AbstractProcessorConstants.addSingleValue("disk-total", (long)(disk_total / 1024) + "MB", elements);
			AbstractProcessorConstants.addSingleValue("disk-used", (disk_used / 1024) + "MB", elements);
			AbstractProcessorConstants.addSingleValue("disk-write", (disk_write / 1048576) + "MB/s", elements);
			AbstractProcessorConstants.addSingleValue("vm_state", "running", elements);
			

			// iterateOverJsonArray(cpuList, elements);
			collection.addData(elements).addMappings(VMSigarConstants.getKvmTopMappers(ctx)).addMappings(VMSigarConstants.getHistoryKvmTopMappers(ctx));
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
			log.warn(e);
			log.error(json.toJSONString());
			e.printStackTrace();
		}
	}
}
