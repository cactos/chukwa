package eu.cactosfp7.datacollection.chukwa.demux.mapper.haproxy;

import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_HISTORY_TABLE;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorConstants.VM_TABLE;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AbstractProcessorNG;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.AccountingReadSpecification;
import eu.cactosfp7.datacollection.chukwa.demux.mapper.ParallelCollector;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.haproxy.HAProxyConstants.HAPROXY_COLUMN_NAMES;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.haproxy.HAProxyConstants.HAPROXY_HEADER_SPLIT_COLUMNS;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.haproxy.HAProxyConstants.haproxyReadSpec;
import static eu.cactosfp7.datacollection.chukwa.demux.mapper.haproxy.HAProxyConstants.VMAPPHISTORY;

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
@Tables(annotations = { @Table(name = VMAPPHISTORY, columnFamily = "app")})
public class HAProxy extends AbstractProcessorNG {

	static Logger log = Logger.getLogger(HAProxy.class);

	@Override
	protected void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable {
		log.info("HAProxy"+recordEntry);
		JSONObject json = (JSONObject) JSONValue.parse(recordEntry);

		long timestamp = ((Long) json.get("timestamp")).longValue();
		collector.setTimestamp(timestamp);
		HAProxyConstants.HAProxyMetricContext ctx = HAProxyConstants.createMapperContext();

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

	private void hardware(JSONObject json, ParallelCollector collection, HAProxyConstants.HAProxyMetricContext ctx) throws IOException {
		try {

			Map<String, AccountingReadSpecification> elements = AccountingReadSpecification.fromReadWritemap(haproxyReadSpec);

			JSONArray appList = (JSONArray) json.get("haProxy");
			int rate_2xx = 0;
			int tot_req_rate = 0;
			String app_name = "";
			String UUID = "not found";
			for (int i = 0; i < appList.size(); i++) {
				JSONObject haproxy = (JSONObject) appList.get(i);
				rate_2xx = Integer.parseInt(haproxy.get("2xxs").toString());
				tot_req_rate = Integer.parseInt(haproxy.get("tot_req").toString());
				app_name = haproxy.get("app_comp_name").toString();
				UUID = haproxy.get("UUID").toString();

				AbstractProcessorConstants.addSingleValueAtIndex("app_comp_name", i, app_name, elements);
				AbstractProcessorConstants.addSingleValueAtIndex("rate_2xx", i, rate_2xx, elements);
				AbstractProcessorConstants.addSingleValueAtIndex("UUID", i, UUID, elements);
				AbstractProcessorConstants.addSingleValueAtIndex("tot_req_rate", i, tot_req_rate, elements);
				AbstractProcessorConstants.addSingleValueAtIndex("vm_status", i, "running", elements);
			}
			collection.addData(elements).addMappings(HAProxyConstants.getHistoryHAProxyMappers(ctx));
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
			log.warn(e);
		}
	}
}
