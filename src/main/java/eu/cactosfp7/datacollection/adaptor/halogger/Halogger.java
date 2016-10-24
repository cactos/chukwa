package eu.cactosfp7.datacollection.adaptor.halogger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.TimerTask;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.Uptime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * TimerTask for collect HA Proxy stats.
 */
public class Halogger extends TimerTask {

	private static Sigar sigar = new Sigar();
	private static Logger log = Logger.getLogger(Halogger.class);
	private ChunkReceiver receiver = null;
	private long sendOffset = 0;
	private HAConstants haproxyMetrics;
	private HashMap<String, JSONObject> previousHAproxystats = new HashMap<String, JSONObject>();
	private HashMap<String, JSONObject> HAproxy = new HashMap<String, JSONObject>();
	private static final Pattern SPACE = Pattern.compile(" ");

	public Halogger(ChunkReceiver dest, HAConstants haproxyMetrics) {
		receiver = dest;
		this.haproxyMetrics = haproxyMetrics;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		boolean skip = false;
		JSONObject json = new JSONObject();
		try {
			// HA proxy calculator
			String Hostname = "not_found";
			Hostname = sigar.getFQDN();
			JSONArray haProxy = new JSONArray();
			// Get output from halog -srv </var/log/haproxy.log
			Runtime rt = Runtime.getRuntime();
			String[] cmd = { "/bin/sh", "-c", "halog -srv </var/log/haproxy.log" };
			Process proc = rt.exec(cmd);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String str;
			while ((str = stdInput.readLine()) != null) {
				String[] HAcolumns = SPACE.split(str);
				log.info(Arrays.toString(HAcolumns));
				JSONObject haproxyMap = new JSONObject();
				if (isInteger(HAcolumns[2])) {
					haproxyMap.put("2xxs", HAcolumns[2]);
					haproxyMap.put("tot_req", HAcolumns[7]);
					if (previousHAproxystats.containsKey(HAcolumns[0])) {
						JSONObject deltaMap = previousHAproxystats.get(HAcolumns[0]);
						log.info("2xxs=" + "key:" + HAcolumns[0] + "get 200:" + deltaMap.get("2xxs") + "get total" + deltaMap.get("tot_req"));
						log.info(Long.parseLong(haproxyMap.get("2xxs").toString()) - Long.parseLong(deltaMap.get("2xxs").toString()));
						deltaMap.put("2xxs", (Long.parseLong(haproxyMap.get("2xxs").toString()) - Long.parseLong(deltaMap.get("2xxs").toString())));
						deltaMap.put("tot_req", (Long.parseLong(haproxyMap.get("tot_req").toString()) - Long.parseLong(deltaMap.get("tot_req").toString())));
						deltaMap.put("app_comp_name", HAcolumns[0]);
						deltaMap.put("Hostname", Hostname);
						// CURL VM-id
						String[] cmd2 = { "/bin/sh", "-c", "curl -s 'http://169.254.169.254/metadata' |grep -o -P '(?<=<server_uuid>).*(?=</server_uuid>)'" };
						proc = rt.exec(cmd2);
						BufferedReader stdInput2 = new BufferedReader(new InputStreamReader(proc.getInputStream()));
						String UUID;
						if ((UUID = stdInput2.readLine()) != null) {
							deltaMap.put("UUID", UUID);
						}
						haProxy.add(deltaMap);
						skip = false;
					} else {
						haProxy.add(haproxyMap);
						skip = true;
					}
					previousHAproxystats.put(HAcolumns[0], haproxyMap);
				}
			}
			json.put("haProxy", haProxy);
			json.put("timestamp", System.currentTimeMillis());
			byte[] data = json.toString().getBytes();
			sendOffset += data.length;
			ChunkImpl c = new ChunkImpl("haproxyMetrics", "HALogger", sendOffset, data, haproxyMetrics);
			if (!skip) {
				receiver.add(c);
			}
		} catch (Exception se) {
			log.error(ExceptionUtil.getStackTrace(se));
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
