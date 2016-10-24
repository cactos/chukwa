package eu.cactosfp7.datacollection.chukwa.writer.hbase;

import java.util.List; 

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.util.ClassUtils;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import static eu.cactosfp7.datacollection.chukwa.writer.hbase.HBaseWriterConstants.*;

final class HBaseSchemaValidator {

	private final static Logger log = Logger.getLogger(HBaseWriterNG.class);

	static void verifyHbaseSchema(Configuration conf, Configuration hconf) {
		if(!conf.getBoolean(HBASE_WRITER_SCHEMA_VERIFICATION_KEY, HBASE_WRITER_SCHEMA_VERIFICATION_DEFAULT)) return;
		log.debug("Verify Demux parser with HBase schema");
		boolean isValid = true;
		try {
			HBaseAdmin admin = new HBaseAdmin(hconf);
			List<Class> demuxParsers = ClassUtils.getClassesForPackage(conf.get("hbase.demux.package"));
			for(Class<?> x : demuxParsers) {
				isValid = isValid && verifyTablesOfClass(admin, x);
			}
		} catch (Exception e) {
			isValid = false;
			log.error(ExceptionUtil.getStackTrace(e));
		}
		handleSchemaValidationResult(isValid, conf);
	}

	private static void handleSchemaValidationResult(boolean isValid, Configuration conf) {
		if(isValid) return;
		log.error("Hbase schema mismatch with demux parser.");
		if(conf.getBoolean(HBASE_HALT_ON_INVALID_SCHEMA_KEY, HBASE_HALT_ON_INVALID_SCHEMA_DEFAULT)) {
			log.error("Exiting...");
			DaemonWatcher.bailout(-1);
		}
	}

	private static <T> boolean verifyTablesOfClass(HBaseAdmin admin, Class<?> x) {
		boolean isValid = true;
		if(x.isAnnotationPresent(Tables.class)) {
			Tables list = x.getAnnotation(Tables.class);
			for(Table table : list.annotations()) {
				isValid = isValid && verifyTableOfClass(admin, table);               
			}
		} else if(x.isAnnotationPresent(Table.class)) {
			Table table = x.getAnnotation(Table.class);
			isValid = isValid && verifyTableOfClass(admin, table);
		}	
		return isValid;
	}

	private static boolean verifyTableOfClass(HBaseAdmin admin, Table table) {
		if(verifyHbaseTable(admin, table)) return true;
		log.warn("Validation failed - table: "+table.name()+" column family: "+table.columnFamily()+" does not exist.");
		return false;
	}

	private static boolean verifyHbaseTable(HBaseAdmin admin, Table table) {
		try {
			final String columnFamily = table.columnFamily();
			if(columnFamily == null) return false;

			if(!admin.tableExists(table.name())) {
				log.fatal("HBase table: "+table.name()+ " does not exist.");
				return false;
			}
			HTableDescriptor descriptor = admin.getTableDescriptor(table.name().getBytes());
			HColumnDescriptor[] columnDescriptors = descriptor.getColumnFamilies();
			for(HColumnDescriptor cd : columnDescriptors) {
				if(columnFamily.equals(cd.getNameAsString())) return true;
			}
			log.info("Verified schema - table: "+table.name()+" column family: "+table.columnFamily() + " not found");
		} catch(Exception e) {
			log.error(ExceptionUtil.getStackTrace(e));
		}
		return false;
	}



	private HBaseSchemaValidator() {
		// no instances of this class
	}
}
