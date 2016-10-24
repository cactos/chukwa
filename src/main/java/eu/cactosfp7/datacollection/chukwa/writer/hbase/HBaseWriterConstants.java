package eu.cactosfp7.datacollection.chukwa.writer.hbase;

final class HBaseWriterConstants {

	static final String CHUKWA_DEFAULT_DEMUX_PROCESSOR_KEY = "chukwa.demux.mapper.default.processor";
	static final String CHUKWA_DEFAULT_DEMUX_PROCESSOR_VALUE = "org.apache.hadoop.chukwa.extraction.demux.processor.mapper.DefaultProcessor";
	static final String HBASE_WRITER_SCHEMA_VERIFICATION_KEY = "hbase.writer.verify.schema";
	static final boolean HBASE_WRITER_SCHEMA_VERIFICATION_DEFAULT = false;
	static final String HBASE_HALT_ON_INVALID_SCHEMA_KEY = "hbase.writer.halt.on.schema.mismatch";
	static final boolean HBASE_HALT_ON_INVALID_SCHEMA_DEFAULT = true;
}
