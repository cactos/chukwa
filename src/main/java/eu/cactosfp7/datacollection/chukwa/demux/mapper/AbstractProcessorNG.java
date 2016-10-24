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

package eu.cactosfp7.datacollection.chukwa.demux.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.ChunkSaver;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.util.RecordConstants;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import eu.cactosfp7.datacollection.chukwa.writer.hbase.KeyRecordPair;
import eu.cactosfp7.datacollection.chukwa.writer.hbase.row.CactosGenericRowKeyGenerator;

public abstract class AbstractProcessorNG implements MapProcessor {
	static Logger log = Logger.getLogger(AbstractProcessorNG.class);

	private byte[] bytes;
	private int[] recordOffsets;
	private int currentPos = 0;
	private int startOffset = 0;

	private ChukwaArchiveKey archiveKey = null;
	
	protected Chunk chunk = null;

	private boolean chunkInErrorSaved = false;
	private OutputCollector<ChukwaRecordKey, ChukwaRecord> output = null;
	Reporter reporter = null;

	public AbstractProcessorNG() {
	}

	@Override
	public final void process(ChukwaArchiveKey archiveKey, Chunk chunk, OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter) {
		
		ParallelCollector collector = new ParallelCollector(chunk.getSource());
		
		chunkInErrorSaved = false;

		this.archiveKey = archiveKey;
		this.output = output;
		this.reporter = reporter;

		reset(chunk);

		while (hasNext()) {
			try {
				parse(nextLine(), reporter, collector);
			} catch (Throwable e) {
				log.warn("cannot save chunk [" + chunk + "], branching to error case", e);
				saveChunkInError(e);
			}
		}
		
		long timestamp = collector.getTimestamp();
		List<KeyRecordPair> entries = collector.harvest();
		
		for(KeyRecordPair value : entries) {
			buildGenericRecordWithoutBody(value.value, timestamp, chunk);
			try {
				buildKeyAndAddToOutput(value, timestamp, chunk.getSource());
			} catch (Throwable ex) {
				log.warn("cannot save chunk [" + chunk + "], branching to error case", ex);
				saveChunkInError(ex);
		    }
		}
	}
	
	protected abstract void parse(String recordEntry, Reporter reporter, ParallelCollector collector) throws Throwable;

	protected final String getChunkTag(String string) {
		return chunk.getTag(string);
	}
	
	protected final void buildKeyAndAddToOutput(KeyRecordPair pair, long timestamp, String dataSource) throws IOException {
		assert CactosGenericRowKeyGenerator.isValidKey(pair.key.getKey()) : "invalid key";
		// pair.key.setKey(CactosGenericRowKeyGenerator.generateRowKey(chunk, timestamp));
		assert pair.key.getReduceType() != null : "reduce type (cfamily) has not been set";
		output.collect(pair.key, pair.value);
	}

	protected final static void buildGenericRecordWithoutBody(ChukwaRecord record, long timestamp, Chunk chunk) {
		buildGenericRecord(record, null, timestamp, chunk);
	}
	
	protected final static void buildGenericRecord(ChukwaRecord record, String body, long timestamp, Chunk chunk) {

		if (body != null) {
			record.add(Record.bodyField, body);
		}
		record.setTime(timestamp);

		record.add(Record.tagsField, chunk.getTags());
		record.add(Record.sourceField, chunk.getSource());
		record.add(Record.applicationField, chunk.getStreamName());
	}

	protected final String nextLine() {
		String log = new String(bytes, startOffset, (recordOffsets[currentPos]
				- startOffset + 1));
		startOffset = recordOffsets[currentPos] + 1;
		currentPos++;
		return RecordConstants.recoverRecordSeparators("\n", log);
	}  

	private void saveChunkInError(Throwable throwable) {
		if (chunkInErrorSaved == false) {
			try {
				ChunkSaver.saveChunk(chunk, throwable, output, reporter);
				chunkInErrorSaved = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void reset(Chunk chunk) {
		this.chunk = chunk;
		this.bytes = chunk.getData();
		this.recordOffsets = chunk.getRecordOffsets();
		currentPos = 0;
		startOffset = 0;
	}

	private boolean hasNext() {
		return (currentPos < recordOffsets.length);
	}
}
