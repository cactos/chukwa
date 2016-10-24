package eu.cactosfp7.datacollection.chukwa.writer.hbase.row;

public interface IndexableRowKeyGenerator extends RowKeyGenerator {

	int getIndex();

	boolean hasIndex();

}
