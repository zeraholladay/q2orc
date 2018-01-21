package com.zeraholladay;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class OrcWriter {
	private String outfile;

	private ResultSetMetaData metaData;
	private VectorizedRowBatch batch;
	private Writer writer;

	private Configuration conf = new Configuration();
	private TypeDescription schema = TypeDescription.createStruct();

	public void build() throws SQLException, IllegalArgumentException,
			IOException {
		int columnCount = metaData.getColumnCount() + 1;
		for (int columnIndex = 1; columnIndex < columnCount; columnIndex++) {
			String columnName = metaData.getColumnName(columnIndex);
			switch (metaData.getColumnType(columnIndex)) {
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
				schema.addField(columnName, TypeDescription.createString());
				break;
			case java.sql.Types.NUMERIC:
			case java.sql.Types.DECIMAL:
				schema.addField(columnName, TypeDescription.createString());
				break;
			case java.sql.Types.BIT:
				schema.addField(columnName, TypeDescription.createBoolean());
				break;
			case java.sql.Types.TINYINT:
			case java.sql.Types.SMALLINT:
			case java.sql.Types.INTEGER:
				schema.addField(columnName, TypeDescription.createInt());
				break;
			case java.sql.Types.BIGINT:
				schema.addField(columnName, TypeDescription.createLong());
				break;
			case java.sql.Types.REAL:
				schema.addField(columnName, TypeDescription.createFloat());
				break;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				schema.addField(columnName, TypeDescription.createDouble());
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				schema.addField(columnName, TypeDescription.createBinary());
				break;
			case java.sql.Types.DATE:
				schema.addField(columnName, TypeDescription.createDate());
				break;
			case java.sql.Types.TIME:
			case java.sql.Types.TIMESTAMP:
				schema.addField(columnName, TypeDescription.createTimestamp());
				break;
			default:
				throw new SQLException("Unmapped type!");
			}
		}

		Path path = new Path(getOutfile());
		WriterOptions writerOptions = OrcFile.writerOptions(conf).setSchema(
				schema);

		writer = OrcFile.createWriter(path, writerOptions);

		batch = schema.createRowBatch();
	}

	public void split(ResultSet row) throws SQLException, IOException {
		int columnCount = metaData.getColumnCount() + 1;
		for (int columnIndex = 1; columnIndex < columnCount; columnIndex++) {
			switch (metaData.getColumnType(columnIndex)) {
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
				write(row.getString(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.NUMERIC:
			case java.sql.Types.DECIMAL:
				write(row.getString(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.BIT:
				write(row.getBoolean(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.TINYINT:
			case java.sql.Types.SMALLINT:
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
				write(row.getLong(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.REAL:
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				write(row.getDouble(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				write(row.getBytes(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.DATE:
				write(row.getDate(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.TIME:
				write(row.getTime(columnIndex), columnIndex - 1);
				break;
			case java.sql.Types.TIMESTAMP:
				write(row.getTimestamp(columnIndex), columnIndex - 1);
				break;
			default:
				throw new SQLException("Unmapped type!");
			}
		}
		if (++batch.size == batch.getMaxSize()) {
			writer.addRowBatch(batch);
			batch.reset();
		}
	}

	public void close() throws IOException {
		if (batch.size != 0) {
			writer.addRowBatch(batch);
			batch.reset();
		}
		writer.close();
	}

	private void write(byte[] cell, int columnIndex) throws IOException {
		BytesColumnVector stringVector = (BytesColumnVector) batch.cols[columnIndex];
		stringVector.setVal(batch.size, cell);
	}

	private void write(String cell, int columnIndex) throws IOException {
		if (null == cell) {
			cell = "";
		}
		BytesColumnVector stringVector = (BytesColumnVector) batch.cols[columnIndex];
		stringVector.setVal(batch.size, cell.getBytes());
	}

	private void write(boolean cell, int columnIndex) {
		LongColumnVector vector = (LongColumnVector) batch.cols[columnIndex];
		vector.vector[batch.size] = cell ? 1 : 0;
	}

	private void write(long cell, int columnIndex) {
		LongColumnVector vector = (LongColumnVector) batch.cols[columnIndex];
		vector.vector[batch.size] = cell;
	}

	private void write(double cell, int columnIndex) {
		DoubleColumnVector vector = (DoubleColumnVector) batch.cols[columnIndex];
		vector.vector[batch.size] = cell;
	}

	private void write(java.sql.Date cell, int columnIndex) {
		write(cell.getTime(), columnIndex);
	}

	private void write(java.sql.Time cell, int columnIndex) {
		write(cell.getTime(), columnIndex);
	}

	private void write(java.sql.Timestamp cell, int columnIndex) {
		TimestampColumnVector vector = (TimestampColumnVector) batch.cols[columnIndex];
		vector.time[batch.size] = cell.getTime();
		vector.nanos[batch.size] = cell.getNanos();
	}

	public ResultSetMetaData getMetaData() {
		return metaData;
	}

	public void setMetaData(ResultSetMetaData metaData) {
		this.metaData = metaData;
	}

	public String getOutfile() {
		return outfile;
	}

	public void setOutfile(String outfile) {
		this.outfile = outfile;
	}
}
