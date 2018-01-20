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
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class OrcWriter {
	private String outfile;
	
	private ResultSetMetaData metaData;
	private VectorizedRowBatch batch;
	private Writer writer;

	private Configuration conf = new Configuration();
	private TypeDescription schema = TypeDescription.createStruct();

	public void build() throws SQLException, IllegalArgumentException, IOException {
		for (int columnIndex = 0; columnIndex < metaData.getColumnCount(); columnIndex++) {
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

			batch = schema.createRowBatch();
		}
		writer = OrcFile.createWriter(new Path(getOutfile()), OrcFile.writerOptions(conf).setSchema(schema));
	}

	public void split(ResultSet resultSet, int columnIndex) throws SQLException, IOException {
		switch (metaData.getColumnType(columnIndex)) {
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.LONGVARCHAR:
			write(resultSet.getString(columnIndex).getBytes(), columnIndex);
			break;
		case java.sql.Types.NUMERIC:
		case java.sql.Types.DECIMAL:
			write(resultSet.getString(columnIndex).getBytes(), columnIndex);
			break;
		case java.sql.Types.BIT:
			write(resultSet.getBoolean(columnIndex), columnIndex);
			break;
		case java.sql.Types.TINYINT:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.INTEGER:
		case java.sql.Types.BIGINT:
			write(resultSet.getLong(columnIndex), columnIndex);
			break;
		case java.sql.Types.REAL:
		case java.sql.Types.FLOAT:
		case java.sql.Types.DOUBLE:
			write(resultSet.getDouble(columnIndex), columnIndex);
			break;
		case java.sql.Types.BINARY:
		case java.sql.Types.VARBINARY:
		case java.sql.Types.LONGVARBINARY:
			write(resultSet.getBytes(columnIndex), columnIndex);
			break;
		case java.sql.Types.DATE:
			write(resultSet.getDate(columnIndex), columnIndex);
			break;
		case java.sql.Types.TIME:
			write(resultSet.getTime(columnIndex), columnIndex);
			break;
		case java.sql.Types.TIMESTAMP:
			write(resultSet.getTimestamp(columnIndex), columnIndex);
			break;
		default:
			throw new SQLException("Unmapped type!");
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

	private void write(boolean cell, int columnIndex) {
		LongColumnVector vector = (LongColumnVector) batch.cols[columnIndex];
		vector.vector[batch.size] = cell ? 1 : 0;
	}
	
	private void write(long cell, int columnIndex) {
		LongColumnVector vector = (LongColumnVector) batch.cols[columnIndex];
		vector.vector[batch.size] =  cell;
	}
	
	private void write(double cell, int columnIndex) {
		DoubleColumnVector vector = (DoubleColumnVector) batch.cols[columnIndex];
		vector.vector[batch.size] =  cell;
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
