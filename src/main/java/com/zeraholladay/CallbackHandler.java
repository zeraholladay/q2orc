package com.zeraholladay;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

public class CallbackHandler implements ResultSetExtractor {
	private OrcWriter orcWriter;

	@Override
	public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		orcWriter.setMetaData(metaData);

		try {
			orcWriter.build();
		} catch (IllegalArgumentException | IOException e) {
			throw new SQLException("IllegalArgumentException or IOException!!!");
		}

		while (resultSet.next()) {
			for (int columnIndex = 0; columnIndex < metaData.getColumnCount(); columnIndex++) {
				try {
					orcWriter.split(resultSet, columnIndex);
				} catch (IOException e) {
					throw new SQLException("IOException!!!");
				}
			}
		}
		try {
			orcWriter.close();
		} catch (IOException e) {
			throw new SQLException("Close failed!!!");
		}
		return null;
	}

	public OrcWriter getOrcWriter() {
		return orcWriter;
	}

	public void setOrcWriter(OrcWriter orcWriter) {
		this.orcWriter = orcWriter;
	}

}
