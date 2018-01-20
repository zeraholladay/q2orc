package com.zeraholladay;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

public class CallbackHandler implements ResultSetExtractor {
	@Override
	public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
		ResultSetMetaData metaData = resultSet.getMetaData();

		OrcWriter orcWriter = new OrcWriter(metaData);
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
}
