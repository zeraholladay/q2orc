package com.zeraholladay;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class OracleDataSourceProcessor {
	public NamedParameterJdbcTemplate getOracleDataSourceTemplate() {
		return oracleDataSourceTemplate;
	}

	public void setOracleDataSourceTemplate(
			NamedParameterJdbcTemplate oracleDataSourceTemplate) {
		this.oracleDataSourceTemplate = oracleDataSourceTemplate;
	}

	NamedParameterJdbcTemplate oracleDataSourceTemplate;
}
