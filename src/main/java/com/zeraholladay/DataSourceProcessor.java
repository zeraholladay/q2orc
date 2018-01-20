package com.zeraholladay;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class DataSourceProcessor {
	private String url = null;
	private String username = null;
	private String password = null;
	
	public NamedParameterJdbcTemplate createSourceTemplate(ApplicationContext context) {
		BasicDataSource dataSource = (BasicDataSource) context.getBean("oracleDataSource");
		dataSource.setUrl(getUrl());
		dataSource.setUsername(getUsername());
		dataSource.setPassword(getPassword());

		NamedParameterJdbcTemplate dataSourceTemplate = new NamedParameterJdbcTemplate(dataSource);

		return dataSourceTemplate;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
