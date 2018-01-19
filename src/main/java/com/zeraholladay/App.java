package com.zeraholladay;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class App 
{
	String name;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"spring.xml");

		String url = null;
		String username = null;
		String password = null;

		BasicDataSource oracleDataSourceTemplate = (BasicDataSource) context.getBean("oracleDataSourceTemplate");
		oracleDataSourceTemplate.setUrl(url);
		oracleDataSourceTemplate.setUsername(username);
		oracleDataSourceTemplate.setPassword(password);

		App app = (App) context.getBean("app");
		System.out.println(app.getName());
	}
}
