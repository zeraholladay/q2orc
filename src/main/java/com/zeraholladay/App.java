package com.zeraholladay;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class App {
	//XXX: Turn to beans!!!
	private DataSourceProcessor dataSourceProcessor = new DataSourceProcessor();
	private CallbackHandler callbackHandler = new CallbackHandler();
	private OrcWriter orcWriter = new OrcWriter();
	
	private Map<String, Object> namedParams = new HashMap<>(); //Not yet implemented
	private String query;

	void config(String[] args) {
		Options options = new Options();

		Option url = new Option("u", "url", true, "JBDC URL");
		url.setRequired(true);
		options.addOption(url);
		
		Option username = new Option("U", "username", true, "Username");
		username.setRequired(true);
		options.addOption(username);
		
		Option password = new Option("p", "password", true, "Password");
		password.setRequired(true);
		options.addOption(password);
		
		Option query = new Option("q", "query", true, "Query");
		query.setRequired(true);
		options.addOption(query);
		
		Option outfile = new Option("o", "outfile", true, "Outfile");
		outfile.setRequired(true);
		options.addOption(outfile);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("utility-name", options);
			System.exit(1);
			return;
		}
		
		setQuery(cmd.getOptionValue("query"));
		
		dataSourceProcessor.setUrl(cmd.getOptionValue("url"));
		dataSourceProcessor.setUsername(cmd.getOptionValue("username"));
		dataSourceProcessor.setPassword(cmd.getOptionValue("password"));
		
		orcWriter.setOutfile(cmd.getOptionValue("outfile"));
		callbackHandler.setOrcWriter(orcWriter);
	}

	void run(ApplicationContext context) {
		dataSourceProcessor.createSourceTemplate(context).query(getQuery(), namedParams, callbackHandler);
	}

	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("spring.xml");

		App app = (App) context.getBean("app");
		app.config(args);
		app.run(context);
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
}
