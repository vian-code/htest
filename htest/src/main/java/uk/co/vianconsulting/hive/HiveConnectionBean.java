package uk.co.vianconsulting.hive;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

public class HiveConnectionBean {

	private static BeanFactory factory;
	private JdbcTemplate template;

	public HiveConnectionBean() {
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "hive-context.xml" });
		factory = (BeanFactory) appContext;
	}

	public int runNumberQuery(String query) {
		if (template == null) {
			template = getTemplate();
		}
		return template.queryForInt(query);
	}

	private static JdbcTemplate getTemplate() {
		org.springframework.jdbc.core.JdbcTemplate bean = (JdbcTemplate) factory
				.getBean("template");
		return bean;
	}

}
