package uk.co.vianconsulting.hive;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class HiveTestCase {
	
	private static HiveConnectionBean hiveBean;

	private static Properties properties;
	
	private static final String PREFIX="ccrsmoke";
	
	private Logger logger = Logger.getLogger(getClass());

	@Before
	public void init() throws FileNotFoundException, IOException
	{
		hiveBean=new HiveConnectionBean();
		properties=new Properties();
		properties.load(new FileInputStream(new File("src/main/resources/android_logging.txt")));
		
	}
	
	@Test
	public void getCountsForAndroid()
	{
		int runNumberQuery =getResults("android_all_counts");
		
		assertEquals(100,runNumberQuery);
	}
	@Test
	public void getCountsForAndroidAt()
	{
		int runNumberQuery =getResults("android_counts_at");
		assertEquals(100,runNumberQuery);
	}
	@Test
	public void getCountsForAndroidDe()
	{
		int runNumberQuery =getResults("android_counts_de");
		
		assertEquals(100,runNumberQuery);
	}
	@Test
	public void getCountsForAndroidEn()
	{
		int runNumberQuery =getResults("android_counts_en");
		
		assertEquals(100,runNumberQuery);
	}
	
	@Test
	public void getCountsForAndroidGr()
	{
		int runNumberQuery =getResults("android_counts_gr");
		
		assertEquals(100,runNumberQuery);
	}
	
	@Test
	public void getCountsForAndroidNl()
	{
		int runNumberQuery =getResults("android_counts_nl");
		
		assertEquals(100,runNumberQuery);
	}
	
	@Test
	public void getCountsForAndroidUk()
	{
		
		int runNumberQuery =getResults("android_counts_uk");
		assertEquals(100,runNumberQuery);
	}
	
	private int getResults(String query)
	{
		String queryWithPrefix = getQuery(query);
		return  hiveBean.runNumberQuery(queryWithPrefix);
	}
	private String getQuery(String queryName)
	{
		logger.info("Looking for "+queryName);
		String property = properties.getProperty(queryName);
		if(property==null)
		{
			throw new RuntimeException("Cannot locate property "+queryName);
		}
		String replaceAll = property.replaceAll("prefix", PREFIX);
		logger.info("Running query "+replaceAll);
		return replaceAll;
	}

}
