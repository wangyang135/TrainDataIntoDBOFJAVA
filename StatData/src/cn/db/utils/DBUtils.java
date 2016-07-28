package cn.db.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.mysql.jdbc.Blob;

public class DBUtils {

	private static String url;
	private static String userName;
	private static String password;
	private static String driverClassName;
	
	private static Connection connect;
	
	static {
		Properties property = new Properties();
		InputStream inputStream = ClassLoader.getSystemResourceAsStream("jdbc.properties");
		try {
			property.load(inputStream);
		} catch (Exception e) {
			System.out.println("加载文件异常！");
		}
		
		url = property.getProperty("url");
		userName = property.getProperty("userName");
		password = property.getProperty("password");
		driverClassName = property.getProperty("driverClassName");
	}
	
	/**
	 * 测试
	 */
	public static void insertInfo() {
		
		try {
			Class.forName(driverClassName);
			connect = DriverManager.getConnection(url, userName, password);
			
			String sql = "insert into train_info (uid, screen_name, avatar_large) values (?, ?, ?)";
			PreparedStatement statement = connect.prepareStatement(sql);
			statement.setLong(1, 12312331);
			statement.setString(2, "test");
			statement.setString(3, "https://tower.im/projects/0f0289b5ffed44cb8e89ef9988b0c176/");
			statement.execute();
			connect.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 读取数据文件，将数据按照行读取，写入数据库，同时需要将重复项去除
	 * 重复项
	 * 2197914831||淡蓝琢紫-zhangwei||http://tp4.sinaimg.cn/2197914831/180/5607185908/1
	 * 2197881945||活泼电饭锅2||http://tp2.sinaimg.cn/2197881945/180/5603940227/1
	 * .......
	 */
	public static void insertInfoBatch(String fileName) {
		
		URL info_url = ClassLoader.getSystemResource(fileName);
		File file = new File(info_url.getFile());
		
		BufferedReader reader = null;
		try {
			Class.forName(driverClassName);
			connect = DriverManager.getConnection(url, userName, password);
			String sql = "insert into train_info (uid, screen_name, avatar_large) values (?, ?, ?)";
			PreparedStatement statement = connect.prepareStatement(sql);
			
			List<String> keyList = new ArrayList<String>();
			reader = new BufferedReader(new FileReader(file));
			String info = "";
			int allCount = 0;
			int count = 0;
			while((info = reader.readLine()) != null) {
				//System.out.println(info);
				String[] infos = info.split("\\|\\|");
				//System.out.println(infos[0] + ">>" + infos[1] + ">>" + infos[2]);
				if(!keyList.contains(infos[0])) {
					keyList.add(infos[0]);
					
					statement.setLong(1, Long.parseLong(infos[0]));
					statement.setString(2, infos[1].equals("None") ? null : infos[1]);
					statement.setString(3, infos[2].equals("None") ? null : infos[2]);
					
					statement.addBatch();
					allCount ++;
					count ++;
				}
				else {
					System.out.println("存在重复项：" + info);
				}
				if(count == 1000) {
					System.out.println("===1000===");
					statement.executeBatch();
					count = 0;
				}
				//break;
			}
			if(count != 0) {
				statement.executeBatch();
			}
			System.out.println("所有的数据量：" + allCount);
			
			if(connect != null)
				connect.close();
			if(connect != null)
				statement.close();
 		} catch (Exception e) {
			System.out.println("====DBUtils====");
			System.out.println("出现异常！！");
			e.printStackTrace();
		}
		finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	/**
	 * 读取数据文件train_labels.txt，将数据按照行读取，写入数据库
	 */
	public static void insertTrainLabels(String fileName) {
		URL info_url = ClassLoader.getSystemResource(fileName);
		File file = new File(info_url.getFile());
		
		BufferedReader reader = null;
		try {
			Class.forName(driverClassName);
			connect = DriverManager.getConnection(url, userName, password);
			String sql = "insert into train_labels (uid, gender, birthday, location_province, location_city) values (?, ?, ?, ?, ?)";
			PreparedStatement statement = connect.prepareStatement(sql);
			
			List<String> keyList = new ArrayList<String>();
			reader = new BufferedReader(new FileReader(file));
			String info = "";
			int allCount = 0;
			int count = 0;
			while((info = reader.readLine()) != null) {
				//System.out.println(info);
				String[] infos = info.split("\\|\\|");
				//System.out.println(infos[0] + ">>" + infos[1] + ">>" + infos[2]);
				if(!keyList.contains(infos[0])) {
					keyList.add(infos[0]);
					
					statement.setLong(1, Long.parseLong(infos[0]));
					statement.setString(2, infos[1].equals("None") ? null : infos[1]);
					statement.setInt(3, infos[2].equals("None") ? null : Integer.parseInt(infos[2]));
					
					if("None".equals(infos[3])) {
						statement.setString(4, null);
						statement.setString(5, null);
					} 
					else {
						String[] locs = infos[3].split(" ");
						if(locs.length == 1) {
							statement.setString(4, locs[0]);
							statement.setString(5, null);
						}
						else if(locs.length == 2) {
							statement.setString(4, locs[0]);
							statement.setString(5, locs[1]);
						}
					}
					
					statement.addBatch();
					allCount ++;
					count ++;
				}
				else {
					System.out.println("存在重复项：" + info);
				}
				if(count == 1000) {
					System.out.println("===1000===");
					statement.executeBatch();
					count = 0;
				}
				//break;
			}
			if(count != 0) {
				statement.executeBatch();
			}
			System.out.println("所有的数据量：" + allCount);
			
			if(connect != null)
				connect.close();
			if(connect != null)
				statement.close();
 		} catch (Exception e) {
			System.out.println("====DBUtils====");
			System.out.println("出现异常！！");
			e.printStackTrace();
		}
		finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	/**
	 * 读取数据文件train_links.txt，将数据按照行读取，写入数据库
	 */
	public static void insertTrainLinks(String fileName) {
		URL info_url = ClassLoader.getSystemResource(fileName);
		File file = new File(info_url.getFile());
		
		BufferedReader reader = null;
		try {
			Class.forName(driverClassName);
			connect = DriverManager.getConnection(url, userName, password);
			String sql = "insert into train_links (uid, uid_follower) values (?, ?)";
			PreparedStatement statement = connect.prepareStatement(sql);
			
			//List<String> keyList = new ArrayList<String>();
			reader = new BufferedReader(new FileReader(file));
			String info = "";
			int allCount = 0;
			int count = 0;
			while((info = reader.readLine()) != null) {
				//System.out.println(info);
				String[] infos = info.split(" ");
				//System.out.println(infos[0] + ">>" + infos[1] + ">>" + infos[2]);
				for(int i = 1; i < infos.length; i++) {
					statement.setLong(1, Long.parseLong(infos[0]));
					statement.setLong(2, Long.parseLong(infos[i]));
					statement.addBatch();
					
					allCount ++;
					count ++;
				}
				
				/*
				if(!keyList.contains(infos[0])) {
					keyList.add(infos[0]);
					
				}
				else {
					System.out.println("存在重复项：" + info);
				}
				*/
				
				if(count > 10000) {
					System.out.println("===10000===");
					statement.executeBatch();
					count = 0;
				}
				
			}
			if(count != 0) {
				statement.executeBatch();
			}
			System.out.println("所有的数据量：" + allCount);
			
			if(connect != null)
				connect.close();
			if(connect != null)
				statement.close();
		} catch (Exception e) {
			System.out.println("====DBUtils====");
			System.out.println("出现异常！！");
			e.printStackTrace();
		}
		finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	/**
	 * 读取数据文件train_status.txt，将数据按照行读取，写入数据库
	 */
	public static void insertTrainStatus(String fileName) {
		URL info_url = ClassLoader.getSystemResource(fileName);
		File file = new File(info_url.getFile());
		
		BufferedReader reader = null;
		try {
			Class.forName(driverClassName);
			connect = DriverManager.getConnection(url, userName, password);
			String sql = "insert into train_status (uid, retweet_count, review_count, source, time_copy, content) values (?, ?, ?, ?, ?, ?)";
			PreparedStatement statement = connect.prepareStatement(sql);
			
			reader = new BufferedReader(new FileReader(file));
			String info = "";
			int allCount = 0;
			int count = 0;
			while((info = reader.readLine()) != null) {
				//System.out.println(info);
				try {
					String[] infos = info.split(",");
					int length = infos[0].length() + infos[1].length() + infos[2].length() + infos[3].length() + infos[4].length() + 5;
					String content = info.substring(length, info.length() - 1);
					//System.out.println(content);
					statement.setLong(1, Long.parseLong(infos[0]));
					statement.setLong(2, Long.parseLong(infos[1]));
					statement.setLong(3, Long.parseLong(infos[2]));
					statement.setString(4, infos[3]);
					statement.setString(5, infos[4]);
					InputStream inputStream = new ByteArrayInputStream(content.getBytes());
					statement.setBlob(6, inputStream);
					statement.addBatch();
					
					allCount ++;
					count ++;
					if(count > 10000) {
						System.out.println("===10000===");
						statement.executeBatch();
						count = 0;
					}
					
				} catch (Exception e) {
					System.out.println("异常数据：" + info);
				}
				
			}
			if(count != 0) {
				statement.executeBatch();
			}
			System.out.println("所有的数据量：" + allCount);
			
			if(connect != null)
				connect.close();
			if(statement != null)
				statement.close();
		} catch (Exception e) {
			System.out.println("====DBUtils====");
			System.out.println("出现异常！！");
			e.printStackTrace();
		}
		finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}
}
