package cn.db.test;

import cn.db.utils.DBUtils;

public class TestInfoDB {
	
	/**
	 * 运行结果
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("===BEGIN===");
		DBUtils.insertInfoBatch("test_info.txt");
		System.out.println("===END===");
	}
}
