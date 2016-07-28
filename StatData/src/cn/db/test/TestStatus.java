package cn.db.test;

import cn.db.utils.DBUtils;

public class TestStatus {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("===BEGIN===");
		DBUtils.insertTrainStatus("test_status.txt");
		System.out.println("===END===");
	}
	/**
	 *
	 */
	
}
