package cn.db.test;

import cn.db.utils.DBUtils;

public class TestLinks {

	public static void main(String[] args) {
		System.out.println("===BEGIN===");
		DBUtils.insertTrainLinks("test_links.txt");
		System.out.println("===END===");
	}
	
	/**
	 * 
	 */
}
