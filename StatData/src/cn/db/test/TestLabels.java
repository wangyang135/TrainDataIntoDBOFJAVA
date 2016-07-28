package cn.db.test;

import cn.db.utils.DBUtils;

public class TestLabels {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("===BEGIN===");
		DBUtils.insertTrainLabels("test_nolabels.txt");
		System.out.println("===END===");
	}
}
