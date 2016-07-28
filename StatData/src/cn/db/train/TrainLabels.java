package cn.db.train;

import cn.db.utils.DBUtils;

public class TrainLabels {

	/**
	 * ===BEGIN===
	   ===1000===
	   ===1000===
	   ===1000===
	         所有的数据量：3200
	   ===END===

	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("===BEGIN===");
		DBUtils.insertTrainLabels("train_labels.txt");
		System.out.println("===END===");
	}
}
