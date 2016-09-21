package com.welab.BDL.droste;

import java.sql.Timestamp;

public class Mytime {
	public static Timestamp timestampToString(Integer time) {
		// int转long时，先进行转型再进行计算，否则会是计算结束后在转型
		long temp = (long) time * 1000;
		Timestamp ts = new Timestamp(temp);

/*		String tsStr = "";
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			// 方法一
			tsStr = dateFormat.format(ts);
			System.out.println(tsStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tsStr;*/
		return ts;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
