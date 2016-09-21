package com.welab.BDL.UserInterests;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

public class readfile {

	public static void main(String[] args) throws Exception {
		String filePath = "D:\\Work\\wolaidai\\用户兴趣模型\\traindata\\体育休闲\\C39-Sports0673.txt";
		String content = readTxt(filePath);
		System.out.println(content);

	}

	/**
	 * 解析普通文本文件 流式文件 如txt
	 * 
	 * @param path
	 * @return
	 */
	@SuppressWarnings("unused")
	public static String readTxt(String path) {
		StringBuilder content = new StringBuilder("");
		try {
			String code = resolveCode(path);
			File file = new File(path);
			InputStream is = new FileInputStream(file);
			InputStreamReader isr = new InputStreamReader(is, code);
			BufferedReader br = new BufferedReader(isr);
			// char[] buf = new char[1024];
			// int i = br.read(buf);
			// String s= new String(buf);
			// System.out.println(s);
			String str = "";
			while (null != (str = br.readLine())) {
				content.append(str);
				System.out.println(str);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("读取文件:" + path + "失败!");
		}
		return content.toString();
	}

	/*
	 * public String GetFileEncodeType(String filename) { System.IO.FileStream
	 * fs = new System.IO.FileStream(filename, System.IO.FileMode.Open,
	 * System.IO.FileAccess.Read); System.IO.BinaryReader br = new
	 * System.IO.BinaryReader(fs); Byte[] buffer = br.ReadBytes(2); if
	 * (buffer[0] >= 0xEF) { if (buffer[0] == 0xEF && buffer[1] == 0xBB) {
	 * return "UTF-8"; } else if (buffer[0] == 0xFE && buffer[1] == 0xFF) {
	 * return "BigEndianUnicode"; } else if (buffer[0] == 0xFF && buffer[1] ==
	 * 0xFE) { return "Unicode"; } else { return "Default"; } } else { return
	 * System.Text.Encoding.Default; } }
	 */
	/**
	 * 判断文件的编码格式
	 * 
	 * @param fileName
	 *            :file
	 * @return 文件编码格式
	 * @throws Exception
	 */
	public static String codeString(String fileName)
			throws Exception {

		File file = new File(fileName);
		if (file == null || !file.exists()) {
			System.out.println("文件不存在..." + file.getAbsolutePath());
			return null;
		}

		BufferedInputStream bin = new BufferedInputStream(new FileInputStream(
				file));
		int p = (bin.read() << 8) + bin.read();
		String code = null;
		// 其中的 0xefbb、0xfffe、0xfeff、0x5c75这些都是这个文件的前面两个字节的16进制数
		switch (p) {
		case 0xefbb:
			code = "UTF-8";
			break;
		case 0xfffe:
			code = "Unicode";
			break;
		case 0xfeff:
			code = "UTF-16BE";
			break;
		case 0x5c75:
			code = "ANSI|ASCII";
			break;
		default:
			code = "GBK";
		}
		bin.close();
		return code;
	}

	public static String resolveCode(String path) throws Exception {
		// String filePath = "D:/article.txt"; //[-76, -85, -71] ANSI
		// String filePath = "D:/article111.txt"; //[-2, -1, 79] unicode big
		// endian
		// String filePath = "D:/article222.txt"; //[-1, -2, 32] unicode
		// String filePath = "D:/article333.txt"; //[-17, -69, -65] UTF-8
		InputStream inputStream = new FileInputStream(path);
		byte[] head = new byte[3];
		inputStream.read(head);
		String code = "GBK"; // 或GBK
		System.out.println(head[0] + ": " + head[1]);
		if (head[0] == -1 && head[1] == -2)
			code = "UTF-16";
		else if (head[0] == -2 && head[1] == -1)
			code = "Unicode";
		else if (head[0] == -17 && head[1] == -69 && head[2] == -65)
			code = "UTF-8";

		inputStream.close();

		System.out.println(code);
		return code;
	}

}
