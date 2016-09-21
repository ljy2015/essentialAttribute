package com.welab.BDL.UserInterests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class ParseXML {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SAXReader reader = new SAXReader();
		String path="D:\\Work\\wolaidai\\中文语料库\\20160919\\news_tensite_xml.full\\news_tensite_xml.txt";

		Document document;

		try {
			String code = readfile.resolveCode(path);
			File file = new File(path);
			InputStream is = new FileInputStream(file);
			InputStreamReader isr = new InputStreamReader(is, code);
			BufferedReader br = new BufferedReader(isr);
/*			while (null != (str = br.readLine())) {
			
				System.out.println(br.readLine());*/
			
			
			document = reader.read(br);
			Element root = document.getRootElement();
			List<Element> childElements = root.elements();
			for (Element child : childElements) {
				// 未知属性名情况下
				  List<Attribute> attributeList = child.attributes(); for
				 (Attribute attr : attributeList) {
				 System.out.println(attr.getName() + ": " + attr.getValue());
				 }
				 

				/*// 已知属性名情况下
				System.out.println("id: " + child.attributeValue("doc"));

				// 未知子元素名情况下
				
				  List<Element> elementList = child.elements(); for (Element
				  ele : elementList) { System.out.println(ele.getName() + ": "
				  + ele.getText()); } System.out.println();
				 

				// 已知子元素名的情况下
				System.out.println("title" + child.elementText("url"));
				System.out.println("author" + child.elementText("content"));
				// 这行是为了格式化美观而存在
				System.out.println();*/

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
