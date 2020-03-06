package com.ias.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;

public class SimpleFileHandler {

	public static byte[] readByte(String p_strPath) {
		byte[] arrReturn = new byte[0];
		try {
			byte[] arrBuffer = new byte[1024];
			File jFile = new File(p_strPath);

			if (!jFile.exists()) {
				return arrReturn;
			}

			FileInputStream fileInput = new FileInputStream(jFile);
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

			int nLength = 0;
			while (nLength != -1) {
				byteOut.write(arrBuffer, 0, nLength);
				nLength = fileInput.read(arrBuffer);
			}

			byteOut.close();
			fileInput.close();

			arrReturn = byteOut.toByteArray();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return arrReturn;
	}

	public static String readString(String p_strPath, String p_strEncoding) {
		byte[] bytData = readByte(p_strPath);
		try {
			return new String(bytData, p_strEncoding);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return "";
	}
}
