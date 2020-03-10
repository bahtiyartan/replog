package com.ias.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Hashtable;

import com.ias.util.SimpleFileHandler;

public class festival {

	public static void main(String[] args) {

		//STEP 1: read configuration file
		String strConfig = SimpleFileHandler.readString("config.ias", "UTF-8");
		String[] strRows = strConfig.split("\n");
		Hashtable<String, String> config = new Hashtable<String, String>();
		for (int i = 0; i < strRows.length; i++) {
			strRows[i] = strRows[i].trim();

			if (strRows[i].length() > 0) {

				int nIndex = strRows[i].indexOf("=");

				if (nIndex > 0) {
					String strKey = strRows[i].substring(0, nIndex).trim();
					String strValue = strRows[i].substring(nIndex + 1).trim();
					config.put(strKey, strValue);
				}
			}
		}

		try {
			Class.forName("org.postgresql.Driver");

			//STEP 2: database connections
			Connection source = DriverManager.getConnection(config.get("source"), config.get("source.user"), config.get("source.password"));
			Connection target = DriverManager.getConnection(config.get("target"), config.get("target.user"), config.get("target.password"));

			//STEP 3: read replication logs
			String startDate = config.get("startdate").trim();
			String endDate = config.get("enddate").trim();

			String strQuery = "SELECT SQLCMD, SAVEDATA, OPERATION, CREATEDAT FROM SYSREPLICATIONLOG WHERE CREATEDAT >= '" + startDate + "' AND CREATEDAT < '" + endDate
					+ "' ORDER BY CREATEDAT, LOGTIME, OPERATION DESC";
			System.out.println("SQL Query:" + strQuery);
			System.out.println();
			Statement stmt = source.createStatement();
			ResultSet rs = stmt.executeQuery(strQuery);

			String[] aExcludeQuery = new String[0];
			try {
				config.get("excludequery").replace(" ", "").split(";");
			} catch (Exception e) {

			}

			String[] aExcludeData = config.get("exclude").split(";");

			String strSaveData = "";
			int nOperation = 0;

			//STEP 4: transfer rows
			int nSuccesful = 0;
			int nItemCount = 0;
			while (rs.next()) {

				strQuery = rs.getString(1);
				strSaveData = rs.getString(2);
				nOperation = rs.getInt(3);

				nItemCount++;
				System.out.print(nItemCount + ": ");

				//check query
				if (strQuery == null || strQuery.trim().isEmpty()) {
					System.out.print("no query  ");
					continue;
				}

				//check savedata
				if (nOperation == 0 && isDataExcluded(strSaveData, aExcludeData)) {
					System.out.print("eleminated (data)");
					continue;
				}

				if (nOperation > 0 && isQueryExcluded(strQuery, aExcludeQuery)) {
					System.out.print("eleminated (query)");
					continue;
				}

				try {
					Statement statement = target.createStatement();
					statement.execute(strQuery);
					statement.close();
					nSuccesful++;
					System.out.print("successful");
				} catch (Exception e) {
					System.out.print("failed     ->" + e.getMessage());
				}

				System.out.println(" (" + nSuccesful + "/" + nItemCount + ")");
			}
			source.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static boolean isDataExcluded(String strSaveData, String[] pExcludeData) {

		if (strSaveData != null && strSaveData.length() > 0) {

			for (int i = 0; i < pExcludeData.length; i++) {
				if (strSaveData.indexOf(pExcludeData[i]) >= 0) {
					return true;
				}
			}
		}

		return false;
	}

	public static boolean isQueryExcluded(String strQuery, String[] pExcludedQuery) {

		int nWhereIndex = strQuery.indexOf("WHERE");
		if (nWhereIndex > 0) {
			strQuery = strQuery.substring(0, nWhereIndex);
			strQuery = strQuery.replace(" ", "");
		}

		for (int i = 0; i < pExcludedQuery.length; i++) {
			if (strQuery.indexOf(pExcludedQuery[i]) >= 0) {
				return true;
			}
		}

		return false;
	}

}
