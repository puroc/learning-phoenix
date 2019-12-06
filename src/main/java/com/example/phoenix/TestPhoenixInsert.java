package com.example.phoenix;

import org.apache.phoenix.jdbc.PhoenixConnection;

import java.sql.*;
import java.util.Date;
import java.util.Random;

public class TestPhoenixInsert {

    public static void main(String[] args) {
        PhoenixConnection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:cdh0");
            conn.setAutoCommit(false);

            int upsertBatchSize = conn.getMutateBatchSize();
            String upsertStatement = "upsert into PUD_TEST_WATER_HISTORY values(?,?,?,?,?)";
            stmt = conn.prepareStatement(upsertStatement);
            int rowCount = 0;
            for (int i = 0; i < 1; i++) {
                Random r = new Random();
                stmt.setString(1, "device0-1528452009555-1069");
                stmt.setString(2, "device0");
                stmt.setInt(3, 1);
                stmt.setInt(4, 1);
                stmt.setTimestamp(5, new Timestamp(new Date().getTime()));
                stmt.execute();
                // Commit when batch size is reached
                if (++rowCount % upsertBatchSize == 0) {
                    conn.commit();
                    System.out.println("Rows upserted: " + rowCount);
                }
            }
            conn.commit();
        } catch (Throwable e) {
            e.printStackTrace();
        }  finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}