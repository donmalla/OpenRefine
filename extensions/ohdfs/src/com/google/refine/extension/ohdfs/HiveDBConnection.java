package com.google.refine.extension.ohdfs;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class HiveDBConnection {
    
    static {
        try 
        {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            File workaround = new File(".");
            System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
            new File("./bin").mkdirs();
            File f = new File("./bin/winutils.exe");
            if (!f.exists())
            {
                f.createNewFile();
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    public HiveDBConnection() {
        
    }
    
    public static Connection getConnection() throws SQLException {
        String ipAddr = System.getProperty("HIVE_SERVER_IP");
        if (ipAddr==null) {
            throw new SQLException("HIVE_SERVER_IP is null");
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://"+ipAddr+":10000/default", "cloudera",
                "cloudera");
        return con;
    }

    public void close() {
        
    }
    
    
    public static void run2(final String[] args) throws Exception
    {
            
    }
    
    public static void main(String[] args) throws Exception {
        
            run2(null);
        // TODO Auto-generated method stub
            /*
            HiveDBConnection hdb = new HiveDBConnection();
            Connection con = hdb.getConnection();
            Statement stmt = con.createStatement();
            String tableName = "testHiveDriverTable";
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName + " (key int, value string)");
            // show tables
            String sql = "show tables '" + tableName + "'";
            System.out.println("Running: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
              System.out.println(res.getString(1));
            }
               // describe table
            sql = "describe " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
              System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            */
         
    }

}
