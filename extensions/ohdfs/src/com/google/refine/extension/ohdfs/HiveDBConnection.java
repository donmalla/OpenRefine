package com.google.refine.extension.ohdfs;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.cli.thrift.TSessionHandle;


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
            // With this we are able to access the private sessionHandleField of the HiveConnection
            final Field sessionHandleField = HiveConnection.class.getDeclaredField("sessHandle");
            sessionHandleField.setAccessible(true);

            // Prepare the Statement
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            final Connection connection = getConnection();
            final Statement statement = connection.createStatement();

            // Read the SessionId Hive will use for its MR Jobs
            final TSessionHandle fieldValue = (TSessionHandle) sessionHandleField.get(connection);
            

            final StringBuilder sessionId = new StringBuilder();
            org.apache.thrift.TBaseHelper.toString(fieldValue.getSessionId().bufferForGuid(), sessionId);

            System.out.println("Gotcha:" + sessionId);

            // Now fire a query that spawns a MR Jobs
            new Thread() {
                    @Override
                    public void run()
                    {
                            try
                            {
                                    statement.executeQuery("SELECT * FROM ca_north_discharges_2007_stg2 WHERE 1=1 LIMIT 1");
                            }
                            catch (final SQLException e)
                            {
                                    e.printStackTrace();
                            }
                            System.out.println("select done");
                    };
            }.start();

            String ipAddr = System.getProperty("HIVE_SERVER_IP");
            if (ipAddr==null) {
                throw new SQLException("HIVE_SERVER_IP is null");
            }
            
            // Connect to the JobTracker to read all Jobs with our ID
            final Configuration conf = new Configuration();
            conf.set("mapred.job.tracker", ipAddr+":19888");
            final JobClient jobClient = new JobClient(conf);
  

            // Search the job with the "hive.session.id" set to "sessionId"
            for (final JobStatus jobStatus : jobClient.getAllJobs())
            {
                    final RunningJob job = jobClient.getJob(jobStatus.getJobID());

                    if (job == null)
                    {
                            continue;
                    }

                    // How to get this "Raw configuration" here?
            }
            System.out.println("list all Jobs Done");

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
