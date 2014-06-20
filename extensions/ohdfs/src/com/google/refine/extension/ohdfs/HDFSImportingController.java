package com.google.refine.extension.ohdfs;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

import com.google.refine.ProjectManager;
import com.google.refine.ProjectMetadata;
import com.google.refine.RefineServlet;
import com.google.refine.commands.HttpUtilities;
import com.google.refine.importing.DefaultImportingController;
import com.google.refine.importing.ImportingController;
import com.google.refine.importing.ImportingJob;
import com.google.refine.importing.ImportingManager;
import com.google.refine.model.Project;
import com.google.refine.util.JSONUtilities;
import com.google.refine.util.ParsingUtilities;



public class HDFSImportingController implements ImportingController {

    protected RefineServlet servlet;
    
    @Override
    public void init(RefineServlet servlet) {
        this.servlet = servlet;
        System.out.println("****** Adding HDFS Importing Controller 3");
    }
    
    
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        Properties parameters = ParsingUtilities.parseUrlParameters(request);
        String subCommand = parameters.getProperty("subCommand");
        if ("list-documents".equals(subCommand)) {
           doListDocuments(request, response, parameters);
        } else if ("initialize-parser-ui".equals(subCommand)) {
            doInitializeParserUI(request, response, parameters);
        } else if ("parse-preview".equals(subCommand)) {
           doParsePreview(request, response, parameters);
        } else if ("create-project".equals(subCommand)) {
           doCreateProject(request, response, parameters);
        } else if ("real-row-cnt".equals(subCommand)) {
             doGetRealCount(request, response, parameters);
        } else if ("submit-sampling-job".equals(subCommand)) {
            doSubmitSamplingJob(request, response, parameters);
        } else {
            HttpUtilities.respond(response, "error", "No such sub command");
        }
    }
    
    
    private void doSubmitSamplingJob(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {
        Writer w = response.getWriter();
        JSONWriter writer = new JSONWriter(w);
        try
        {
            HiveDBConnection hdb = new HiveDBConnection();
            Connection con = hdb.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from CA_NORTH_DISCHARGES_2007_STG");
            
            writer.object();
            while(rs.next())
            {
                writer.key("count"); writer.value(rs.getLong(1));
            }
            writer.endObject();
        } catch (Exception e) {
            throw new ServletException(e);
        } finally {
            w.flush();
            w.close();
        }
        
    }
    
    
    private void doGetRealCount(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {
        Writer w = response.getWriter();
        JSONWriter writer = new JSONWriter(w);
        try
        {
            HiveDBConnection hdb = new HiveDBConnection();
            Connection con = hdb.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from CA_NORTH_DISCHARGES_2007_STG");
            
            writer.object();
            while(rs.next())
            {
                writer.key("count"); writer.value(rs.getLong(1));
            }
            writer.endObject();
        } catch (Exception e) {
            throw new ServletException(e);
        } finally {
            w.flush();
            w.close();
        }
        
    } 
    
    private void doCreateProject(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {
        
            long jobID = Long.parseLong(parameters.getProperty("jobID"));
            final ImportingJob job = ImportingManager.getJob(jobID);
            if (job == null) {
                HttpUtilities.respond(response, "error", "No such import job");
                return;
            }
            
            job.updating = true;
            try {
                final JSONObject optionObj = ParsingUtilities.evaluateJsonStringToObject(
                    request.getParameter("options"));
                
                final List<Exception> exceptions = new LinkedList<Exception>();
                
                job.setState("creating-project");
                
                final Project project = new Project();
                new Thread() {
                    @Override
                    public void run() {
                        ProjectMetadata pm = new ProjectMetadata();
                        pm.setName(JSONUtilities.getString(optionObj, "projectName", "Untitled"));
                        pm.setEncoding(JSONUtilities.getString(optionObj, "encoding", "UTF-8"));
                        
                        HiveDataImporter.parse(
                            project,
                            pm,
                            job,
                            -1,
                            optionObj,
                            exceptions
                        );
                        
                        if (!job.canceled) {
                            if (exceptions.size() > 0) {
                                job.setError(exceptions);
                            } else {
                                project.update(); // update all internal models, indexes, caches, etc.
                                
                                ProjectManager.singleton.registerProject(project, pm);
                                
                                job.setState("created-project");
                                job.setProjectID(project.id);
                            }
                            
                            job.touch();
                            job.updating = false;
                        }
                    }
                }.start();
                
                HttpUtilities.respond(response, "ok", "done");
            } catch (JSONException e) {
                throw new ServletException(e);
            }
        }

    
    private void doParsePreview(
            HttpServletRequest request, HttpServletResponse response, Properties parameters)
                throws ServletException, IOException {
            
            long jobID = Long.parseLong(parameters.getProperty("jobID"));
            ImportingJob job = ImportingManager.getJob(jobID);
            if (job == null) {
                HttpUtilities.respond(response, "error", "No such import job");
                return;
            }
            
            job.updating = true;
            try {
                JSONObject optionObj = ParsingUtilities.evaluateJsonStringToObject(
                    request.getParameter("options"));
                
                List<Exception> exceptions = new LinkedList<Exception>();
                
                job.prepareNewProject();
                
                HiveDataImporter.parse(
                    job.project,
                    job.metadata,
                    job,
                    100,
                    optionObj,
                    exceptions
                );
                
                Writer w = response.getWriter();
                JSONWriter writer = new JSONWriter(w);
                try {
                    writer.object();
                    if (exceptions.size() == 0) {
                        job.project.update(); // update all internal models, indexes, caches, etc.
                        
                        writer.key("status"); writer.value("ok");
                    } else {
                        writer.key("status"); writer.value("error");
                        
                        writer.key("errors");
                        writer.array();
                        DefaultImportingController.writeErrors(writer, exceptions);
                        writer.endArray();
                    }
                    writer.endObject();
                } catch (JSONException e) {
                    throw new ServletException(e);
                } finally {
                    w.flush();
                    w.close();
                }

            } catch (JSONException e) {
                throw new ServletException(e);
            } finally {
                job.touch();
                job.updating = false;
            }
        }

    
    private void doListDocuments(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {

            Writer w = response.getWriter();
            JSONWriter writer = new JSONWriter(w);
            try {
                writer.object();
                writer.key("documents");
                writer.array();
                
                try {
                    getHDFSTablesService(writer);
                } finally {
                    writer.endArray();
                    writer.endObject();
                }
            } catch (Exception e) {
                throw new ServletException(e);
            } finally {
                w.flush();
                w.close();
            }
        }
    
    
    private void getHDFSTablesService(JSONWriter writer)
            throws SQLException,IOException, JSONException {
        
        HiveDBConnection hdb = new HiveDBConnection();
        Connection con = hdb.getConnection();
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("show tables in default");
        int i=0;
        while (rs.next()) {
            writer.object();
            writer.key("docId"); writer.value(rs.getString(1));
            writer.key("docLink"); writer.value(rs.getString(1));
            writer.key("docSelfLink"); writer.value("SELF-LINK-"+ i);
            writer.key("title"); writer.value(rs.getString(1));
            writer.key("type"); writer.value("table");
            
            writer.key("updated"); writer.value("2 Days ago");
            
            writer.key("authors"); writer.array();
            writer.value("Person "+ i);
            
            writer.endArray();

            writer.endObject();
        }
        
    }
    
    private void doInitializeParserUI(
            HttpServletRequest request, HttpServletResponse response, Properties parameters)
                throws ServletException, IOException {
            
                        
            String type = parameters.getProperty("docType");
            String urlString = parameters.getProperty("docUrl");
            
          
            try {
                JSONObject result = new JSONObject();
                JSONObject options = new JSONObject();
                JSONUtilities.safePut(result, "status", "ok");
                JSONUtilities.safePut(result, "options", options);
                
                JSONUtilities.safePut(options, "skipDataLines", 0); // number of initial data lines to skip
                JSONUtilities.safePut(options, "storeBlankRows", true);
                JSONUtilities.safePut(options, "storeBlankCellsAsNulls", true);
                
                if ("spreadsheet".equals(type)) {
                 
                } else if ("table".equals(type)) {
                    // No metadata for a fusion table.
                }
                /* TODO: else */
                
                HttpUtilities.respond(response, result.toString());
            } catch (Exception e) {
                e.printStackTrace();
                HttpUtilities.respond(response, "error", "Internal error: " + e.getLocalizedMessage());
            }
        }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // TODO Auto-generated method stub
        HttpUtilities.respond(response, "error", "GET not implemented");

    }

}
