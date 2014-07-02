package com.google.refine.extension.ohdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.apache.tools.tar.TarOutputStream;

import com.google.refine.history.HistoryProcess;
import com.google.refine.io.FileProjectManager;
import com.google.refine.io.ProjectUtilities;
import com.google.refine.model.Project;
import com.google.refine.process.Process;


public class OHDFSUtil {
    
    public static void untar(File destDir, InputStream inputStream) throws IOException {
        TarInputStream tin = new TarInputStream(inputStream);
        TarEntry tarEntry = null;

        while ((tarEntry = tin.getNextEntry()) != null) {
            File destEntry = new File(destDir, tarEntry.getName());
            File parent = destEntry.getParentFile();

            if (!parent.exists()) {
                parent.mkdirs();
            }

            if (tarEntry.isDirectory()) {
                destEntry.mkdirs();
            } else {
                FileOutputStream fout = new FileOutputStream(destEntry);
                try {
                    tin.copyEntryContents(fout);
                } finally {
                    fout.close();
                }
            }
        }
        
        tin.close();
    }
    
    public static void tarDir(String relative, File dir, TarOutputStream tos) throws IOException{
        File[] files = dir.listFiles();
        for (File file : files) {
            if (!file.isHidden()) {
                String path = relative + file.getName();

                if (file.isDirectory()) {
                    tarDir(path + File.separator, file, tos);
                } else {
                    TarEntry entry = new TarEntry(path);

                    entry.setMode(TarEntry.DEFAULT_FILE_MODE);
                    entry.setSize(file.length());
                    entry.setModTime(file.lastModified());

                    tos.putNextEntry(entry);

                    copyFile(file, tos);

                    tos.closeEntry();
                }
            }
        }
    }
    
    public static void  copyFile(File file, OutputStream os) throws IOException {
        final int buffersize = 4096;

        FileInputStream fis = new FileInputStream(file);
        try {
            byte[] buf = new byte[buffersize];
            int count;

            while((count = fis.read(buf, 0, buffersize)) != -1) {
                os.write(buf, 0, count);
            }
        } finally {
            fis.close();
        }
    }

    public static void main(String[] args) throws Exception {
        
        String projectId="1441048234103";
        String destBaseDir="/tmp/openrefine4";
        String srcDir = "/home/ratnakar/.local/share/openrefine";
        initializeRefineProject(srcDir, projectId, destBaseDir);
        
    }

    public static void initializeRefineProject(String srcDir, String projectId, String destBaseDir)
            throws FileNotFoundException, IOException, Exception {
        Project project;
        String destDir=destBaseDir+"/"+projectId+".project";
        new File(destDir).mkdirs();
        System.out.println("Copying files from: " + new File(srcDir+"/"+projectId+".project").getPath()
                  + " TO " + new File(destDir).getPath());;
        FileUtils.copyDirectory(new File(srcDir+"/"+projectId+".project"), 
                new File(destDir),true);
        /*
        FileProjectManager.initialize(new File(destBaseDir));
        System.out.println("Initializing Project Dir: " + destDir + " DONE ");
        System.out.println(FileProjectManager.singleton==null);
        project = ProjectUtilities.load(new File(destDir), Long.parseLong(projectId));
        FileProjectManager.singleton.registerProject(project, project.getMetadata());
        Process process = new HistoryProcess(
                FileProjectManager.singleton.getProject(Long.parseLong(projectId)), 0);
        process.performImmediate();
        */
    }
}
