package com.google.refine.extension.ohdfs;


public class HiveService {
    
        private String srcName;
        
        public HiveService(String _srcName)
        {
            srcName=_srcName;
        }
        
        public Integer getRowCount() {
            return 1000;
        }
        
        public Integer getColCount() {
            return 10;
        }
        
    
}
