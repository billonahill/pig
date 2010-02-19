/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.test.utils.LocalSeekableInputStream;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.tools.bzip2r.CBZip2InputStream;
import org.apache.tools.bzip2r.CBZip2OutputStream;
import org.junit.Test;

public class TestBZip {
    MiniCluster cluster = MiniCluster.buildCluster();
    
   /**
    * Tests the end-to-end writing and reading of a BZip file.
    */
    @Test
    public void testBzipInPig() throws Exception {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
       
        File in = File.createTempFile("junit", ".bz2");
        in.deleteOnExit();
        
        File out = File.createTempFile("junit", ".bz2");
        out.deleteOnExit();
        out.delete();
               
        CBZip2OutputStream cos = 
            new CBZip2OutputStream(new FileOutputStream(in));
        for (int i = 1; i < 100; i++) {
            StringBuffer sb = new StringBuffer();
            sb.append(i).append("\n").append(-i).append("\n");
            byte bytes[] = sb.toString().getBytes();
            cos.write(bytes);
        }
        cos.close();
                       
        pig.registerQuery("AA = load '"
                + Util.generateURI(in.getAbsolutePath(), pig.getPigContext())
                + "';");
        pig.registerQuery("A = foreach (group (filter AA by $0 > 0) all) generate flatten($1);");
        pig.registerQuery("store A into '" + out.getAbsolutePath() + "';");
        
        File dir = new File("testbzip");     
        deleteFiles(dir);
        
        processCopyToLocal(pig, out.getAbsolutePath(), dir.getAbsolutePath());
        
        LocalSeekableInputStream is = new LocalSeekableInputStream(
                new File(dir.getAbsolutePath() + "/part-r-00000.bz2")); 
        
        CBZip2InputStream cis = new CBZip2InputStream(is);
        
        // Just a sanity check, to make sure it was a bzip file; we
        // will do the value verification later
        assertEquals(100, cis.read(new byte[100]));
        cis.close();
        
        pig.registerQuery("B = load '" + out.getAbsolutePath() + "';");
        
        Iterator<Tuple> i = pig.openIterator("B");
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        while (i.hasNext()) {
            Integer val = DataType.toInteger(i.next().get(0));
            map.put(val, val);            
        }
        
        assertEquals(new Integer(99), new Integer(map.keySet().size()));
        
        for (int j = 1; j < 100; j++) {
            assertEquals(new Integer(j), map.get(j));
        }
        
        in.delete();
        out.delete();
        
        deleteFiles(dir);
    }
    
    /**
     * Tests the end-to-end writing and reading of an empty BZip file.
     */
     @Test
     public void testEmptyBzipInPig() throws Exception {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                .getProperties());
 
        File in = File.createTempFile("junit", ".tmp");
        in.deleteOnExit();

        File out = File.createTempFile("junit", ".bz2");
        out.deleteOnExit();
        out.delete();
        
        FileOutputStream fos = new FileOutputStream(in);
        fos.write("55\n".getBytes());
        fos.close();
        System.out.println(in.getAbsolutePath());
        
        pig.registerQuery("AA = load '"
                + Util.generateURI(in.getAbsolutePath(), pig.getPigContext())
                + "';");
        pig.registerQuery("A=foreach (group (filter AA by $0 < '0') all) generate flatten($1);");
        pig.registerQuery("store A into '" + out.getAbsolutePath() + "';");
            
        File dir = new File("testbzip2");     
        deleteFiles(dir);
        
        processCopyToLocal(pig, out.getAbsolutePath(), dir.getAbsolutePath());
        
        LocalSeekableInputStream is = new LocalSeekableInputStream(
                new File(dir.getAbsolutePath() + "/part-r-00000.bz2")); 
        
        CBZip2InputStream cis = new CBZip2InputStream(is);
        
        // Just a sanity check, to make sure it was a bzip file; we
        // will do the value verification later
        assertEquals(-1, cis.read(new byte[100]));
        cis.close();
        
        pig.registerQuery("B = load '" + out.getAbsolutePath() + "';");
        pig.openIterator("B");
        
        in.delete();
        out.delete();
        
        deleteFiles(dir);
    }

    /**
     * Tests the writing and reading of an empty BZip file.
     */
    @Test
    public void testEmptyBzip() throws Exception {
        File tmp = File.createTempFile("junit", ".tmp");
        tmp.deleteOnExit();
        CBZip2OutputStream cos = new CBZip2OutputStream(new FileOutputStream(
                tmp));
        cos.close();
        assertNotSame(0, tmp.length());
        CBZip2InputStream cis = new CBZip2InputStream(
                new LocalSeekableInputStream(tmp));
        assertEquals(-1, cis.read(new byte[100]));
        cis.close();
        tmp.delete();
    }
    
    private void processCopyToLocal(PigServer pig, String src, String dst) 
            throws IOException {

        ElementDescriptor srcPath = pig.getPigContext().getDfs().asElement(src);
        ElementDescriptor dstPath = pig.getPigContext().getLfs().asElement(dst);
            
        srcPath.copy(dstPath, false);
    }
    
    private void deleteFiles(File file) {
        if (!file.exists()) return;
            
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                deleteFiles(f);
            }
        }
        System.out.println("delete file: " + file.getAbsolutePath() 
                + " : " + file.delete());
    }
}
