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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.test.utils.Identity;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Test cases to test join statement
 */
public class TestJoin extends TestCase {
    
    MiniCluster cluster;
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    ExecType[] execTypes = new ExecType[] {ExecType.LOCAL, ExecType.MAPREDUCE};
    
    @Before
    @Override
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
    }
    
    
    private void setUp(ExecType execType) throws ExecException {
        // cause a reinitialization of FileLocalizer's
        // internal state
        FileLocalizer.setInitialized(false);
        if(execType == ExecType.MAPREDUCE) {
            cluster =  MiniCluster.buildCluster();
            pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        } else if(execType == ExecType.LOCAL) {
            pigServer = new PigServer(ExecType.LOCAL);       
        }
    }
    
    private String createInputFile(ExecType execType, String fileNameHint, String[] data) throws IOException {
        String fileName = "";
        if(execType == ExecType.MAPREDUCE) {
            Util.createInputFile(cluster, fileNameHint, data);
            fileName = fileNameHint;
        } else if (execType == ExecType.LOCAL) {
            File f = Util.createInputFile("test", fileNameHint, data);
            fileName = "file://" + f.getAbsolutePath();
        }
        return fileName;
    }
    
    private void deleteInputFile(ExecType execType, String fileName) throws IOException {
    
        if(execType == ExecType.MAPREDUCE) {
            Util.deleteFile(cluster, fileName);
        } else if(execType == ExecType.LOCAL){
            fileName = fileName.replace("file://", "");
            new File(fileName).delete();
        }
    }

    
    @Test
    public void testJoinUnkownSchema() throws Exception {
        // If any of the input schema is unknown, the resulting schema should be unknown as well
        for (ExecType execType : execTypes) {
            setUp(execType);
            String script = "a = load 'a.txt';" +
                    "b = load 'b.txt'; " +
                    "c = join a by $0, b by $0;";
            Util.registerMultiLineQuery(pigServer, script);
            Schema schema = pigServer.dumpSchema("c");
            assertTrue(schema == null);
        }
    }
    
    @Test
    public void testDefaultJoin() throws IOException, ParseException {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            Tuple expectedResult = (Tuple)Util.getPigConstant("('hello',1,'hello','world')");
            
            // with schema
            String script = "a = load '"+ firstInput +"' as (n:chararray, a:int); " +
            		"b = load '"+ secondInput +"' as (n:chararray, m:chararray); " +
            		"c = join a by $0, b by $0;";
            Util.registerMultiLineQuery(pigServer, script);
            Iterator<Tuple> it = pigServer.openIterator("c");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult, it.next());
            assertEquals(false, it.hasNext());
            
            // without schema
            script = "a = load '"+ firstInput + "'; " +
            "b = load '" + secondInput + "'; " +
            "c = join a by $0, b by $0;";
            Util.registerMultiLineQuery(pigServer, script);
            it = pigServer.openIterator("c");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult.toString(), it.next().toString());
            assertEquals(false, it.hasNext());
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    
    @Test
    public void testJoinSchema() throws Exception {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "1\t2",
                    "2\t3",
                    "3\t4"
            };
            String[] input2 = {
                    "1\thello",
                    "4\tbye",
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            Tuple expectedResult = (Tuple)Util.getPigConstant("(1,2,1,'hello',1,2,1,'hello')");
            
            // with schema
            String script = "a = load '"+ firstInput +"' as (i:int, j:int); " +
                    "b = load '"+ secondInput +"' as (k:int, l:chararray); " +
                    "c = join a by $0, b by $0;" +
                    "d = foreach c generate i,j,k,l,a::i,a::j,b::k,b::l;";
            Util.registerMultiLineQuery(pigServer, script);
            Iterator<Tuple> it = pigServer.openIterator("d");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult, it.next());
            assertEquals(false, it.hasNext());
            
            // schema with duplicates
            script = "a = load '"+ firstInput +"' as (i:int, j:int); " +
            "b = load '"+ secondInput +"' as (i:int, l:chararray); " +
            "c = join a by $0, b by $0;" +
            "d = foreach c generate i,j,l,a::i,a::j,b::i,b::l;";
            boolean exceptionThrown = false;
            try{
                Util.registerMultiLineQuery(pigServer, script);
            }catch (Exception e) {
                PigException pe = LogUtils.getPigException(e);
                assertEquals(1025, pe.getErrorCode());
                exceptionThrown = true;
            }
            assertEquals(true, exceptionThrown);
            
            // schema with duplicates with resolution
            script = "a = load '"+ firstInput +"' as (i:int, j:int); " +
            "b = load '"+ secondInput +"' as (i:int, l:chararray); " +
            "c = join a by $0, b by $0;" +
            "d = foreach c generate a::i,j,b::i,l,a::i,a::j,b::i,b::l;";
            Util.registerMultiLineQuery(pigServer, script);
            it = pigServer.openIterator("d");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult, it.next());
            assertEquals(false, it.hasNext());
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
        
        
    }
    
    @Test
    public void testLeftOuterJoin() throws IOException, ParseException {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
    
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "('hello',1,'hello','world')",
                            "('bye',2,null,null)",
                            "(null,3,null,null)"
                    });
            
            // with and without optional outer
            for(int i = 0; i < 2; i++) {
                //with schema
                String script = "a = load '"+ firstInput +"' as (n:chararray, a:int); " +
                        "b = load '"+ secondInput +"' as (n:chararray, m:chararray); ";
                if(i == 0) {
                    script +=  "c = join a by $0 left outer, b by $0;" ;
                } else {
                    script +=  "c = join a by $0 left, b by $0;" ;
                }
                script += "d = order c by $1;";
                // ensure we parse correctly
                LogicalPlanTester lpt = new LogicalPlanTester();
                lpt.buildPlan(script);
                
                // run query and test results only once
                if(i == 0) {
                    Util.registerMultiLineQuery(pigServer, script);
                    Iterator<Tuple> it = pigServer.openIterator("d");
                    int counter= 0;
                    while(it.hasNext()) {
                        assertEquals(expectedResults.get(counter++), it.next());
                    }
                    assertEquals(expectedResults.size(), counter);
                    
                    // without schema
                    script = "a = load '"+ firstInput +"'; " +
                    "b = load '"+ secondInput +"'; ";
                    if(i == 0) {
                        script +=  "c = join a by $0 left outer, b by $0;" ;
                    } else {
                        script +=  "c = join a by $0 left, b by $0;" ;
                    }
                    try {
                        Util.registerMultiLineQuery(pigServer, script);
                    } catch (Exception e) {
                        PigException pe = LogUtils.getPigException(e);
                        assertEquals(1105, pe.getErrorCode());
                    }
                }
            }
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }

    @Test
    public void testRightOuterJoin() throws IOException, ParseException {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
    
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(null,null,null,'evening')",
                            "(null,null,'good','morning')",
                            "('hello',1,'hello','world')"
                                           });
            // with and without optional outer
            for(int i = 0; i < 2; i++) {
                // with schema
                String script = "a = load '"+ firstInput +"' as (n:chararray, a:int); " +
                        "b = load '"+ secondInput +"' as (n:chararray, m:chararray); ";
                if(i == 0) {
                    script +=  "c = join a by $0 right outer, b by $0;" ;
                } else {
                    script +=  "c = join a by $0 right, b by $0;" ;
                }
                script += "d = order c by $3;";
                // ensure we parse correctly
                LogicalPlanTester lpt = new LogicalPlanTester();
                lpt.buildPlan(script);
                
                // run query and test results only once
                if(i == 0) {
                    Util.registerMultiLineQuery(pigServer, script);
                    Iterator<Tuple> it = pigServer.openIterator("d");
                    int counter= 0;
                    while(it.hasNext()) {
                        assertEquals(expectedResults.get(counter++), it.next());
                    }
                    assertEquals(expectedResults.size(), counter);
                    
                    // without schema
                    script = "a = load '"+ firstInput +"'; " +
                    "b = load '"+ secondInput +"'; " ;
                    if(i == 0) {
                        script +=  "c = join a by $0 right outer, b by $0;" ;
                    } else {
                        script +=  "c = join a by $0 right, b by $0;" ;
                    }
                    try {
                        Util.registerMultiLineQuery(pigServer, script);
                    } catch (Exception e) {
                        PigException pe = LogUtils.getPigException(e);
                        assertEquals(1105, pe.getErrorCode());
                    }
                }
            }
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    @Test
    public void testFullOuterJoin() throws IOException, ParseException {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
    
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(null,null,null,'evening')" ,
                            "(null,null,'good','morning')" ,
                            "('hello',1,'hello','world')" ,
                            "('bye',2,null,null)" ,
                            "(null,3,null,null)"
                                           });
            // with and without optional outer
            for(int i = 0; i < 2; i++) {
                // with schema
                String script = "a = load '"+ firstInput +"' as (n:chararray, a:int); " +
                        "b = load '"+ secondInput +"' as (n:chararray, m:chararray); ";
                if(i == 0) {
                    script +=  "c = join a by $0 full outer, b by $0;" ;
                } else {
                    script +=  "c = join a by $0 full, b by $0;" ;
                }
                script += "d = order c by $1, $3;";
                // ensure we parse correctly
                LogicalPlanTester lpt = new LogicalPlanTester();
                lpt.buildPlan(script);
                
                // run query and test results only once
                if(i == 0) {
                    Util.registerMultiLineQuery(pigServer, script);
                    Iterator<Tuple> it = pigServer.openIterator("d");
                    int counter= 0;
                    while(it.hasNext()) {
                        assertEquals(expectedResults.get(counter++), it.next());
                    }
                    assertEquals(expectedResults.size(), counter);
                    
                    // without schema
                    script = "a = load '"+ firstInput +"'; " +
                    "b = load '"+ secondInput +"'; " ;
                    if(i == 0) {
                        script +=  "c = join a by $0 full outer, b by $0;" ;
                    } else {
                        script +=  "c = join a by $0 full, b by $0;" ;
                    }
                    try {
                        Util.registerMultiLineQuery(pigServer, script);
                    } catch (Exception e) {
                        PigException pe = LogUtils.getPigException(e);
                        assertEquals(1105, pe.getErrorCode());
                    }
                }
            }
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    @Test
    public void testMultiOuterJoinFailure() {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'a.txt' as (n:chararray, a:int); ");
        lpt.buildPlan("b = load 'b.txt' as (n:chararray, m:chararray); ");
        lpt.buildPlan("c = load 'c.txt' as (n:chararray, m:chararray); ");
        String[] types = new String[] { "left", "right", "full" };
        for (int i = 0; i < types.length; i++) {
            boolean errCaught = false;
            try {
                lpt.buildPlanThrowExceptionOnError("d = join a by $0 " + types[i] + " outer, b by $0, c by $0;") ;
                
            } catch(Exception e) {
                errCaught = true;
                assertEquals("(left|right|full) outer joins are only supported for two inputs", e.getMessage());
            }
            assertEquals(true, errCaught);
            
        }
        
    }
    
    @Test
    public void testNonRegularOuterJoinFailure() {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'a.txt' as (n:chararray, a:int); ");
        lpt.buildPlan("b = load 'b.txt' as (n:chararray, m:chararray); ");
        String[] types = new String[] { "left", "right", "full" };
        String[] joinTypes = new String[] { "replicated", "repl", "skewed", "merge" };
        for (int i = 0; i < types.length; i++) {
            for(int j = 0; j < joinTypes.length; j++) {
                boolean errCaught = false;
                try {
                    lpt.buildPlanThrowExceptionOnError(
                            "d = join a by $0 " + types[i] + " outer, b by $0 using \"" + joinTypes[j] +"\" ;") ;
                    
                } catch(Exception e) {
                    errCaught = true;
                    assertEquals(true, e.getMessage().contains("does not support (left|right|full) outer joins"));
                }
                assertEquals(true, errCaught);
            }
            
        }
        
    }
    
    @Test
    public void testJoinTupleFieldKey() throws Exception{
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "(1,a)",
                    "(2,aa)"
            };
            String[] input2 = {
                    "(1,b)",
                    "(2,bb)"
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            
            String script = "a = load '"+ firstInput +"' as (a:tuple(a1:int, a2:chararray));" +
                    "b = load '"+ secondInput +"' as (b:tuple(b1:int, b2:chararray));" +
                    "c = join a by a.a1, b by b.b1;";
            Util.registerMultiLineQuery(pigServer, script);
            Iterator<Tuple> it = pigServer.openIterator("c");
            
            assertTrue(it.hasNext());
            Tuple t = it.next();
            assertTrue(t.toString().equals("((1,a),(1,b))"));
            
            assertTrue(it.hasNext());
            t = it.next();
            assertTrue(t.toString().equals("((2,aa),(2,bb))"));
            
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }

}
