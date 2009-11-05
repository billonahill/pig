/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.zebra.io;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.io.file.tfile.Utils;
import org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists;
import org.apache.hadoop.io.file.tfile.MetaBlockDoesNotExist;
import org.apache.hadoop.io.file.tfile.Utils.Version;
import org.apache.hadoop.zebra.io.ColumnGroup.Reader.CGRangeSplit;
import org.apache.hadoop.zebra.types.CGSchema;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.TableSchemaParser;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.pig.data.Tuple;

/**
 * A materialized table that consists of one or more tightly coupled Column
 * Groups.
 * 
 * The following Configuration parameters can customize the behavior of
 * BasicTable.
 * <ul>
 * <li><b>table.output.tfile.minBlock.size</b> (int) Minimum compression block
 * size for underlying TFile (default to 1024*1024).
 * <li><b>table.output.tfile.compression</b> (String) Compression method (one of
 * "none", "lzo", "gz") (default is "gz"). @see
 * {@link TFile#getSupportedCompressionAlgorithms()}
 * <li><b>table.input.split.minSize</b> (int) Minimum split size (default to
 * 64*1024).
 * </ul>
 */
public class BasicTable {
  
  static Log LOG = LogFactory.getLog(BasicTable.class);
  
  // name of the BasicTable schema file
  private final static String BT_SCHEMA_FILE = ".btschema";
  // schema version
  private final static Version SCHEMA_VERSION =
      new Version((short) 1, (short) 1);
  // name of the BasicTable meta-data file
  private final static String BT_META_FILE = ".btmeta";
  // column group prefix
  private final static String CGPathPrefix = "CG";

  private final static String DELETED_CG_PREFIX = ".deleted-";
  
  // no public ctor for instantiating a BasicTable object
  private BasicTable() {
    // no-op
  }

  /**
   * Deletes the data for column group specified by cgName.
   * When the readers try to read the fields that were stored in the
   * column group get null since the underlying data is removed.
   * <br> <br>
   * 
   * Effect on the readers that are currently reading from the table while
   * a column group is droped is unspecified. Suggested practice is to 
   * drop column groups when there are no readers or writes for the table.
   * <br> <br>
   * 
   * Column group names are usually specified in the "storage hint" while
   * creating a table. If no name is specified, system assigns a simple name.
   * These names could be obtained through "dumpInfo()" and other methods.
   * <br> <br> 
   *
   * Dropping a column group that has already been removed is a no-op no 
   * exception is thrown.
   * 
   * @param path path to BasicTable
   * @param conf Configuration determines file system and other parameters.
   * @param cgName name of the column group to drop.
   * @throws IOException IOException could occur for various reasons. E.g.
   *         a user does not have permissions to write to table directory.
   *         
   */
  public static void dropColumnGroup(Path path, Configuration conf,
                                     String cgName) 
                                     throws IOException {
    
    FileSystem fs = FileSystem.get(conf);
    int triedCount = 0;
    int numCGs =  SchemaFile.getNumCGs(path, conf);
    SchemaFile schemaFile = null;
    
    /* Retry up to numCGs times accounting for other CG deleting threads or processes.*/
    while (triedCount ++ < numCGs) {
      try {
        schemaFile = new SchemaFile(path, conf);
        break;
      } catch (FileNotFoundException e) {
        LOG.info("Try " + triedCount + " times : " + e.getMessage());
      } catch (Exception e) {
        throw new IOException ("Cannot construct SchemaFile : " + e.getMessage());
      }
    }
    
    if (schemaFile == null) {
      throw new IOException ("Cannot construct SchemaFile");
    }
    
    int cgIdx = schemaFile.getCGByName(cgName);
    if (cgIdx < 0) {
      throw new IOException(path + 
             " : Could not find a column group with the name '" + cgName + "'");
    }
    
    Path cgPath = new Path(path, schemaFile.getName(cgIdx));
        
    //Clean up any previous unfinished attempts to drop column groups?    
    if (schemaFile.isCGDeleted(cgIdx)) {
      // Clean up unfinished delete if it exists. so that clean up can 
      // complete if the previous deletion was interrupted for some reason.
      if (fs.exists(cgPath)) {
        LOG.info(path + " : " + 
                 " clearing unfinished deletion of column group " +
                 cgName + ".");
        fs.delete(cgPath, true);
      }
      LOG.info(path + " : column group " + cgName + " is already deleted.");
      return;
    }
    
    // try to delete the column group:
    
    // first check if the user has enough permissions to list the directory
    fs.listStatus(cgPath);   
    
    //verify if the user has enough permissions by trying to create
    //a temporary file in cg.
    OutputStream out = fs.create(
              new Path(cgPath, ".tmp" + DELETED_CG_PREFIX + cgName), true);
    out.close();
    
    //First try to create a file indicating a column group is deleted.
    try {
      Path deletedCGPath = new Path(path, DELETED_CG_PREFIX + cgName);
      // create without overriding.
      out = fs.create(deletedCGPath, false);
      // should we write anything?
      out.close();
    } catch (IOException e) {
      // one remote possibility is that another user 
      // already deleted CG. 
      SchemaFile tempSchema = new SchemaFile(path, conf);
      if (tempSchema.isCGDeleted(cgIdx)) {
        LOG.info(path + " : " + cgName + 
                 " is deleted by someone else. That is ok.");
        return;
      }
      // otherwise, it is some other error.
      throw e;
    }
    
    // At this stage, the CG is marked deleted. Now just try to
    // delete the actual directory:
    if (!fs.delete(cgPath, true)) {
      String msg = path + " : Could not detete column group " +
                   cgName + ". It is marked deleted.";
      LOG.warn(msg);
      throw new IOException(msg);
    }
    
    LOG.info("Dropped " + cgName + " from " + path);
  }
  
  /**
   * BasicTable reader.
   */
  public static class Reader implements Closeable {
    private Path path;
    private boolean closed = true;
    private SchemaFile schemaFile;
    private Projection projection;
    boolean inferredMapping;
    private MetaFile.Reader metaReader;
    private BasicTableStatus status;
    private int firstValidCG = -1; /// First column group that exists.
    Partition partition;
    ColumnGroup.Reader[] colGroups;
    Tuple[] cgTuples;

    private synchronized void checkInferredMapping() throws ParseException, IOException {
      if (!inferredMapping) {
        for (int i = 0; i < colGroups.length; ++i) {
          if (colGroups[i] != null) {
            colGroups[i].setProjection(partition.getProjection(i));
          } 
          if (partition.isCGNeeded(i)) {
            if (isCGDeleted(i)) {
              // this is a deleted column group. Warn about it.
              LOG.warn("Trying to read from deleted column group " + 
                       schemaFile.getName(i) + 
                       ". NULL is returned for corresponding columns. " +
                       "Table at " + path);
            } else {
              cgTuples[i] = TypesUtils.createTuple(colGroups[i].getSchema());
            }
          }
          else
            cgTuples[i] = null;
        }
        partition.setSource(cgTuples);
        inferredMapping = true;
      }
      else {
        // the projection is not changed, so we do not need to recalculate the
        // mapping
      }
    }

    /**
     * Returns true if a column group is deleted.
     */
    private boolean isCGDeleted(int nx) {
      return colGroups[nx] == null;
    }
    
    /**
     * Create a BasicTable reader.
     * 
     * @param path
     *          The directory path to the BasicTable.
     * @param conf
     *          Optional configuration parameters.
     * @throws IOException
     */
    public Reader(Path path, Configuration conf) throws IOException {
      try {
        this.path = path;
        schemaFile = new SchemaFile(path, conf);
        metaReader = MetaFile.createReader(new Path(path, BT_META_FILE), conf);
        // create column group readers
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        Schema schema;
        colGroups = new ColumnGroup.Reader[numCGs];
        cgTuples = new Tuple[numCGs];
        // set default projection that contains everything
        schema = schemaFile.getLogical();
        projection = new Projection(schema);
        String storage = schemaFile.getStorageString();
        String comparator = schemaFile.getComparator();
        partition = new Partition(schema, projection, storage, comparator);
        for (int nx = 0; nx < numCGs; nx++) {
          if (!schemaFile.isCGDeleted(nx)) {
            colGroups[nx] =
              new ColumnGroup.Reader(new Path(path, partition.getCGSchema(nx).getName()),
                                     conf);
            if (firstValidCG < 0) {
              firstValidCG = nx;
            }
          }
          if (colGroups[nx] != null && partition.isCGNeeded(nx))
            cgTuples[nx] = TypesUtils.createTuple(colGroups[nx].getSchema());
          else
            cgTuples[nx] = null;
        }
        buildStatus();
        closed = false;
      }
      catch (Exception e) {
        throw new IOException("BasicTable.Reader constructor failed : "
            + e.getMessage());
      }
      finally {
        if (closed) {
          /**
           * Construction fails.
           */
          if (colGroups != null) {
            for (int i = 0; i < colGroups.length; ++i) {
              if (colGroups[i] != null) {
                try {
                  colGroups[i].close();
                }
                catch (Exception e) {
                  // ignore error
                }
              }
            }
          }
          if (metaReader != null) {
            try {
              metaReader.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }
    }

    /**
     * Is the Table sorted?
     * 
     * @return Whether the table is sorted.
     */
    public boolean isSorted() {
      return schemaFile.isSorted();
    }

    /**
     * @return the list of sorted columns
     */
    public SortInfo getSortInfo()
    {
      return schemaFile.getSortInfo();
    }

    /**
     * Set the projection for the reader. This will affect calls to
     * {@link #getScanner(RangeSplit, boolean)},
     * {@link #getScanner(BytesWritable, BytesWritable, boolean)},
     * {@link #getStatus()}, {@link #getSchema()}.
     * 
     * @param projection
     *          The projection on the BasicTable for subsequent read operations.
     *          For this version of implementation, the projection is a comma
     *          separated list of column names, such as
     *          "FirstName, LastName, Sex, Department". If we want select all
     *          columns, pass projection==null.
     * @throws IOException
     */
    public synchronized void setProjection(String projection)
        throws ParseException, IOException {
      if (projection == null) {
        this.projection = new Projection(schemaFile.getLogical());
        partition =
            new Partition(schemaFile.getLogical(), this.projection, schemaFile
                .getStorageString(), schemaFile.getComparator());
      }
      else {
        /**
         * the typed schema from projection which is untyped or actually typed
         * as "bytes"
         */
        this.projection =
            new Projection(schemaFile.getLogical(), projection);
        partition =
            new Partition(schemaFile.getLogical(), this.projection, schemaFile
                .getStorageString(), schemaFile.getComparator());
      }
      inferredMapping = false;
    }

    /**
     * Get the status of the BasicTable.
     */
    public BasicTableStatus getStatus() {
      return status;
    }

    /**
     * Given a split range, calculate how the file data that fall into the range
     * are distributed among hosts.
     * 
     * @param split
     *          The range-based split. Can be null to indicate the whole TFile.
     * @return An object that conveys how blocks fall in the split are
     *         distributed across hosts.
     * @see #rangeSplit(int)
     */
    public BlockDistribution getBlockDistribution(RangeSplit split)
        throws IOException {
      BlockDistribution bd = new BlockDistribution();
      for (int nx = 0; nx < colGroups.length; nx++) {
        if (!isCGDeleted(nx)) {
          bd.add(colGroups[nx].getBlockDistribution(split == null ? null : split
            .get(nx)));
        }
      }
      return bd;
    }

    /**
     * Collect some key samples and use them to partition the table. Only
     * applicable to sorted BasicTable. The returned {@link KeyDistribution}
     * object also contains information on how data are distributed for each
     * key-partitioned bucket.
     * 
     * @param n
     *          Targeted size of the sampling.
     * @return KeyDistribution object.
     * @throws IOException
     */
    public KeyDistribution getKeyDistribution(int n) throws IOException {
      KeyDistribution kd =
          new KeyDistribution(TFile.makeComparator(schemaFile.getComparator()));
      for (int nx = 0; nx < colGroups.length; nx++) {
        if (!isCGDeleted(nx)) {
           kd.add(colGroups[nx].getKeyDistribution(n));
        }
      }
      if (n >= 0 && kd.size() > (int) (n * 1.5)) {
        kd.resize(n);
      }
      return kd;
    }

    /**
     * Get a scanner that reads all rows whose row keys fall in a specific
     * range. Only applicable to sorted BasicTable.
     * 
     * @param beginKey
     *          The begin key of the scan range. If null, start from the first
     *          row in the table.
     * @param endKey
     *          The end key of the scan range. If null, scan till the last row
     *          in the table.
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @return A scanner object.
     * @throws IOException
     */
    public synchronized TableScanner getScanner(BytesWritable beginKey,
        BytesWritable endKey, boolean closeReader) throws IOException {
      try {
        checkInferredMapping();
      }
      catch (Exception e) {
        throw new IOException("getScanner failed : " + e.getMessage());
      }
      return new BTScanner(beginKey, endKey, closeReader, partition);
    }

    /**
     * Get a scanner that reads a consecutive number of rows as defined in the
     * {@link RangeSplit} object, which should be obtained from previous calls
     * of {@link #rangeSplit(int)}.
     * 
     * @param split
     *          The split range. If null, get a scanner to read the complete
     *          table.
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @return A scanner object.
     * @throws IOException
     */
    public synchronized TableScanner getScanner(RangeSplit split,
        boolean closeReader) throws IOException, ParseException {
      checkInferredMapping();
      return new BTScanner(split, partition, closeReader);
    }

    /**
     * Get the schema of the table. The schema may be different from
     * {@link BasicTable.Reader#getSchema(Path, Configuration)} if a projection
     * has been set on the table.
     * 
     * @return The schema of the BasicTable.
     */
    public Schema getSchema() {
      return projection.getSchema();
    }

    /**
     * Get the BasicTable schema without loading the full table index.
     * 
     * @param path
     *          The path to the BasicTable.
     * @param conf
     * @return The logical Schema of the table (all columns).
     * @throws IOException
     */
    public static Schema getSchema(Path path, Configuration conf)
        throws IOException {
      SchemaFile schF = new SchemaFile(path, conf);
      return schF.getLogical();
    }

    /**
     * Get the path to the table.
     * 
     * @return The path string to the table.
     */
    public String getPath() {
      return path.toString();
    }

    /**
     * Split the table into at most n parts.
     * 
     * @param n
     *          Maximum number of parts in the output list.
     * @return A list of RangeSplit objects, each of which can be used to
     *         construct TableScanner later.
     */
    @SuppressWarnings("unchecked")
    public List<RangeSplit> rangeSplit(int n) throws IOException {
      // assume all CGs will be split into the same number of horizontal
      // slices
      List<CGRangeSplit>[] cgSplitsAll = new ArrayList[colGroups.length];
      // split each CG
      for (int nx = 0; nx < colGroups.length; nx++) {
        if (!isCGDeleted(nx))
          cgSplitsAll[nx] = colGroups[nx].rangeSplit(n);
      }

      // verify all CGs have same number of slices
      int numSlices = -1;
      for (int nx = 0; nx < cgSplitsAll.length; nx++) {
        if (isCGDeleted(nx)) {
          continue;
        }
        if (numSlices < 0) {
          numSlices = cgSplitsAll[nx].size();
        }
        else if (cgSplitsAll[nx].size() != numSlices) {
          throw new IOException(
              "BasicTable's column groups were not equally split.");
        }
      }
      if (numSlices <= 0) {
        // This could happen because of various reasons.
        // One possibility is that all the CGs are deleted.
        numSlices = 1;
      }
      // return horizontal slices as RangeSplits
      List<RangeSplit> ret = new ArrayList<RangeSplit>(numSlices);
      for (int slice = 0; slice < numSlices; slice++) {
        CGRangeSplit[] oneSliceSplits = new CGRangeSplit[cgSplitsAll.length];
        for (int cgIndex = 0; cgIndex < cgSplitsAll.length; cgIndex++) {
          if (isCGDeleted(cgIndex)) {
            // set a dummy split
            oneSliceSplits[cgIndex] = new CGRangeSplit(0, 0);
          } else {
            oneSliceSplits[cgIndex] = cgSplitsAll[cgIndex].get(slice);
          }
        }
        ret.add(new BasicTable.Reader.RangeSplit(oneSliceSplits));
      }
      return ret;
    }

    /**
     * Close the BasicTable for reading. Resources are released.
     */
    @Override
    public void close() throws IOException {
      if (!closed) {
        try {
          closed = true;
          metaReader.close();
          for (int i = 0; i < colGroups.length; ++i) {
            if (colGroups[i] != null) {
              colGroups[i].close();
            }
          }
        }
        finally {
          try {
            metaReader.close();
          }
          catch (Exception e) {
            // no-op
          }
          for (int i = 0; i < colGroups.length; ++i) {
            try {
              colGroups[i].close();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }
    }

    String getBTSchemaString() {
      return schemaFile.getBTSchemaString();
    }

    String getStorageString() {
      return schemaFile.getStorageString();
    }

    private void buildStatus() {
      status = new BasicTableStatus();
      if (firstValidCG >= 0) {
        status.beginKey = colGroups[firstValidCG].getStatus().getBeginKey();
        status.endKey = colGroups[firstValidCG].getStatus().getEndKey();
        status.rows = colGroups[firstValidCG].getStatus().getRows();
      } else {
        status.beginKey = new BytesWritable(new byte[0]);
        status.endKey = status.beginKey;
        status.rows = 0;
      }
      status.size = 0;
      for (int nx = 0; nx < colGroups.length; nx++) {
        if (colGroups[nx] != null) {
          status.size += colGroups[nx].getStatus().getSize();
        }
      }
    }

    /**
     * Obtain an input stream for reading a meta block.
     * 
     * @param name
     *          The name of the meta block.
     * @return The input stream for reading the meta block.
     * @throws IOException
     * @throws MetaBlockDoesNotExist
     */
    public DataInputStream getMetaBlock(String name)
        throws MetaBlockDoesNotExist, IOException {
      return metaReader.getMetaBlock(name);
    }

    /**
     * A range-based split on the metaReadertable.The content of the split is
     * implementation-dependent.
     */
    public static class RangeSplit implements Writable {
      CGRangeSplit[] slice;

      RangeSplit(CGRangeSplit[] splits) {
        slice = splits;
      }

      /**
       * Default constructor.
       */
      public RangeSplit() {
        // no-op
      }

      /**
       * @see Writable#readFields(DataInput)
       */
      @Override
      public void readFields(DataInput in) throws IOException {
        int count = Utils.readVInt(in);
        slice = new CGRangeSplit[count];
        for (int nx = 0; nx < count; nx++) {
          CGRangeSplit cgrs = new CGRangeSplit();
          cgrs.readFields(in);
          slice[nx] = cgrs;
        }
      }

      /**
       * @see Writable#write(DataOutput)
       */
      @Override
      public void write(DataOutput out) throws IOException {
        Utils.writeVInt(out, slice.length);
        for (CGRangeSplit split : slice) {
          split.write(out);
        }
      }

      CGRangeSplit get(int index) {
        return slice[index];
      }
    }

    /**
     * BasicTable scanner class
     */
    private class BTScanner implements TableScanner {
      private Projection schema;
      private TableScanner[] cgScanners;
      private int opCount = 0;
      Random random = new Random(System.nanoTime());
      // checking for consistency once every 1000 times.
      private static final int VERIFY_FREQ = 1000;
      private boolean sClosed = false;
      private boolean closeReader;
      private Partition partition;

      private synchronized boolean checkIntegrity() {
        return ((++opCount % VERIFY_FREQ) == 0) && (cgScanners.length > 1);
      }

      public BTScanner(BytesWritable beginKey, BytesWritable endKey,
          boolean closeReader, Partition partition) throws IOException {
        this.partition = partition;
        boolean anyScanner = false;
        try {
          schema = partition.getProjection();
          cgScanners = new TableScanner[colGroups.length];
          for (int i = 0; i < colGroups.length; ++i) {
            if (!isCGDeleted(i) && partition.isCGNeeded(i)) 
            {
              anyScanner = true;
              cgScanners[i] = colGroups[i].getScanner(beginKey, endKey, false);
            } else
              cgScanners[i] = null;
          }
          if (!anyScanner && firstValidCG >= 0) {
            // if no CG is needed explicitly by projection but the "countRow" still needs to access some column group
            cgScanners[firstValidCG] = colGroups[firstValidCG].
                                         getScanner(beginKey, endKey, false);
          }
          this.closeReader = closeReader;
          sClosed = false;
        }
        catch (Exception e) {
          throw new IOException("BTScanner constructor failed : "
              + e.getMessage());
        }
        finally {
          if (sClosed) {
            if (cgScanners != null) {
              for (int i = 0; i < cgScanners.length; ++i) {
                if (cgScanners[i] != null) {
                  try {
                    cgScanners[i].close();
                    cgScanners[i] = null;
                  }
                  catch (Exception e) {
                    // no-op
                  }
                }
              }
            }
          }
        }
      }

      public BTScanner(RangeSplit split, Partition partition,
          boolean closeReader) throws IOException {
        try {
          schema = partition.getProjection();
          cgScanners = new TableScanner[colGroups.length];
          boolean anyScanner = false;
          for (int i = 0; i < colGroups.length; ++i) {
            // if no CG is needed explicitly by projection but the "countRow" still needs to access some column group
            if (!isCGDeleted(i) && partition.isCGNeeded(i))
            {
              cgScanners[i] =
                  colGroups[i].getScanner(split == null ? null : split.get(i),
                      false);
              anyScanner = true;
            } else
              cgScanners[i] = null;
          }
          if (!anyScanner && firstValidCG >= 0) {
            // if no CG is needed explicitly by projection but the "countRow" still needs to access some column group
            cgScanners[firstValidCG] = colGroups[firstValidCG].
              getScanner(split == null ? null : split.get(firstValidCG), false);
          }
          this.partition = partition;
          this.closeReader = closeReader;
          sClosed = false;
        }
        catch (Exception e) {
          throw new IOException("BTScanner constructor failed : "
              + e.getMessage());
        }
        finally {
          if (sClosed) {
            if (cgScanners != null) {
              for (int i = 0; i < cgScanners.length; ++i) {
                if (cgScanners[i] != null) {
                  try {
                    cgScanners[i].close();
                    cgScanners[i] = null;
                  }
                  catch (Exception e) {
                    // no-op
                  }
                }
              }
            }
          }
        }
      }

      @Override
      public boolean advance() throws IOException {
        boolean first = false, cur, firstAdvance = true;
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] != null)
          {
            cur = cgScanners[nx].advance();
            if (!firstAdvance) {
              if (cur != first) {
                throw new IOException(
                    "advance() failed: Column Groups are not evenly positioned.");
              }
            }
            else {
              firstAdvance = false;
              first = cur;
            }
          }
        }
        return first;
      }

      @Override
      public boolean atEnd() throws IOException {
        if (cgScanners.length == 0) {
          return true;
        }
        boolean ret = true;
        int i;
        for (i = 0; i < cgScanners.length; i++)
        {
          if (cgScanners[i] != null)
          {
            ret = cgScanners[i].atEnd();
            break;
          }
        }

        if (i == cgScanners.length)
        {
          return true;
        }
        
        if (!checkIntegrity()) {
          return ret;
        }

        while (true)
        {
          int index = random.nextInt(cgScanners.length);
          if (cgScanners[index] != null) {
            if (cgScanners[index].atEnd() != ret) {
              throw new IOException(
                  "atEnd() failed: Column Groups are not evenly positioned.");
            }
            break;
          }
        }
        return ret;
      }

      @Override
      public void getKey(BytesWritable key) throws IOException {
        if (cgScanners.length == 0) {
          return;
        }
        
        int i;
        for (i = 0; i < cgScanners.length; i++)
        {
          if (cgScanners[i] != null)
          {
            cgScanners[i].getKey(key);
            break;
          }
        }

        if (i == cgScanners.length)
          return;

        if (!checkIntegrity()) {
          return;
        }

        while (true)
        {
          int index = random.nextInt(cgScanners.length);
          if (cgScanners[index] != null)
          {
            BytesWritable key2 = new BytesWritable();
            cgScanners[index].getKey(key2);
            if (key.equals(key2)) {
              return;
            }
            break;
          }
        }
        throw new IOException(
            "getKey() failed: Column Groups are not evenly positioned.");
      }

      @Override
      public void getValue(Tuple row) throws IOException {
        if (row.size() < projection.getSchema().getNumColumns()) {
          throw new IOException("Mismatched tuple object");
        }

        for (int i = 0; i < cgScanners.length; ++i)
        {
          if (cgScanners[i] != null)
          {
            if (partition.isCGNeeded(i))
            {
              if (cgTuples[i] == null)
                throw new AssertionError("cgTuples["+i+"] is null");
              cgScanners[i].getValue(cgTuples[i]);
            }
          }
        }

        try {
          partition.read(row);
        }
        catch (Exception e) {
          throw new IOException("getValue() failed: " + e.getMessage());
        }
      }

      @Override
      public boolean seekTo(BytesWritable key) throws IOException {
        boolean first = false, cur;
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] == null)
            continue;
          cur = cgScanners[nx].seekTo(key);
          if (nx != 0) {
            if (cur != first) {
              throw new IOException(
                  "seekTo() failed: Column Groups are not evenly positioned.");
            }
          }
          else {
            first = cur;
          }
        }
        return first;
      }

      @Override
      public void seekToEnd() throws IOException {
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] == null)
            continue;
          cgScanners[nx].seekToEnd();
        }
      }

      @Override
      public String getProjection() {
        return schema.toString();
      }
      
      @Override
      public Schema getSchema() {
        return schema.getSchema();
      }

      @Override
      public void close() throws IOException {
        if (sClosed) return;
        sClosed = true;
        try {
          for (int nx = 0; nx < cgScanners.length; nx++) {
            if (cgScanners[nx] == null)
              continue;
            cgScanners[nx].close();
            cgScanners[nx] = null;
          }
          if (closeReader) {
            BasicTable.Reader.this.close();
          }
        }
        finally {
          for (int nx = 0; nx < cgScanners.length; nx++) {
            if (cgScanners[nx] == null)
              continue;
            try {
              cgScanners[nx].close();
              cgScanners[nx] = null;
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (closeReader) {
            try {
              BasicTable.Reader.this.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }
    }
  }

  /**
   * BasicTable writer.
   */
  public static class Writer implements Closeable {
    private SchemaFile schemaFile;
    private MetaFile.Writer metaWriter;
    private boolean closed = true;
    ColumnGroup.Writer[] colGroups;
    Partition partition;
    boolean sorted;
    private boolean finished;
    Tuple[] cgTuples;

    /**
     * Create a BasicTable writer. The semantics are as follows:
     * <ol>
     * <li>If path does not exist:
     * <ul>
     * <li>create the path directory, and initialize the directory for future
     * row insertion..
     * </ul>
     * <li>If path exists and the directory is empty: initialize the directory
     * for future row insertion.
     * <li>If path exists and contains what look like a complete BasicTable,
     * IOException will be thrown.
     * </ol>
     * This constructor never removes a valid/complete BasicTable.
     * 
     * @param path
     *          The path to the Basic Table, either not existent or must be a
     *          directory.
     * @param btSchemaString
     *          The schema of the Basic Table. For this version of
     *          implementation, the schema of a table is a comma or
     *          semicolon-separated list of column names, such as
     *          "FirstName, LastName; Sex, Department".
     * @param sortColumns
     *          String of comma-separated sorted columns: null for unsorted tables
     * @param comparator
     *          Name of the comparator used in sorted tables
     * @param conf
     *          Optional Configuration objects.
     * 
     * @throws IOException
     * @see Schema
     */
    public Writer(Path path, String btSchemaString, String btStorageString, String sortColumns,
        String comparator, Configuration conf) throws IOException {
      try {
        schemaFile =
            new SchemaFile(path, btSchemaString, btStorageString, sortColumns,
                comparator, conf);
        partition = schemaFile.getPartition();
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        colGroups = new ColumnGroup.Writer[numCGs];
        cgTuples = new Tuple[numCGs];
        sorted = schemaFile.isSorted();
        for (int nx = 0; nx < numCGs; nx++) {
          colGroups[nx] =
              new ColumnGroup.Writer( 
                 new Path(path, schemaFile.getName(nx)),
            		 schemaFile.getPhysicalSchema(nx), 
            		 sorted, 
                 comparator,
            		 schemaFile.getName(nx),
            		 schemaFile.getSerializer(nx), 
            		 schemaFile.getCompressor(nx), 
            		 schemaFile.getOwner(nx), 
            		 schemaFile.getGroup(nx),
            		 schemaFile.getPerm(nx), 
            		 false, 
            		 conf);
          cgTuples[nx] = TypesUtils.createTuple(colGroups[nx].getSchema());
        }
        metaWriter = MetaFile.createWriter(new Path(path, BT_META_FILE), conf);
        partition.setSource(cgTuples);
        closed = false;
      }
      catch (Exception e) {
        throw new IOException("ColumnGroup.Writer constructor failed : "
            + e.getMessage());
      }
      finally {
        ;
        if (!closed) return;
        if (metaWriter != null) {
          try {
            metaWriter.close();
          }
          catch (Exception e) {
            // no-op
          }
        }
        if (colGroups != null) {
          for (int i = 0; i < colGroups.length; ++i) {
            if (colGroups[i] != null) {
              try {
                colGroups[i].close();
              }
              catch (Exception e) {
                // no-op
              }
            }
          }
        }
      }
    }

    /**
     * a wrapper to support backward compatible constructor
     */
    public Writer(Path path, String btSchemaString, String btStorageString,
        Configuration conf) throws IOException {
      this(path, btSchemaString, btStorageString, null, null, conf);
    }

    /**
    /**
     * Reopen an already created BasicTable for writing. Exception will be
     * thrown if the table is already closed, or is in the process of being
     * closed.
     */
    public Writer(Path path, Configuration conf) throws IOException {
      try {
        schemaFile = new SchemaFile(path, conf);
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        partition = schemaFile.getPartition();
        sorted = schemaFile.isSorted();
        colGroups = new ColumnGroup.Writer[numCGs];
        cgTuples = new Tuple[numCGs];
        for (int nx = 0; nx < numCGs; nx++) {
          colGroups[nx] =
            new ColumnGroup.Writer(new Path(path, partition.getCGSchema(nx).getName()),
                  conf);
          cgTuples[nx] = TypesUtils.createTuple(colGroups[nx].getSchema());
        }
        partition.setSource(cgTuples);
        metaWriter = MetaFile.createWriter(new Path(path, BT_META_FILE), conf);
        closed = false;
      }
      catch (Exception e) {
        throw new IOException("ColumnGroup.Writer failed : " + e.getMessage());
      }
      finally {
        if (!closed) return;
        if (metaWriter != null) {
          try {
            metaWriter.close();
          }
          catch (Exception e) {
            // no-op
          }
        }
        if (colGroups != null) {
          for (int i = 0; i < colGroups.length; ++i) {
            if (colGroups[i] != null) {
              try {
                colGroups[i].close();
              }
              catch (Exception e) {
                // no-op
              }
            }
          }
        }
      }
    }

    /**
     * Release resources used by the object. Unlike close(), finish() does not
     * make the table immutable.
     */
    public void finish() throws IOException {
      if (finished) return;
      finished = true;
      try {
        for (int nx = 0; nx < colGroups.length; nx++) {
          if (colGroups[nx] != null) {
            colGroups[nx].finish();
          }
        }
        metaWriter.finish();
      }
      finally {
        try {
          metaWriter.finish();
        }
        catch (Exception e) {
          // no-op
        }
        for (int i = 0; i < colGroups.length; ++i) {
          try {
            colGroups[i].finish();
          }
          catch (Exception e) {
            // no-op
          }
        }
      }
    }

    /**
     * Close the BasicTable for writing. No more inserters can be obtained after
     * close().
     */
    @Override
    public void close() throws IOException {
      if (closed) return;
      closed = true;
      if (!finished)
        finish();
      try {
        for (int nx = 0; nx < colGroups.length; nx++) {
          if (colGroups[nx] != null) {
            colGroups[nx].close();
          }
        }
        metaWriter.close();
      }
      finally {
        try {
          metaWriter.close();
        }
        catch (Exception e) {
          // no-op
        }
        for (int i = 0; i < colGroups.length; ++i) {
          try {
            colGroups[i].close();
          }
          catch (Exception e) {
            // no-op
          }
        }
      }
    }

    /**
     * Get the schema of the table.
     * 
     * @return the Schema object.
     */
    public Schema getSchema() {
      return schemaFile.getLogical();
    }
    
    /**
     * @return sortness
     */
    public boolean isSorted() {
    	return sorted;
    }

    /**
     * Get the list of sorted columns.
     * @return the list of sorted columns
     */
    public SortInfo getSortInfo()
    {
      return schemaFile.getSortInfo();
    }

    /**
     * Get a inserter with a given name.
     * 
     * @param name
     *          the name of the inserter. If multiple calls to getInserter with
     *          the same name has been called, we expect they are the result of
     *          speculative execution and at most one of them will succeed.
     * @param finishWriter
     *          finish the underlying Writer object upon the close of the
     *          Inserter. Should be set to true if there is only one inserter
     *          operate on the table, so we should call finish() after the
     *          Inserter is closed.
     * 
     * @return A inserter object.
     * @throws IOException
     */
    public TableInserter getInserter(String name, boolean finishWriter)
        throws IOException {
      if (closed) {
        throw new IOException("BasicTable closed");
      }
      return new BTInserter(name, finishWriter, partition);
    }

    /**
     * Obtain an output stream for creating a Meta Block with the specific name.
     * This method can only be called after we insert all rows into the table.
     * All Meta Blocks must be created by a single process prior to closing the
     * table. No more inserter can be created after this call.
     * 
     * @param name
     *          The name of the Meta Block
     * @return The output stream. Close the stream to conclude the writing.
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     */
    public DataOutputStream createMetaBlock(String name)
        throws MetaBlockAlreadyExists, IOException {
      return metaWriter.createMetaBlock(name);
    }

    private class BTInserter implements TableInserter {
      private TableInserter cgInserters[];
      private boolean sClosed = true;
      private boolean finishWriter;
      private Partition partition = null;

      BTInserter(String name, boolean finishWriter, Partition partition)
          throws IOException {
        try {
          cgInserters = new ColumnGroup.Writer.CGInserter[colGroups.length];
          for (int nx = 0; nx < colGroups.length; nx++) {
            cgInserters[nx] = colGroups[nx].getInserter(name, false);
          }
          this.finishWriter = finishWriter;
          this.partition = partition;
          sClosed = false;
        }
        catch (Exception e) {
          throw new IOException("BTInsert constructor failed :"
              + e.getMessage());
        }
        finally {
          if (sClosed) {
            if (cgInserters != null) {
              for (int i = 0; i < cgInserters.length; ++i) {
                if (cgInserters[i] != null) {
                  try {
                    cgInserters[i].close();
                  }
                  catch (Exception e) {
                    // no-op
                  }
                }
              }
            }
          }
        }
      }

      @Override
      public Schema getSchema() {
        return Writer.this.getSchema();
      }

      @Override
      public void insert(BytesWritable key, Tuple row) throws IOException {
        if (sClosed) {
          throw new IOException("Inserter already closed");
        }

        // break the input row into sub-tuples, then insert them into the
        // corresponding CGs
        int curTotal = 0;
        try {
          partition.insert(key, row);
        }
        catch (Exception e) {
          throw new IOException("insert failed : " + e.getMessage());
        }
        for (int nx = 0; nx < colGroups.length; nx++) {
          Tuple subTuple = cgTuples[nx];
          int numCols = subTuple.size();
          cgInserters[nx].insert(key, subTuple);
          curTotal += numCols;
        }
      }

      @Override
      public void close() throws IOException {
        if (sClosed) return;
        sClosed = true;
        try {
          for (TableInserter ins : cgInserters) {
            ins.close();
          }
          if (finishWriter) {
            BasicTable.Writer.this.finish();
          }
        }
        finally {
          for (TableInserter ins : cgInserters) {
            try {
              ins.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (finishWriter) {
            try {
              BasicTable.Writer.this.finish();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }
    }
  }

  /**
   * Drop a Basic Table, all files consisting of the BasicTable will be removed.
   * 
   * @param path
   *          the path to the Basic Table.
   * @param conf
   *          The configuration object.
   * @throws IOException
   */
  public static void drop(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
  }

  static class SchemaFile {
    private Version version;
    String comparator;
    Schema logical;
    Schema[] physical;
    Partition partition;
    boolean sorted;
    SortInfo sortInfo = null;
    String storage;
    CGSchema[] cgschemas;
    
    // Array indicating if a physical schema is already dropped
    // It is probably better to create "CGProperties" class and
    // store multiple properties like name there.
    boolean[] cgDeletedFlags;
   
    // ctor for reading
    public SchemaFile(Path path, Configuration conf) throws IOException {
      readSchemaFile(path, conf);
    }

    public Schema[] getPhysicalSchema() {
      return physical;
    }

    // ctor for writing
    public SchemaFile(Path path, String btSchemaStr, String btStorageStr, String sortColumns,
        String btComparator, Configuration conf)
        throws IOException {
      storage = btStorageStr;
      try {
        partition = new Partition(btSchemaStr, btStorageStr, btComparator, sortColumns);
      }
      catch (Exception e) {
        throw new IOException("Partition constructor failed :" + e.getMessage());
      }
      this.sortInfo = partition.getSortInfo();
      this.sorted = partition.isSorted();
      this.comparator = (this.sortInfo == null ? null : this.sortInfo.getComparator());
      if (this.comparator == null)
        this.comparator = "";
      logical = partition.getSchema();
      cgschemas = partition.getCGSchemas();
      physical = new Schema[cgschemas.length];
      for (int nx = 0; nx < cgschemas.length; nx++) {
        physical[nx] = cgschemas[nx].getSchema();
      }
      cgDeletedFlags = new boolean[physical.length];

      version = SCHEMA_VERSION;

      // write out the schema
      createSchemaFile(path, conf);
    }

    public String getComparator() {
      return comparator;
    }

    public Partition getPartition() {
      return partition;
    }

    public boolean isSorted() {
      return sorted;
    }

    public SortInfo getSortInfo() {
      return sortInfo;
    }

    public Schema getLogical() {
      return logical;
    }

    public int getNumOfPhysicalSchemas() {
      return physical.length;
    }

    public Schema getPhysicalSchema(int nx) {
      return physical[nx];
    }
    
    public String getName(int nx) {
      return cgschemas[nx].getName();
    }
    
    public String getSerializer(int nx) {
      return cgschemas[nx].getSerializer();
    }

    public String getCompressor(int nx) {
      return cgschemas[nx].getCompressor();
    }

    /**
     * Returns the index for CG with the given name. -1 indicates that there is
     * no CG with the name.
     */
    int getCGByName(String cgName) {
      for(int i=0; i<physical.length; i++) {
        if (cgName.equals(getName(i))) {
          return i;
        }
      }
      return -1;
    }
    
    /** Returns if the CG at the given index is delete */
    boolean isCGDeleted(int idx) {
      return cgDeletedFlags[idx];
    }
    
    public String getOwner(int nx) {
        return cgschemas[nx].getOwner();
      }

    public String getGroup(int nx) {
        return cgschemas[nx].getGroup();
    }

    public short getPerm(int nx) {
        return cgschemas[nx].getPerm();
    }
    
    /**
     * @return the string representation of the physical schema.
     */
    public String getBTSchemaString() {
      return logical.toString();
    }

    /**
     * @return the string representation of the storage hints
     */
    public String getStorageString() {
      return storage;
    }

    private void createSchemaFile(Path path, Configuration conf)
        throws IOException {
      // TODO: overwrite existing schema file, or need a flag?
      FSDataOutputStream outSchema =
          path.getFileSystem(conf).create(makeSchemaFilePath(path), true);
      version.write(outSchema);
      WritableUtils.writeString(outSchema, comparator);
      WritableUtils.writeString(outSchema, logical.toString());
      WritableUtils.writeString(outSchema, storage);
      WritableUtils.writeVInt(outSchema, physical.length);
      for (int nx = 0; nx < physical.length; nx++) {
        WritableUtils.writeString(outSchema, physical[nx].toString());
      }
      WritableUtils.writeVInt(outSchema, sorted ? 1 : 0);
      WritableUtils.writeVInt(outSchema, sortInfo == null ? 0 : sortInfo.size());
      if (sortInfo != null && sortInfo.size() > 0)
      {
        String[] sortedCols = sortInfo.getSortColumnNames();
        for (int i = 0; i < sortInfo.size(); i++)
        {
          WritableUtils.writeString(outSchema, sortedCols[i]);
        }
      }
      outSchema.close();
    }

    private void readSchemaFile(Path path, Configuration conf)
        throws IOException {
      Path pathSchema = makeSchemaFilePath(path);
      if (!path.getFileSystem(conf).exists(pathSchema)) {
        throw new IOException("BT Schema file doesn't exist: " + pathSchema);
      }
      // read schema file
      FSDataInputStream in = path.getFileSystem(conf).open(pathSchema);
      version = new Version(in);
      // verify compatibility against SCHEMA_VERSION
      if (!version.compatibleWith(SCHEMA_VERSION)) {
        new IOException("Incompatible versions, expecting: " + SCHEMA_VERSION
            + "; found in file: " + version);
      }
      comparator = WritableUtils.readString(in);
      String logicalStr = WritableUtils.readString(in);
      try {
        logical = new Schema(logicalStr);
      }
      catch (Exception e) {
        ;
        throw new IOException("Schema build failed :" + e.getMessage());
      }
      storage = WritableUtils.readString(in);
      try {
        partition = new Partition(logicalStr, storage, comparator);
      }
      catch (Exception e) {
        throw new IOException("Partition constructor failed :" + e.getMessage());
      }
      cgschemas = partition.getCGSchemas();
      int numCGs = WritableUtils.readVInt(in);
      physical = new Schema[numCGs];
      cgDeletedFlags = new boolean[physical.length];
      TableSchemaParser parser;
      String cgschemastr;
      try {
        for (int nx = 0; nx < numCGs; nx++) {
          cgschemastr = WritableUtils.readString(in);
          parser = new TableSchemaParser(new StringReader(cgschemastr));
          physical[nx] = parser.RecordSchema(null);
        }
      }
      catch (Exception e) {
        throw new IOException("parser.RecordSchema failed :" + e.getMessage());
      }
      sorted = WritableUtils.readVInt(in) == 1 ? true : false;
      setCGDeletedFlags(path, conf);
      if (version.compareTo(new Version((short)1, (short)0)) > 0)
      {
        int numSortColumns = WritableUtils.readVInt(in);
        if (numSortColumns > 0)
        {
          String[] sortColumnStr = new String[numSortColumns];
          for (int i = 0; i < numSortColumns; i++)
          {
            sortColumnStr[i] = WritableUtils.readString(in);
          }
          sortInfo = SortInfo.parse(SortInfo.toSortString(sortColumnStr), logical, comparator);
        }
      }
      in.close();
    }

    private static int getNumCGs(Path path, Configuration conf) throws IOException {
      Path pathSchema = makeSchemaFilePath(path);
      if (!path.getFileSystem(conf).exists(pathSchema)) {
        throw new IOException("BT Schema file doesn't exist: " + pathSchema);
      }
      // read schema file
      FSDataInputStream in = path.getFileSystem(conf).open(pathSchema);
      Version version = new Version(in);
      // verify compatibility against SCHEMA_VERSION
      if (!version.compatibleWith(SCHEMA_VERSION)) {
        new IOException("Incompatible versions, expecting: " + SCHEMA_VERSION
            + "; found in file: " + version);
      }
      
      // read comparator
      WritableUtils.readString(in);
      // read logicalStr
      WritableUtils.readString(in);
      // read storage
      WritableUtils.readString(in);
      int numCGs = WritableUtils.readVInt(in);
      in.close();

      return numCGs;
    }

    private static Path makeSchemaFilePath(Path parent) {
      return new Path(parent, BT_SCHEMA_FILE);
    }
    
    /**
     * Sets cgDeletedFlags array by checking presense of
     * ".deleted-CGNAME" directory in the table top level
     * directory. 
     */
    void setCGDeletedFlags(Path path, Configuration conf) throws IOException {
      
      Set<String> deletedCGs = new HashSet<String>(); 
      
      for (FileStatus file : path.getFileSystem(conf).listStatus(path)) {
        if (!file.isDir()) {
          String fname =  file.getPath().getName();
          if (fname.startsWith(DELETED_CG_PREFIX)) {
            deletedCGs.add(fname.substring(DELETED_CG_PREFIX.length()));
          }
        }
      }
      
      for(int i=0; i<physical.length; i++) {
        cgDeletedFlags[i] = deletedCGs.contains(getName(i));
      }
    }
    
    
  }

  static public void dumpInfo(String file, PrintStream out, Configuration conf)
      throws IOException {
    dumpInfo(file, out, conf, 0);
  }

  static public void dumpInfo(String file, PrintStream out, Configuration conf, int indent)
      throws IOException {
    IOutils.indent(out, indent);
    out.println("Basic Table : " + file);
    Path path = new Path(file);
    try {
      BasicTable.Reader reader = new BasicTable.Reader(path, conf);
      String schemaStr = reader.getBTSchemaString();
      String storageStr = reader.getStorageString();
      IOutils.indent(out, indent);
      out.printf("Schema : %s\n", schemaStr);
      IOutils.indent(out, indent);
      out.printf("Storage Information : %s\n", storageStr);
      SortInfo sortInfo = reader.getSortInfo();
      if (sortInfo != null && sortInfo.size() > 0)
      {
        IOutils.indent(out, indent);
        String[] sortedCols = sortInfo.getSortColumnNames();
        out.println("Sorted Columns :");
        for (int nx = 0; nx < sortedCols.length; nx++) {
          if (nx > 0)
            out.printf(" , ");
          out.printf("%s", sortedCols[nx]);
        }
        out.printf("\n");
      }
      IOutils.indent(out, indent);
      out.println("Column Groups within the Basic Table :");
      for (int nx = 0; nx < reader.colGroups.length; nx++) {
        IOutils.indent(out, indent);
        out.printf("\nColumn Group [%d] :", nx);
        if (reader.colGroups[nx] != null) {
          ColumnGroup.dumpInfo(reader.colGroups[nx].path, out, conf, indent);
        } else {
          // print basic info for deleted column groups.
          out.printf("\nColum Group : DELETED");
          out.printf("\nName : %s", reader.schemaFile.getName(nx));
          out.printf("\nSchema : %s\n", 
                     reader.schemaFile.cgschemas[nx].getSchema().toString());
        }
      }
    }
    catch (Exception e) {
      throw new IOException("BasicTable.Reader failed : " + e.getMessage());
    }
    finally {
      // no-op
    }
  }

  public static void main(String[] args) {
    System.out.printf("BasicTable Dumper\n");
    if (args.length == 0) {
      System.out
          .println("Usage: java ... org.apache.hadoop.zebra.io.BasicTable path [path ...]");
      System.exit(0);
    }
    Configuration conf = new Configuration();
    for (String file : args) {
      try {
        dumpInfo(file, System.out, conf);
      }
      catch (IOException e) {
        e.printStackTrace(System.err);
      }
    }
  }
}
