package com.cloudera.cam2;

import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


/**
 * package: com.cloudera.cam2
*/

public class SequenceFileTest {

    //HDFS Path
    static String inpath = "/home/cloudera/hbase-example/picHDFS";
    static String outpath = "/home/cloudera/hbase-example/out";
    static SequenceFile.Writer writer = null;
    static HTable htable = null;

    public static void main(String[] args) throws Exception{

        //HBase Configuration
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.clear();
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.setStrings("hbase.zookeeper.quorum", "127.0.0.1");
        
        //Define Table Name -- "picHbase"
        htable = new HTable(hbaseConf,"picHbase");

        //Read Local Disk File Configuration
        Configuration conf = new Configuration();
        URI uri = new URI(inpath);

        //Get a FileSystem instance based on the uri, the passed in configuration and the user.
        FileSystem fileSystem = FileSystem.get(uri, conf,"cloudera");

        //Instantiate Writer Object
        writer = SequenceFile.createWriter(fileSystem, conf, new Path(outpath), Text.class, BytesWritable.class);

        //Recursively Traverse the Folder, Write File Under the Folder to sequenceFile File
        listFileAndWriteToSequenceFile(fileSystem,inpath);
        

        //Close the Writer Stream
        org.apache.hadoop.io.IOUtils.closeStream(writer);


        //Read all the sequenceFile
        URI seqURI = new URI(outpath);
        
        //Get a FileSystem for this URI's scheme and authority.
        FileSystem fileSystemSeq = FileSystem.get(seqURI, conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystemSeq, new Path(outpath), conf);

        Text key = new Text();
        BytesWritable val = new BytesWritable();

        int i = 0;
        while(reader.next(key, val)){
            String temp = key.toString();
            temp = temp.substring(temp.lastIndexOf("/") + 1);
            
            //RowKey Design
            String rowKey = temp;
            System.out.println(rowKey);

            //Specify  ROWKEY Value
            Put put = new Put(Bytes.toBytes(rowKey));

            //Specify Column Family Name, Column Descriptor, Column Value
            put.addColumn("picinfo".getBytes(), "content".getBytes() , val.getBytes());
            htable.put(put);
        }

        htable.close();
        org.apache.hadoop.io.IOUtils.closeStream(reader);
    }


    /****
     * SequenceFile
     * @param fileSystem
     * @param path
     * @throws Exception
     */
    public static void listFileAndWriteToSequenceFile(FileSystem fileSystem,String path) throws Exception{
        //List the statuses of the files/directories in the given path if the path is a directory.
        final FileStatus[] listStatuses = fileSystem.listStatus(new Path(path));

        for (FileStatus fileStatus : listStatuses) {
            if(fileStatus.isFile()){
                Text fileText = new Text(fileStatus.getPath().toString());
                System.out.println(fileText.toString());

                //Return a SequenceFile.Writer Instance, Write Data to Path Object, Need Stream and Path Object
                FSDataInputStream in = fileSystem.open(new Path(fileText.toString()));
                byte[] buffer = IOUtils.toByteArray(in);
                in.read(buffer);
                BytesWritable value = new BytesWritable(buffer);

                //Write to SequenceFile
                writer.append(fileText, value);

            }

            if(fileStatus.isDirectory()){
                listFileAndWriteToSequenceFile(fileSystem,fileStatus.getPath().toString());
            }
        }
    }
}
