package com.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 展示HDFS的Java api常用方法
 */
public class AppTest {

    // FIXME 修改namenode地址
    private static final String HDFS_URL = "hdfs://node1:8020";
    // FIXME 修改username
    private static final String USERNAME = "yancai";

    private static final String PATH_LOCAL = "/home/yancai/demo-hdfs/files";

    private static final String DIR_UPLOAD = "upload";
    private static final String PATH_UPLOAD = "/user/" + USERNAME + "/" + DIR_UPLOAD;

    private static FileSystem fs;

    static {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URL);
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String toDateString(Long timestamp) {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(timestamp));
    }

    /**
     * listStatus 查询指定路径下的文件或目录
     *
     * @throws IOException
     */
    @Test
    public void test_01_listStatus() throws IOException {
        FileStatus[] files = fs.listStatus(new Path(HDFS_URL + "/"));
        for (FileStatus f : files) {
            System.out.println(
                f.getPermission()
                    + "\t" + f.getOwner() + " " + f.getGroup()
                    + "\t" + f.getBlockSize()
                    + "\t" + toDateString(f.getModificationTime())
                    + "\t" + f.getPath()
            );
        }
    }

    /**
     * exists 判断文件是否存在
     * mkdirs 创建文件夹
     * delete 删除文件夹
     *
     * @throws IOException
     */
    @Test
    public void test_02_exists_mkdirs_delete() throws IOException {
        // 创建path，如果存在则先删除
        boolean isExists = fs.exists(new Path(PATH_UPLOAD));
        if (isExists) {
            boolean result = fs.delete(new Path(PATH_UPLOAD), true);
            Assert.assertTrue(result);
            result = fs.exists(new Path(PATH_UPLOAD));
            Assert.assertFalse(result);
        }

        // 创建path，默认创建在用户目录下
        fs.mkdirs(new Path(DIR_UPLOAD));
        boolean result = fs.exists(new Path(PATH_UPLOAD));
        Assert.assertTrue(result);
    }

    /**
     * create   创建文件
     * open     打开文件
     * @throws IOException
     */
    @Test
    public void test_03_create() throws IOException {
        String localFile = PATH_LOCAL + "/file_to_upload.txt";
        String hdfsFile = PATH_UPLOAD + "/test.txt";

        if (fs.exists(new Path(hdfsFile))) {
            fs.delete(new Path(hdfsFile), true);
        }

        InputStream in = new FileInputStream(localFile);
        OutputStream out = fs.create(new Path(hdfsFile));
        IOUtils.copyBytes(in, out, 4096, true);

        InputStream hdfsStream = fs.open(new Path(hdfsFile));
        InputStream fileStream = new FileInputStream(localFile);
        String hdfsStr = org.apache.commons.io.IOUtils.toString(hdfsStream);
        String fileStr = org.apache.commons.io.IOUtils.toString(fileStream);

        Assert.assertEquals(hdfsStr, fileStr);
    }


    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(HDFS_URL + "/user/" + USERNAME), true);
        while (iterator.hasNext()) {
            LocatedFileStatus f = iterator.next();
            System.out.println(
                f.getPermission()
                    + "\t" + f.getOwner() + " " + f.getGroup()
                    + "\t" + f.getBlockSize()
                    + "\t" + toDateString(f.getModificationTime())
                    + "\t" + f.getPath()
            );
        }
    }


}
