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

package io.github.spark_redshift_community.spark.redshift;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;


/**
 * A stub implementation of NativeFileSystemStore for testing
 * S3AFileSystem without actually connecting to S3.
 */
public class InMemoryS3AFileSystem extends FileSystem {
    public static final String BUCKET = "test-bucket";
    public static final URI FS_URI = URI.create("s3a://" + BUCKET + "/");

    private static final long DEFAULT_BLOCK_SIZE_TEST = 33554432;

    private final Path root = new Path(FS_URI.toString());

    private SortedMap<String, ByteArrayOutputStream> dataMap = new TreeMap<String, ByteArrayOutputStream>();

    private Configuration conf;

    @Override
    public URI getUri() {
        return FS_URI;
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(root, "work");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        // Not implemented
        return false;
    }

    @Override
    public void initialize(URI name, Configuration originalConf)
            throws IOException {
        conf = originalConf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public boolean exists(Path f) throws IOException {

        SortedMap<String, ByteArrayOutputStream> subMap = dataMap.tailMap(toS3Key(f));
        for (String filePath: subMap.keySet()) {
            if (filePath.contains(toS3Key(f))) {
                return true;
            }
        }
        return false;
    }

    private String toS3Key(Path f) {
        return f.toString();
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        if (getFileStatus(f).isDirectory())
            throw new IOException("TESTING: path can't be opened - it's a directory");

        return new FSDataInputStream(
            new SeekableByteArrayInputStream(
                dataMap.get(toS3Key(f)).toByteArray()
            )
        );
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return open(f);
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {

        if (exists(f)) {
            throw new FileAlreadyExistsException();
        }

        String key = toS3Key(f);
        ByteArrayOutputStream inMemoryS3File = new ByteArrayOutputStream();

        dataMap.put(key, inMemoryS3File);

        return new FSDataOutputStream(inMemoryS3File);

    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        // Not Implemented
        return null;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        // Not Implemented
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        dataMap.put(toS3Key(dst), dataMap.get(toS3Key(src)));
        return true;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        dataMap.remove(toS3Key(f));
        return true;
    }

    private Set<String> childPaths(Path f) {
        Set<String> children = new HashSet<>();

        String fDir = f + "/";
        for (String subKey: dataMap.tailMap(toS3Key(f)).keySet()){
            children.add(
                fDir + subKey.replace(fDir, "").split("/")[0]
            );
        }
        return children;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {

        if (!exists(f)) throw new FileNotFoundException();

        if (getFileStatus(f).isDirectory()){
            ArrayList<FileStatus> statuses = new ArrayList<>();

            for (String child: childPaths(f)) {
                statuses.add(getFileStatus(new Path(child)));
            }

            FileStatus[] arrayStatuses = new FileStatus[statuses.size()];
            return statuses.toArray(arrayStatuses);
        }

        else {
            FileStatus[] statuses = new FileStatus[1];
            statuses[0] = this.getFileStatus(f);
            return statuses;
        }
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        // Not implemented
    }

    private boolean isDir(Path f) throws IOException{
        return exists(f) && dataMap.get(toS3Key(f)) == null;
    }


    @Override
    public  S3AFileStatus getFileStatus(Path f) throws IOException {

        if (!exists(f)) throw new FileNotFoundException();

        if (isDir(f)) {
            return new S3AFileStatus(
                true,
                dataMap.tailMap(toS3Key(f)).size() == 1 && dataMap.containsKey(toS3Key(f)),
                f
            );
        }
        else {
            return new S3AFileStatus(
                dataMap.get(toS3Key(f)).toByteArray().length,
                System.currentTimeMillis(),
                f,
                this.getDefaultBlockSize()
            );
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public long getDefaultBlockSize() {
        return DEFAULT_BLOCK_SIZE_TEST;
    }
}