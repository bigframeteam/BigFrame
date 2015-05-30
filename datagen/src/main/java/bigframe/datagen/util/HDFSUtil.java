/*******************************************************************************
 *    Licensed to the Apache Software Foundation (ASF) under one or more 
 *    contributor license agreements.  See the NOTICE file distributed with 
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with 
 *    the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package bigframe.datagen.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {

  public static void deleteFileOnHDFS(Configuration mapreduce_config,
      String filename) {
    Path hdfs_path = new Path(filename);

    try {
      FileSystem fileSystem = FileSystem.get(mapreduce_config);
      if (fileSystem.exists(hdfs_path)) {
        fileSystem.delete(hdfs_path, true);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void copyFromLocal2HDFS(Configuration mapreduce_config,
      String local, String hdfs) {

    Path localPath = new Path(local);
    Path hdfsPath = new Path(hdfs);
    
    try {
      FileSystem fileSystem = FileSystem.get(mapreduce_config);
      
      fileSystem.copyFromLocalFile(localPath, hdfsPath);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  
}
