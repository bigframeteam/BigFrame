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

package bigframe.datagen.nested.tweet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * A class to represent a vertex in our seed graph.
 */
public class SeedVertex {

  private LongWritable id;
  private Text value;
  
  public SeedVertex(LongWritable id, Text value) {
    this.setId(id);
    this.setValue(value);
  }

  /**
   * @return the id
   */
  public LongWritable getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(LongWritable id) {
    this.id = id;
  }

  /**
   * @return the value
   */
  public Text getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(Text value) {
    this.value = value;
  }
  
  
}
