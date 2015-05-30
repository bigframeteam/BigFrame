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

import bigframe.datagen.text.tweet.TweetTextGen;

/**
 * A simple distribution to control tweet generation.
 * Including:
 * 1. Which twitter user mention this tweet;
 * 2. If a product is mentioned in the tweet;
 * 3. If a promoted product is mentioned. 
 * 
 * @author andy
 *
 */
public class SimpleTweetGenDist2 extends TweetGenDist {

  /**
   * @param random_seed
   * @param text_gen
   * @param ID
   */
  public SimpleTweetGenDist2(long random_seed, TweetTextGen text_gen, long ID) {
    super(random_seed, text_gen, ID);
    // TODO Auto-generated constructor stub
  }

  /* (non-Javadoc)
   * @see bigframe.datagen.nested.tweet.TweetGenDist#getNextTweet()
   */
  @Override
  public String getNextTweet() {
    // TODO Auto-generated method stub
    return null;
  }

}
