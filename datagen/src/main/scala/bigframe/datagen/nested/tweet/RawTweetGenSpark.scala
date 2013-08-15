package bigframe.datagen.nested.tweet

import bigframe.bigif.BigDataInputFormat

class RawTweetGenSpark (conf: BigDataInputFormat, targetGB: Float ) extends 
RawTweetGen (conf: BigDataInputFormat, targetGB: Float) {
  
  def generate(): Unit = {}

  def getAbsSizeBySF(sf: Int): Int = { 0 }

  def getSFbyAbsSize(absSize: Int): Int = { 0 }

}