package bigframe.avro

import org.apache.avro.io._
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import java.io.IOException
import java.io.ObjectStreamException
import java.io.Serializable
import java.io.IOException
import java.io.ObjectStreamException
import java.lang.ClassNotFoundException

class SerializableTweet(tweet: Tweet) extends Tweet with Serializable {

	this.setValues(tweet)
	
	private def setValues(tweet: Tweet) = {
		setContributors(tweet.getContributors())
		setCoordinates(tweet.getCoordinates())
		setCreatedAt(tweet.getCreatedAt())
		setEntities(new SerializableEntities(tweet.getEntities()))
		setFavorited(tweet.getFavorited())
		setGeo(tweet.getGeo())
		setId(tweet.getId())
		setIdStr(tweet.getIdStr())
		setInReplyToScreenName(tweet.getInReplyToScreenName())
		setInReplyToStatusId(tweet.getInReplyToStatusId())
		setInReplyToStatusIdStr(tweet.getInReplyToStatusIdStr())
		setInReplyToUserId(tweet.getInReplyToUserId())
		setInReplyToUserIdStr(tweet.getInReplyToUserIdStr())
		setPlace(tweet.getPlace())
		setUser(new SerializableUser(tweet.getUser()))
		setTruncated(tweet.getTruncated())
		setText(tweet.getText())
		setSource(tweet.getSource())
		setRetweeted(tweet.getRetweeted())
		setRetweetCount(tweet.getRetweetCount())
	}
	
	@throws(classOf[IOException])
	private def writeObject(out: java.io.ObjectOutputStream ) = {
		val writer = new SpecificDatumWriter[Tweet](classOf[Tweet])
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(this, encoder)
        encoder.flush()
	}
	
	@throws(classOf[IOException])
	@throws(classOf[ClassNotFoundException])
	private def readObject(in: java.io.ObjectInputStream) = {
        val reader = new SpecificDatumReader[Tweet](classOf[Tweet])
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        setValues(reader.read(null, decoder))
    }
	
	@throws(classOf[ObjectStreamException]) 
	private def readObjectNoData = {}
}