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

import scala.collection.JavaConversions._

class SerializableEntities(entities: Entities) extends Entities
	with Serializable {
	
	this.setValues(entities)
	
	private def setValues(entities: Entities) = {
		setHashtags(entities.getHashtags())
		
		var urls = List[SerializableUrl]()
		entities.getUrls().foreach(url => urls = (new SerializableUrl(url)) :: urls)
		setUrls(urls)
		
		var user_mentions = List[SerializableUser_Mention]()
		entities.getUserMentions().foreach(user_mention => user_mentions = (
				new SerializableUser_Mention(user_mention)) :: user_mentions)
		setUserMentions(user_mentions)
	}
	
	@throws(classOf[IOException])
	private def writeObject(out: java.io.ObjectOutputStream ) = {
		val writer = new SpecificDatumWriter[Entities](classOf[Entities])
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(this, encoder)
        encoder.flush()
	}
	
	@throws(classOf[IOException])
	@throws(classOf[ClassNotFoundException])
	private def readObject(in: java.io.ObjectInputStream) = {
        val reader = new SpecificDatumReader[Entities](classOf[Entities])
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        setValues(reader.read(null, decoder))
    }
	
	@throws(classOf[ObjectStreamException]) 
	private def readObjectNoData = {}	

}