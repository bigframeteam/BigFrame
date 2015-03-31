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

class SerializableUser_Mention(user_mention: User_Mention) extends User_Mention 
	with Serializable {
	
	this.setValues(user_mention)
	
	private def setValues(user_mention: User_Mention) = {
		setId(user_mention.getId())
		setIdStr(user_mention.getIdStr())
		setIndices(user_mention.getIndices())
		setName(user_mention.getName())
		setScreenName(user_mention.getScreenName())
	}
	
	@throws(classOf[IOException])
	private def writeObject(out: java.io.ObjectOutputStream ) = {
		val writer = new SpecificDatumWriter[User_Mention](classOf[User_Mention])
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(this, encoder)
        encoder.flush()
	}
	
	@throws(classOf[IOException])
	@throws(classOf[ClassNotFoundException])
	private def readObject(in: java.io.ObjectInputStream) = {
        val reader = new SpecificDatumReader[User_Mention](classOf[User_Mention])
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        setValues(reader.read(null, decoder))
    }
	
	@throws(classOf[ObjectStreamException]) 
	private def readObjectNoData = {}
}