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

class SerializableUrl(url: Url) extends Url with Serializable {
	
	this.setValues(url)
	
	private def setValues(url: Url) = {
		setExpandedUrl(url.getExpandedUrl())
		setIndices(url.getIndices())
		setUrl(url.getUrl())
	}
	
	@throws(classOf[IOException])
	private def writeObject(out: java.io.ObjectOutputStream ) = {
		val writer = new SpecificDatumWriter[Url](classOf[Url])
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(this, encoder)
        encoder.flush()
	}
	
	@throws(classOf[IOException])
	@throws(classOf[ClassNotFoundException])
	def readObject(in: java.io.ObjectInputStream) = {
        val reader = new SpecificDatumReader[Url](classOf[Url])
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        setValues(reader.read(null, decoder))
    }
	
	@throws(classOf[ObjectStreamException]) 
	def readObjectNoData = {}
}