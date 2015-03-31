package bigframe.avro

import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecord}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, BinaryEncoder, EncoderFactory}
import org.apache.spark.serializer.KryoRegistrator

class AvroSerializer[T <: SpecificRecord : ClassManifest] extends Serializer[T] {
  val reader = new SpecificDatumReader[T](classManifest[T].erasure.asInstanceOf[Class[T]])
  val writer = new SpecificDatumWriter[T](classManifest[T].erasure.asInstanceOf[Class[T]])
  var encoder = null.asInstanceOf[BinaryEncoder]
  var decoder = null.asInstanceOf[BinaryDecoder]
 
  setAcceptsNull(false)
 
  def write(kryo: Kryo, output: Output, record: T) = {
    encoder = EncoderFactory.get().directBinaryEncoder(output, encoder)
    writer.write(record, encoder)
  }
 
  def read(kryo: Kryo, input: Input, klazz: Class[T]): T = this.synchronized {
    decoder = DecoderFactory.get().directBinaryDecoder(input, decoder)
    reader.read(null.asInstanceOf[T], decoder)
  }
}
 
class BigFrameKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Promotion], new AvroSerializer[Promotion]())
    kryo.register(classOf[SerializablePromotion], new AvroSerializer[SerializablePromotion]())
  }
}