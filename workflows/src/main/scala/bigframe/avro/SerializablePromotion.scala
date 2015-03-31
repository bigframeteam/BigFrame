package bigframe.avro

import org.apache.avro.io._
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import java.io.IOException
import java.io.ObjectStreamException
import java.io.Serializable
import java.lang.ClassNotFoundException

//object SerializablePromotion extends Promotion{
//	val SCHEMA = Promotion.SCHEMA$
//}

class SerializablePromotion extends Promotion with Serializable {
	
	def setValues(promo: Promotion) = {
		
		setPPromoSk(promo.getPPromoSk())
		setPPromoId(promo.getPPromoId())
		setPStartDateSk(promo.getPStartDateSk())
		setPEndDateSk(promo.getPEndDateSk())
		setPItemSk(promo.getPItemSk())
		setPCost(promo.getPCost())
		setPResponseTarget(promo.getPResponseTarget())
		setPPromoName(promo.getPPromoName())
		setPChannelDmail(promo.getPChannelDmail())
		setPChannelEmail(promo.getPChannelEmail())
		setPChannelCatalog(promo.getPChannelCatalog())
		setPChannelTv(promo.getPChannelTv())
		setPChannelRadio(promo.getPChannelRadio())
		setPChannelPress(promo.getPChannelPress())
		setPChannelEvent(promo.getPChannelEvent())
		setPChannelDemo(promo.getPChannelDemo())
		setPChannelDetails(promo.getPChannelDetails())
		setPPurpose(promo.getPPurpose())
		setPDiscountActive(promo.getPDiscountActive())
	}
	
	@throws(classOf[IOException])
	private def writeObject(out: java.io.ObjectOutputStream ) = {
//		val loader = Thread.currentThread.getContextClassLoader
//		val data = new SpecificData(loader)
//		val writer = new SpecificDatumWriter[Promotion](Promotion.SCHEMA$, data)
		val writer = new SpecificDatumWriter[Promotion](classOf[Promotion])
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        println("PromoSK In Write:" + getPPromoSk)
        writer.write(this, encoder)
        encoder.flush()
	}
	
	@throws(classOf[IOException])
	@throws(classOf[ClassNotFoundException])
	private def readObject(in: java.io.ObjectInputStream) = {
//		val loader = Thread.currentThread.getContextClassLoader
//		val data = new SpecificData(loader)
//		val reader = new SpecificDatumReader[Promotion](data)
//		reader.setSchema(Promotion.SCHEMA$)
        val reader = new SpecificDatumReader[Promotion](classOf[Promotion])
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        setValues(reader.read(null, decoder))
        println("PromoSK In Read:" + getPPromoSk)
    }
	
	@throws(classOf[ObjectStreamException]) 
	private def readObjectNoData = {
	}
	
}