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

class SerializableUser(user: User) extends User with Serializable{

	this.setValues(user)
	
	private def setValues(user: User) = {
		setContributorsEnabled(user.getContributorsEnabled())
		setCreatedAt(user.getCreatedAt())
		setDescription(user.getDescription())
		setFavouritesCount(user.getFavouritesCount())
		setFollowersCount(user.getFollowersCount())
		setFollowing(user.getFollowing())
		setFollowRequestSent(user.getFollowRequestSent())
		setFriendsCount(user.getFriendsCount())
		setGeoEnabled(user.getGeoEnabled())
		setId(user.getId())
		setIdStr(user.getIdStr())
		setLang(user.getLang())
		setListedCount(user.getListedCount())
		setLocation(user.getLocation())
		setName(user.getName())
		setNotifications(user.getNotifications())
		setProfileBackgroundColor(user.getProfileBackgroundColor())
		setProfileBackgroundImageUrl(user.getProfileBackgroundImageUrl())
		setProfileBackgroundTile(user.getProfileBackgroundTile())
		setProfileImageUrl(user.getProfileImageUrl())
		setShowAllInlineMedia(user.getShowAllInlineMedia())
		setProfileLinkColor(user.getProfileLinkColor())
		setProfileSidebarBorderColor(user.getProfileSidebarBorderColor())
		setProfileSidebarFillColor(user.getProfileSidebarFillColor())
		setProfileTextColor(user.getProfileTextColor())
		setVerified(user.getVerified())
		setUtcOffset(user.getUtcOffset())
		setUrl(user.getUrl())
		setTimeZone(user.getTimeZone())
		setStatusesCount(user.getStatusesCount())
		setScreenName(user.getScreenName())
		setProfileUseBackgroundImage(user.getProfileUseBackgroundImage())
		setProtected$(user.getProtected$())
	}
	
	@throws(classOf[IOException])
	private def writeObject(out: java.io.ObjectOutputStream ) = {
		val writer = new SpecificDatumWriter[User](classOf[User])
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(this, encoder)
        encoder.flush()
	}
	
	@throws(classOf[IOException])
	@throws(classOf[ClassNotFoundException])
	private def readObject(in: java.io.ObjectInputStream) = {
        val reader = new SpecificDatumReader[User](classOf[User])
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        setValues(reader.read(null, decoder))
    }
	
	@throws(classOf[ObjectStreamException]) 
	private def readObjectNoData = {}
	
}