/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package bigframe.avro;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public interface SparkParquetAvro_TG {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"SparkParquetAvro_TG\",\"namespace\":\"bigframe.avro\",\"types\":[{\"type\":\"record\",\"name\":\"Twitter_Graph\",\"fields\":[{\"name\":\"friend_id\",\"type\":\"int\"},{\"name\":\"follower_id\",\"type\":\"int\"}]}],\"messages\":{}}");

  @SuppressWarnings("all")
  public interface Callback extends SparkParquetAvro_TG {
    public static final org.apache.avro.Protocol PROTOCOL = bigframe.avro.SparkParquetAvro_TG.PROTOCOL;
  }
}