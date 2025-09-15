package org.gbif.pipelines.interpretation.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

public class KryoUtils {
  private static final ThreadLocal<Kryo> kryoThreadLocal =
      ThreadLocal.withInitial(
          () -> {
            Kryo kryo = new Kryo();
            kryo.register(ExtendedRecord.class);
            kryo.register(LocationRecord.class);
            kryo.register(TemporalRecord.class);
            kryo.register(MultiTaxonRecord.class);
            kryo.register(TaxonRecord.class);
            kryo.register(ParsedName.class);
            kryo.register(GrscicollRecord.class);
            kryo.register(IdentifierRecord.class);
            kryo.register(IssueRecord.class);
            kryo.register(RankedNameWithAuthorship.class);
            kryo.register(RankedName.class);
            kryo.register(Nomenclature.class);
            kryo.register(Diagnostic.class);
            kryo.register(MatchType.class);
            kryo.register(java.util.ArrayList.class);
            kryo.register(java.util.HashMap.class);
            kryo.register(BasicRecord.class);
            kryo.register(EventDate.class);
            kryo.register(GeologicalContext.class);
            kryo.register(ClusteringRecord.class);
            kryo.register(MultimediaRecord.class);
            kryo.register(DnaDerivedDataRecord.class);
            kryo.register(GadmFeatures.class);
            kryo.register(Status.class);
            kryo.register(ArrayList.class);
            return kryo;
          });

  public static byte[] serialize(Object obj) {
    Kryo kryo = kryoThreadLocal.get();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeClassAndObject(output, obj);
    output.close();
    return baos.toByteArray();
  }

  public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
    Kryo kryo = kryoThreadLocal.get();
    Input input = new Input(new ByteArrayInputStream(bytes));
    return clazz.cast(kryo.readClassAndObject(input));
  }
}
