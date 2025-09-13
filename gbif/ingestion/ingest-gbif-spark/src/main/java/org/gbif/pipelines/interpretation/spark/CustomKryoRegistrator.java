package org.gbif.pipelines.interpretation.spark;

import com.esotericsoftware.kryo.Kryo;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

public class CustomKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
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
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
    }
}
