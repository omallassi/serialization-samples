package org.home;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.home.pojo.MyPojo;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;


/**
 * Created by oliv on 23/05/2016.
 */
public class JacksonSerializerTest {

    @Test
    public void testSimpleJsonSerialization() throws IOException {
        JacksonSerializer sut = new JacksonSerializer();

        MyPojo pojo = new MyPojo();
        pojo.setAge(12);

        pojo.setMyName("a name");

        StringWriter writer = new StringWriter();
        sut.serialize(writer, pojo);

        System.out.println("**** write pojo");
        System.out.println(writer.toString());


        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();

        JsonParser parser = factory.createParser(writer.toString());
        parser.nextToken(); // Should be JsonToken.START_OBJECT
        parser.nextToken();
        Assert.assertTrue(parser.getCurrentName().equals("name"));
        parser.nextToken();
        Assert.assertTrue(parser.getText().equals("a name"));

        //json schema generation
        SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
        mapper.acceptJsonFormatVisitor(mapper.constructType(MyPojo.class), visitor);
        JsonSchema jsonSchema = visitor.finalSchema();

        StringWriter schemaWriter = new StringWriter();
        mapper.writeValue(schemaWriter, jsonSchema);

        System.out.println("**** write Json Schema (Jackson) ");
        System.out.println(schemaWriter.toString());


        //avro schema generation
        mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(MyPojo.class, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();

        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String asJson = avroSchema.toString(true);

        System.out.println("**** write Avro Schema (Jackson) ");
        System.out.println(asJson);
        //generate file
        FileWriter fwriter = new FileWriter(".//src//main//avro//generated-pojo.avsc");
        fwriter.write(asJson);
        fwriter.flush();
        fwriter.close();

        //write Avro data
        Schema raw = new Schema.Parser().setValidate(true).parse(asJson);
        AvroSchema schema = new AvroSchema(raw);

        AvroMapper avroMapper = new AvroMapper();
        byte[] avroData = avroMapper.writer(schema).writeValueAsBytes(pojo);

        //read value with jackson
        MyPojo unSerPojo = mapper.reader(MyPojo.class).with(schema).readValue(avroData);

        System.out.println("**** unser Avro Bean (Jackson) ");
        System.out.println(unSerPojo);

        //read value with avro


        SeekableByteArrayInput sin = new SeekableByteArrayInput(avroData);
        DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<GenericRecord>(raw);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);

        System.out.println("**** unser Avro Bean (Avro) ");
        GenericRecord avrPojo = null;
        avrPojo = userDatumReader.read(avrPojo, decoder);
        System.out.println(avrPojo);


        //TODO essayer en générant le code à partir du schema généré.
    }
}