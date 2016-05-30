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
import example.avro.User;
import example.avro.another.AnotherUser;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.home.pojo.JacksonUser;
import org.home.pojo.MyPojo;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;


/**
 * Created by oliv on 23/05/2016.
 */
public class JacksonSerializerTest {

    @Test
    public void testJacksonJsonSerDeser() throws IOException {
        ObjectMapper serializer = new ObjectMapper();


        MyPojo pojo = getMyPojo();

        StringWriter writer = new StringWriter();
        serializer.writeValue(writer, pojo);

        System.out.println("**** write pojo");
        System.out.println(writer.toString());

        Assert.assertEquals("{\"name\":\"a name\",\"age\":12}", writer.toString());

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();

        JsonParser parser = factory.createParser(writer.toString());
        parser.nextToken(); // Should be JsonToken.START_OBJECT
        parser.nextToken();
        Assert.assertTrue(parser.getCurrentName().equals("name"));
        parser.nextToken();
        Assert.assertTrue(parser.getText().equals("a name"));

    }

    @Test(expected = ValidationException.class)
    public void testJacksonJsonSchemaGenerationAndJsonSchemaValidation() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        //json schema generation
        SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
        mapper.acceptJsonFormatVisitor(mapper.constructType(MyPojo.class), visitor);
        JsonSchema jsonSchema = visitor.finalSchema();

        StringWriter schemaWriter = new StringWriter();
        mapper.writeValue(schemaWriter, jsonSchema);

        System.out.println("**** write Json Schema (Jackson) ");
        System.out.println(schemaWriter.toString());

        String expectedJsonSchema = "{\"type\":\"object\",\"id\":\"urn:jsonschema:org:home:pojo:MyPojo\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}}}";

        Assert.assertEquals(expectedJsonSchema, schemaWriter.toString());


        //validate the object using the everit.org lib
        JSONObject rawSchema = new JSONObject(new JSONTokener(new StringReader(expectedJsonSchema)));
        org.everit.json.schema.Schema schema = SchemaLoader.load(rawSchema);
        schema.validate(new JSONObject(getMyPojo()));

        String wrongJsonSchema = "{\"type\":\"object\",\"id\":\"urn:jsonschema:org:home:pojo:MyPojo\"," +
                "\"properties\":{\"name\":{\"type\":\"integer\"},\"age\":{\"type\":\"integer\"}}}";
        rawSchema = new JSONObject(new JSONTokener(new StringReader(wrongJsonSchema)));
        schema = SchemaLoader.load(rawSchema);
        schema.validate(new JSONObject(getMyPojo()));
    }

    @Test
    public void testJacksonAvroSchemaGeneration() throws IOException {
        //avro schema generation
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(MyPojo.class, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();

        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String asJson = avroSchema.toString(true);

        System.out.println("**** write Avro Schema (Jackson) ");
        System.out.println(asJson);
        //generate file
        String expectedAvroSchema = "{" +
                "  \"type\" : \"record\"," +
                "  \"name\" : \"MyPojo\"," +
                "  \"namespace\" : \"org.home.pojo\"," +
                "  \"doc\" : \"Schema for org.home.pojo.MyPojo\"," +
                "  \"fields\" : [ {" +
                "    \"name\" : \"age\"," +
                "    \"type\" : \"int\"" +
                "  }, {" +
                "    \"name\" : \"name\"," +
                "    \"type\" : [ \"null\", \"string\" ]" +
                "  } ]" +
                "}";


        Assert.assertEquals(expectedAvroSchema, asJson.replaceAll("(\\r|\\n)", ""));

    }

    @Test
    public void testJacksonAvroSerDeser() throws IOException {
        String jsonSchema = "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"MyPojo\",\n" +
                "  \"namespace\" : \"org.home.pojo\",\n" +
                "  \"doc\" : \"Schema for org.home.pojo.MyPojo\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"age\",\n" +
                "    \"type\" : \"int\"\n" +
                "  }, {\n" +
                "    \"name\" : \"name\",\n" +
                "    \"type\" : [ \"null\", \"string\" ]\n" +
                "  } ]\n" +
                "}";
        //write Avro data w/ jackson
        Schema raw = new Schema.Parser().setValidate(true).parse(jsonSchema);
        AvroSchema schema = new AvroSchema(raw);
        AvroMapper avroMapper = new AvroMapper();

        MyPojo pojo = getMyPojo();
        byte[] avroData = avroMapper.writer(schema).writeValueAsBytes(pojo);

        //persist the data on  disk
        FileOutputStream fWriter = new FileOutputStream(System.getProperty("java.io.tmpdir") + "user.avro");
        avroMapper.writer(schema).writeValue(fWriter, pojo);

        fWriter.flush();
        fWriter.close();
        System.out.println("Has written file to [" + System.getProperty("java.io.tmpdir") + "user.avro" + "]");

        //read Avro data with jackson
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        MyPojo unSerPojo = mapper.reader(MyPojo.class).with(schema).readValue(avroData);

        System.out.println("**** unser Avro Bean (Jackson) ");
        System.out.println(unSerPojo);

        Assert.assertEquals(pojo, unSerPojo);
    }

    @Test
    public void testJacksonAvroSerAndDeserWithAvroAsGenericRecord() throws IOException {
        String jsonSchema = "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"MyPojo\",\n" +
                "  \"namespace\" : \"org.home.pojo\",\n" +
                "  \"doc\" : \"Schema for org.home.pojo.MyPojo\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"age\",\n" +
                "    \"type\" : \"int\"\n" +
                "  }, {\n" +
                "    \"name\" : \"name\",\n" +
                "    \"type\" : [ \"null\", \"string\" ]\n" +  //union
                "  } ]\n" +
                "}";

        //write Avro data w/ jackson
        Schema raw = new Schema.Parser().setValidate(true).parse(jsonSchema);
        AvroSchema schema = new AvroSchema(raw);
        AvroMapper avroMapper = new AvroMapper();

        MyPojo pojo = getMyPojo();
        byte[] avroData = avroMapper.writer(schema).writeValueAsBytes(pojo);

        //read "jackson written" Avro data with avro
        SeekableByteArrayInput sin = new SeekableByteArrayInput(avroData);
        DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<GenericRecord>(raw);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);

        System.out.println("**** unser Avro Bean (Avro) - as a GenericRecord ");
        GenericRecord avrPojo = null;
        avrPojo = userDatumReader.read(avrPojo, decoder);
        System.out.println(avrPojo);

        Assert.assertEquals(pojo.getName(), ((org.apache.avro.util.Utf8) avrPojo.get("name")).toString());
        Assert.assertEquals(pojo.getAge(), avrPojo.get("age"));
    }

    @Test
    public void testJacksonAvroSerAndDeserWithAvroAsObject() throws IOException {
        String avroSchema = "{\"namespace\": \"example.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"User\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
                "    {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";

        //write Avro data w/ jackson
        Schema raw = new Schema.Parser().setValidate(true).parse(avroSchema);
        AvroSchema schema = new AvroSchema(raw);
        AvroMapper avroMapper = new AvroMapper();
        schema = avroMapper.schemaFrom(avroSchema);

        JacksonUser user = getJacksonUserObject();
        byte[] avroData = avroMapper.writer(schema).writeValueAsBytes(user);

        //read "jackson written" Avro data with avro
        SeekableByteArrayInput sin = new SeekableByteArrayInput(avroData);
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(raw);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);

        System.out.println("**** unser Avro Bean (Avro) - as an Object ");
        User avrPojo = null; //User is an avro object
        avrPojo = userDatumReader.read(avrPojo, decoder);
        System.out.println(avrPojo);

        Assert.assertEquals("this is my name", ((org.apache.avro.util.Utf8) avrPojo.getName()).toString());
        Assert.assertEquals("faavorite Color", ((org.apache.avro.util.Utf8) avrPojo.getFavoriteColor()).toString());
    }

    @Test
    public void testJacksonDeserJsonGeneratedFromAvroObject() throws IOException {
        User avroUser = getAvroUserObject();
        //write in json
        Schema schema = ReflectData.get().getSchema(User.class);
        DatumWriter<User> userDatumWriter = new GenericDatumWriter<User>(schema);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, stream);
        userDatumWriter.write(avroUser, encoder);
        encoder.flush();

        String expectedObjectAsJson = "{\"name\":\"Charlie\",\"favorite_number\":null,\"favorite_color\":\"blue\"}";
        String avroGeneratedJson = stream.toString();
        Assert.assertEquals(expectedObjectAsJson, avroGeneratedJson);

        //try to read it from jackson
        ObjectMapper mapper = new ObjectMapper();
        JacksonUser jacksonUser = mapper.readValue(avroGeneratedJson, JacksonUser.class);
        Assert.assertNotNull(jacksonUser);
        Assert.assertEquals("blue", jacksonUser.getFavoriteColor());

        jacksonUser = mapper.readValue("{\"name\":\"Charlie\",\"favorite_number\":null,\"favorite_color\":null}", JacksonUser.class);
        Assert.assertNotNull(jacksonUser);
        Assert.assertNull(jacksonUser.getFavoriteColor());

        jacksonUser = mapper.readValue("{\"name\":\"Charlie\",\"favorite_number\":null}", JacksonUser.class);
        Assert.assertNotNull(jacksonUser);
        Assert.assertNull(jacksonUser.getFavoriteColor());
    }

    @Test
    public void testJacksonAvroDeserFromAvroSerFile() throws IOException {
        AvroMapper mapper = new AvroMapper();
        JacksonUser jacksonBean = getJacksonUserObject();
        String avroSchemaAsString = "{\"namespace\": \"example.avro.another\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"AnotherUser\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"]},\n" +
                "    {\"name\": \"favoriteColor\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";

        Schema raw = new Schema.Parser().setValidate(true).parse(avroSchemaAsString);
        AvroSchema schema = new AvroSchema(raw);

        DatumWriter<JacksonUser> datumWriter = new ReflectDatumWriter<JacksonUser>(schema.getAvroSchema());
        DataFileWriter<JacksonUser> dataFileWriter = new DataFileWriter<JacksonUser>(datumWriter);

        dataFileWriter.create(schema.getAvroSchema(), new File("users.avro"));
        //write the object three times
        dataFileWriter.append(jacksonBean);
        dataFileWriter.append(jacksonBean);
        dataFileWriter.append(jacksonBean);
        dataFileWriter.close();

        DatumReader<AnotherUser> userDatumReader = new SpecificDatumReader<AnotherUser>(AnotherUser.getClassSchema());
        DataFileReader<AnotherUser> dataFileReader = new DataFileReader<AnotherUser>(new File("users.avro"), userDatumReader);

        AnotherUser user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);
            Assert.assertEquals("this is my name", ((org.apache.avro.util.Utf8) user.getName()).toString());
            Assert.assertEquals("faavorite Color", ((org.apache.avro.util.Utf8) user.getFavoriteColor()).toString());
        }
    }

    private MyPojo getMyPojo() {
        MyPojo pojo = new MyPojo();
        pojo.setAge(12);
        pojo.setName("a name");
        return pojo;
    }

    private User getAvroUserObject() {
        return User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();
    }

    private AnotherUser getAvroAnotherUserObject() {
        return AnotherUser.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();
    }

    private JacksonUser getJacksonUserObject() {
        JacksonUser user = new JacksonUser();
        user.setFavoriteColor("faavorite Color");
        user.setFavoriteNumber(132);
        user.setName("this is my name");

        return user;
    }
}