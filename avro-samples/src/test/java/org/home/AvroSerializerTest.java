package org.home;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.home.pojo.MyPojo;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;


/**
 * Created by oliv on 23/05/2016.
 */
public class AvroSerializerTest {

    @Test
    public void testSimpleAvroSerialization() throws IOException {
//        User user1 = new User();
//        user1.setName("Alyssa");
//        user1.setFavoriteNumber(256);
//
//        User user2 = new User("Ben", 7, "red");

// Construct via builder
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        File file = new File("users.avro");

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user3.getSchema(), file);
//        dataFileWriter.append(user1);
//        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();


        // Deserialize Users from disk
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println("SpecificDatumReader " + user);
        }


        userDatumWriter = new ReflectDatumWriter<User>(User.class);
        dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user3.getSchema(), file);
//        dataFileWriter.append(user1);
//        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();


        // Deserialize Users from disk
        userDatumReader = new SpecificDatumReader<User>(User.class);
        dataFileReader = new DataFileReader<User>(file, userDatumReader);
        user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println("ReflectDatumWriter " + user);
        }


        //write in json
        Schema schema = ReflectData.get().getSchema(User.class);

        userDatumWriter = new GenericDatumWriter<User>(schema);

        OutputStream os = new ByteArrayOutputStream();

        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, System.out);
        userDatumWriter.write(user, encoder);
        encoder.flush();

        System.out.println();
    }

}
