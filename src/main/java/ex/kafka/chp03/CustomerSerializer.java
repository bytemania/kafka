package ex.kafka.chp03;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    /**
     We are serializing Customer as:
     4 byte int representing customerId
     4 byte int representing length of customerName in UTF-8 bytes (0 if
     name is Null)
     N bytes representing customerName in UTF-8
     */
    @Override
    public byte[] serialize(String topic, Customer data) {
        try {
            byte [] serializedName;
            int stringSize;
            if (data == null)
                return null;
            else {
                if (data.getName() != null) {
                    serializedName = data.getName().getBytes(Charset.defaultCharset());
                    stringSize = serializedName.length;
                }
                else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getId());
            buffer.putInt(stringSize);
            buffer.put(serializedName);

            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }

    }

    @Override
    public byte[] serialize(String topic, Headers headers, Customer data) {
        List<ByteBuffer> headersBuffer = new ArrayList<>();

        headers.forEach(header -> {
          byte[] serializedHeaderKey = header.key().getBytes(Charset.defaultCharset());
          byte[] serializedSeparator = ":".getBytes(Charset.defaultCharset());
          ByteBuffer byteBuffer = ByteBuffer.allocate(serializedHeaderKey.length +
                  + serializedSeparator.length +  header.value().length);
          byteBuffer.put(serializedHeaderKey);
          byteBuffer.put(serializedSeparator);

          headersBuffer.add(byteBuffer);
        });

        int size = 0;

        for(var buffer: headersBuffer) {
            size += buffer.array().length;
        }

        byte[] serializedSeparator = "|".getBytes(Charset.defaultCharset());

        byte[] content = serialize(topic, data);

        ByteBuffer buffer = ByteBuffer.allocate(size + serializedSeparator.length + content.length);

        for (var header: headersBuffer) {
            buffer.put(header);
        }
        buffer.put(serializedSeparator);
        buffer.put(content);
        return buffer.array();
    }

    @Override
    public void close() {
    }
}
