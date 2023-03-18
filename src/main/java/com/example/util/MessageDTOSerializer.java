package com.example.util;


import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.example.dto.MessageDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MessageDTOSerializer implements Serializer<MessageDTO> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, MessageDTO data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            System.out.println("Unable to serialize object");
            return null;
        }
    }

    @Override
    public void close() {
    }

}
