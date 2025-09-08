package com.wavefront.loadgenerator;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class Util {
  private static ObjectMapper objectMapper = new ObjectMapper();

  public static <T> T fromMap(Map map, Class<T> clazz) throws IOException {
    String json = null;
    try {
      json = objectMapper.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      // Should never happen in practice -- if it does, we will fail fast since this
      // application won't get far without configuration objects.
      throw new RuntimeException(e);
    }
    return objectMapper.readValue(json, clazz);
  }

  public static <T> T fromMap(Map map, String clazzName)
      throws IOException, ClassNotFoundException {
    Class<T> clazz = (Class<T>) Class.forName(clazzName);
    return fromMap(map, clazz);
  }

  public static <T> T fromFile(String filePath, Class<T> clazz) throws IOException {
    // Yaml is nice, but the best config <-> Object library out there is for JSON.
    // So we convert from yaml to JSON and use that library.
    YamlReader yamlReader = new YamlReader(new FileReader(filePath));
    Map map = yamlReader.read(Map.class);
    yamlReader.close();
    return Util.fromMap(map, clazz);
  }
}
