package com.wavefront.loadgenerator.adapter;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.ConfigurationException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FileAdapter extends Adapter {
  private static LoadingCache<String, FileWriter> fileWriterLoadingCache = CacheBuilder.newBuilder()
      .build(new CacheLoader<String, FileWriter>() {
        @Override
        public FileWriter load(String fileName) throws Exception {
          return new FileWriter(new File(fileName));
        }
      });

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      for (Map.Entry<String, FileWriter> entry : fileWriterLoadingCache.asMap().entrySet()) {
        try {
          entry.getValue().flush();
          entry.getValue().close();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }));
  }

  @JsonProperty
  public String outputFile = "";
  // Every FileAdapter has access to a cache of writers, so that concurrent usage of multiple
  // fileAdapters will use the same writer.
  private FileWriter fileWriter;

  @JsonProperty
  public boolean asHistogram = false;

  private PointWriter pointWriter;

  @Override
  public void init() throws Exception {
    super.init();
    ensure(!outputFile.equals(""), "Must give an output file.");
    fileWriter = fileWriterLoadingCache.get(outputFile);
    pointWriter = asHistogram ? PointWriter::asHistogram : PointWriter::asString;
  }

  @Override
  public void sendPointInternal(Point point) throws IOException {
    synchronized (fileWriter) {
      StringBuilder builder = new StringBuilder();
      pointWriter.writePoint(point, builder);
      builder.append("\n");
      fileWriter.write(builder.toString());
    }
  }
}
