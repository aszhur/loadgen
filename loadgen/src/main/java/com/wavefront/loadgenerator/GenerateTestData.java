package com.wavefront.loadgenerator;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * Generate golden data for a system test. <p> The first parameter should be a yaml config file in
 * src/test/resources/golden. The second should be the path to an output yaml file.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class GenerateTestData {
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static final Logger log = Logger.getLogger(GenerateTestData.class.getCanonicalName());

//  public static void main(String[] args)
//      throws IOException, ClassNotFoundException, ConfigurationException, InterruptedException {
//    Map testConfig =
//        new YamlReader(new FileReader("src/test/resources/golden/" + args[0])).read(Map.class);
//    YamlWriter yamlWriter = new YamlWriter(new FileWriter(args[1]));
//    Integer pointPlayDuration = Integer.parseInt((String) testConfig.get("duration"));
//    Integer maxPoints = Integer.parseInt((String) testConfig.get("maxPoints"));
//    ImmutableMap topLevelConfigMap =
//        new ImmutableMap.Builder<>().put("pointCreator", testConfig.get("pointCreator"))
//            .put("numThreads", 1).put("adapter",
//            new ImmutableMap.Builder().put("type", "TestAdapterConfig")
//                .put("pointsToRemember", maxPoints).build()).build();
//    LoadGeneratorConfig loadGeneratorConfig = LoadGeneratorConfig.fromMap(topLevelConfigMap);
//    LoadGenerator loadGenerator = new LoadGenerator(() -> loadGeneratorConfig);
//    loadGenerator.start();
//    TestAdapter adapter = (TestAdapter) loadGenerator.adapters.get(0);
//
//    System.out.println("Playing load for " + pointPlayDuration + " seconds.");
//    Long start = System.currentTimeMillis();
//    scheduler.scheduleAtFixedRate(() -> {
//      log.info("Played " + adapter.getPoints().size() + " points.");
//      if (adapter.getPoints().size() == maxPoints) {
//        System.err.println("Point overflow!");
//        System.exit(1);
//      } else if (System.currentTimeMillis() - start > pointPlayDuration * 1000) {
//        System.out.println("Finished.");
//        try {
//          yamlWriter.write(ImmutableMap.of("golden",
//              adapter.getPoints().stream().map(x -> x.toString())
//                  .collect(Collectors.toList())));
//          yamlWriter.close();
//        } catch (YamlException e) {
//          log.log(Level.SEVERE, "Bad yaml write", e);
//        }
//        System.exit(0);
//      }
//    }, 0, 5, TimeUnit.SECONDS);
  //}
}
