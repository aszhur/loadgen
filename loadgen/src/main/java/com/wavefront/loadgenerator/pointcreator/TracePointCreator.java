package com.wavefront.loadgenerator.pointcreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.loadgenerator.adapter.Point;
import javafx.util.Pair;
import net.jcip.annotations.ThreadSafe;

import java.util.*;

/**
 * TracePointCreator extends CartesianPointCreator with "traceID" and "spandID" tags
 * YAML file properties are set for the super class before calling super.init()
 *
 * @author Sky Liu (skyl@vmware.com)
 */
@ThreadSafe
public class TracePointCreator extends CartesianPointCreator {
    @JsonProperty
    public Integer
            intervalSeconds = 0,
            numMetrics = 0,
            numHosts = 0,
            numTags = 0,
            metricsWeight = 0,
            hostsWeight = 0,
            spanIdsWeight = 1,
            tagsWeight = 0,
            randomPerHostTagIntervalSeconds = 0,
            randomPerHostTagSpace = 100;

    @JsonProperty
    public String constantTag = null;
    @JsonProperty
    public String additionalTagFromHost = null;
    @JsonProperty
    public boolean newTelemetryPerPoint = false;
    @JsonProperty
    public boolean omitTimestamp = false;
    private List<Pair<UUID, UUID>> traceSpanIdList;

    @Override
    public void init() throws Exception {
        super.intervalSeconds = intervalSeconds;
        super.numMetrics = numMetrics;
        super.numHosts = numHosts;
        super.numTags = numTags;
        super.metricsWeight = metricsWeight;
        super.hostsWeight = hostsWeight;
        super.tagsWeight = tagsWeight;
        super.randomPerHostTagIntervalSeconds = randomPerHostTagIntervalSeconds;
        super.randomPerHostTagSpace = randomPerHostTagSpace;
        super.constantTag = constantTag;
        super.additionalTagFromHost = additionalTagFromHost;
        super.newTelemetryPerPoint = newTelemetryPerPoint;
        super.omitTimestamp = omitTimestamp;

        this.traceSpanIdList = new ArrayList<>();
        createTraceSpanIdList(this.traceSpanIdList);

        super.init();
    }

    @Override
    public synchronized void nextPoint(Point point) {
        if(traceSpanIdList.size() == 0){
            createTraceSpanIdList(traceSpanIdList);
        }
        Pair<UUID, UUID> traceSpanIdPair = traceSpanIdList.remove(0);
        point.tags.add("traceId=" + traceSpanIdPair.getKey().toString());
        point.tags.add("spanId=" + traceSpanIdPair.getValue().toString());
        super.getPoint(point);
    }

    @Override
    public synchronized void nextPointList(List<Point> points) {
        Point point = new Point();
        if(traceSpanIdList.size() == 0){
            createTraceSpanIdList(traceSpanIdList);
        }
        Pair<UUID, UUID> traceSpanIdPair = traceSpanIdList.remove(0);
        point.tags.add("traceId=" + traceSpanIdPair.getKey().toString());
        point.tags.add("spanId=" + traceSpanIdPair.getValue().toString());
        super.getPoint(point);
        points.add(point);
    }

    private void createTraceSpanIdList(List<Pair<UUID, UUID>> traceSpanIdList){
        traceSpanIdList.clear();
        UUID traceId = UUID.randomUUID();
        for (int j = 1; j <= spanIdsWeight; j++) {
            traceSpanIdList.add(new Pair<>(traceId, UUID.randomUUID()));
        }
    }

}
