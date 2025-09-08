package com.wavefront.loadgenerator.adapter;

import java.io.IOException;

/**
 * Sends a sample as a wavefront tracing-span.
 *
 * @author Sky Liu (skyl@vmware.com).
 */
public class WavefrontTraceAdapter extends SocketBasedAdapter {

//  Wavefront tracing-span data format:
//      <span_name> <source> <traceId> <spanId> <other span tags> <datetime_ms> <duration_ms>
//
//  Example:
//      "getAllUsers source=localhost traceId=7b3bf470-9456-11e8-9eb6-529269fb1459
//      spanId=0313bafe-9457-11e8-9eb6-529269fb1459 application=Wavefront service=eso
//      http.method=GET 1533531013000 343500"
//
//  ts(dataingester.report-spans) can be used to check ingested span count

    @Override
    protected void sendPointInternal(Point point) throws IOException {
        write(point.name);
        write(" ");
        for (String tag : point.tags) {
            write(tag);
            write(" ");
        }
        write(Long.toString(point.timestamp));
        write(" ");
        write(Double.toString(point.value));
        write("\n");
    }
}
