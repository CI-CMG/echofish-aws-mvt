package edu.colorado.cires.cmg.echofish.aws.lambda.mvt.mvt;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.colorado.cires.cmg.echofish.aws.lambda.mvt.lambda.MvtEventContext;
import edu.colorado.cires.cmg.mvtset.GeometryDetails;
import edu.colorado.cires.cmg.mvtset.GeometrySource;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoJsonGeometrySource implements GeometrySource, Iterator<GeometryDetails>, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoJsonGeometrySource.class);

  private enum State {
    START,
    PROCESSING_MULTI_LINE_STRING,
    PROCESSING_LINE_STRING,
    PROCESSING_POINT,
    DONE,
    PROCESSING_LINE_STRING_EMIT,
    PROCESSING_LINE_STRING_EMIT_CONTINUE,
    PROCESSING_MULTI_LINE_STRING_EMIT,
    PROCESSING_MULTI_LINE_STRING_EMIT_CONTINUE,
  }


  private final JsonParser jsonParser;
  private final GeometryFactory geometryFactory;
  private final int maxBufferPoints;
  private final MvtEventContext mvtEventContext;
  private List<Coordinate> buffer;

  private State state;

  public GeoJsonGeometrySource(InputStream in, ObjectMapper objectMapper, int maxBufferPoints,
      GeometryFactory geometryFactory, MvtEventContext mvtEventContext) throws IOException {
    buffer = new ArrayList<>(maxBufferPoints);
    this.geometryFactory = geometryFactory;
    jsonParser = objectMapper.createParser(in);
    this.maxBufferPoints = maxBufferPoints;
    this.mvtEventContext = mvtEventContext;
    init();
    state = State.START;
    readBuffer();
  }

  @Override
  public void close() throws IOException {
    jsonParser.close();
  }

  @Override
  public boolean hasNext() {
    return !buffer.isEmpty();
  }

  @Override
  public GeometryDetails next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more GeometryDetails");
    }
    LineString lineString;
    switch (state) {
      case DONE:
        lineString = geometryFactory.createLineString(buffer.toArray(new Coordinate[0]));
        buffer = new ArrayList<>(0);
        break;
      case PROCESSING_LINE_STRING_EMIT:
        lineString = geometryFactory.createLineString(buffer.toArray(new Coordinate[0]));
        state = State.PROCESSING_LINE_STRING_EMIT_CONTINUE;
        readBuffer();
        break;
      case PROCESSING_MULTI_LINE_STRING_EMIT:
        lineString = geometryFactory.createLineString(buffer.toArray(new Coordinate[0]));
        state = State.PROCESSING_MULTI_LINE_STRING_EMIT_CONTINUE;
        readBuffer();
        break;
      default:
        throw new IllegalStateException("Unsupported state " + state);
    }
    return new GeometryDetails(lineString, new HashMap<>(){{
      put("cruise_name", mvtEventContext.getSurvey());
    }});
  }


  @Override
  public Stream<GeometryDetails> streamGeometries() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED), false)
        .onClose(() -> {
          try {
            close();
          } catch (IOException e) {
            LOGGER.warn("Unable to close iterator", e);
          }
        });
  }

  private void init() throws IOException {
    if (jsonParser.nextToken() == JsonToken.START_OBJECT) {
      jsonParser.nextToken();
      if (!jsonParser.getCurrentName().equals("type")) {
        throw new IllegalStateException("GeoJson type not defined first");
      }
      jsonParser.nextToken();
      if (!jsonParser.getText().equals("Feature")) {
        throw new IllegalStateException("GeoJson is not a Feature");
      }
      jsonParser.nextToken();
      if (!jsonParser.getCurrentName().equals("geometry")) {
        throw new IllegalStateException("geometry not defined");
      }
      if (jsonParser.nextToken() != JsonToken.START_OBJECT) {
        throw new IllegalStateException("geometry is not an object");
      }
      jsonParser.nextToken();
      if (!jsonParser.getCurrentName().equals("type")) {
        throw new IllegalStateException("geometry type not defined");
      }
      jsonParser.nextToken();
      String type = jsonParser.getText();
      if (type.equals("MultiLineString")) {
        jsonParser.nextToken();
        if (!jsonParser.getCurrentName().equals("coordinates")) {
          throw new IllegalStateException("coordinates not defined");
        }
      } else {
        throw new IllegalStateException("Unsupported type: " + type);
      }

    } else {
      throw new IllegalStateException("Invalid JSON, not an object");
    }
  }

  private void readBuffer() {
    while (state != State.DONE && state != State.PROCESSING_LINE_STRING_EMIT && state != State.PROCESSING_MULTI_LINE_STRING_EMIT) {
      processLineStrings(jsonParser);
    }
  }


  private void processLineStrings(JsonParser jsonParser) {
    try {
      switch (state) {
        case START: {
          if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("not a multilinestring");
          }
          state = State.PROCESSING_MULTI_LINE_STRING;
        }
        break;
        case PROCESSING_MULTI_LINE_STRING_EMIT_CONTINUE: {
          buffer = new ArrayList<>(maxBufferPoints);
          state = State.PROCESSING_LINE_STRING;
        }
        break;
        case PROCESSING_MULTI_LINE_STRING: {
          jsonParser.nextToken();
          if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
            if (!buffer.isEmpty()) {
              state = State.PROCESSING_MULTI_LINE_STRING_EMIT;
            } else {
              state = State.PROCESSING_LINE_STRING;
            }
          } else if (jsonParser.currentToken() == JsonToken.END_ARRAY) {
            state = State.DONE;
          } else {
            throw new IllegalStateException("invalid token" + jsonParser.currentToken());
          }
        }
        break;
        case PROCESSING_LINE_STRING_EMIT_CONTINUE: {
          buffer = new ArrayList<>(buffer.subList(buffer.size() - 1, buffer.size()));
          state = State.PROCESSING_POINT;
        }
        break;
        case PROCESSING_LINE_STRING: {
          jsonParser.nextToken();
          if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
            if (buffer.size() == maxBufferPoints) {
              state = State.PROCESSING_LINE_STRING_EMIT;
            } else {
              state = State.PROCESSING_POINT;
            }
          } else if (jsonParser.currentToken() == JsonToken.END_ARRAY) {
            state = State.PROCESSING_MULTI_LINE_STRING;
          } else {
            throw new IllegalStateException("invalid token" + jsonParser.currentToken());
          }
        }
        break;
        case PROCESSING_POINT: {
          ArrayNode pointArray = jsonParser.readValueAsTree();
          buffer.add(new Coordinate(pointArray.get(0).asDouble(), pointArray.get(1).asDouble()));
          state = State.PROCESSING_LINE_STRING;
        }
        break;
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to parse JSON", e);
    }
  }
}
