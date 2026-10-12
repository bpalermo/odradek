package io.github.bpalermo.odradek.server.admin;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The standard gRPC health service, {@code grpc.health.v1.Health/Check}, answering from {@link
 * Readiness} at the moment of the call.
 *
 * <ul>
 *   <li>service {@code ""}: SERVING when the server can do its work, NOT_SERVING otherwise. For
 *       startup and readiness probes.
 *   <li>service {@code "liveness"}: SERVING while the process runs. For the liveness probe.
 *   <li>any other service: NOT_FOUND, as the protocol asks.
 * </ul>
 *
 * <p>{@code Watch} is not offered; a caller gets UNIMPLEMENTED, which the protocol allows. The two
 * messages are one field each and are written out by hand, so the server needs no further library
 * and the native image no further metadata.
 */
public final class HealthService implements BindableService {
  private static final Logger LOG = LoggerFactory.getLogger(HealthService.class);

  /** The service name a liveness probe asks for. */
  public static final String LIVENESS = "liveness";

  /** {@code HealthCheckResponse.ServingStatus}. */
  public enum ServingStatus {
    UNKNOWN,
    SERVING,
    NOT_SERVING,
    SERVICE_UNKNOWN
  }

  /** {@code Check}: the request is the service name, the response its status. */
  public static final MethodDescriptor<String, ServingStatus> CHECK =
      MethodDescriptor.<String, ServingStatus>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("grpc.health.v1.Health/Check")
          .setRequestMarshaller(new RequestMarshaller())
          .setResponseMarshaller(new ResponseMarshaller())
          .build();

  private final Readiness readiness;
  private final Clock clock;

  /** What the last readiness check found, to log each change once. */
  private List<String> lastProblems;

  public HealthService(Readiness readiness, Clock clock) {
    this.readiness = readiness;
    this.clock = clock;
  }

  @Override
  public ServerServiceDefinition bindService() {
    return ServerServiceDefinition.builder("grpc.health.v1.Health")
        .addMethod(
            CHECK,
            ServerCalls.asyncUnaryCall(
                (service, responses) -> {
                  if (service.equals(LIVENESS)) {
                    responses.onNext(ServingStatus.SERVING);
                    responses.onCompleted();
                  } else if (service.isEmpty()) {
                    responses.onNext(ready() ? ServingStatus.SERVING : ServingStatus.NOT_SERVING);
                    responses.onCompleted();
                  } else {
                    responses.onError(
                        Status.NOT_FOUND
                            .withDescription("unknown service " + service)
                            .asRuntimeException());
                  }
                }))
        .build();
  }

  /** The response carries no reason, so the log does. */
  private synchronized boolean ready() {
    List<String> problems = readiness.problems(clock.instant());
    if (!problems.equals(lastProblems)) {
      lastProblems = problems;
      if (problems.isEmpty()) {
        LOG.info("ready");
      } else {
        LOG.warn("not ready: {}", String.join("; ", problems));
      }
    }
    return problems.isEmpty();
  }

  /** HealthCheckRequest: service = 1 (string). */
  private static final class RequestMarshaller implements MethodDescriptor.Marshaller<String> {
    @Override
    public InputStream stream(String service) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      CodedOutputStream out = CodedOutputStream.newInstance(bytes);
      try {
        if (!service.isEmpty()) {
          out.writeString(1, service);
        }
        out.flush();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      return new ByteArrayInputStream(bytes.toByteArray());
    }

    @Override
    public String parse(InputStream stream) {
      try {
        CodedInputStream in = CodedInputStream.newInstance(stream);
        String service = "";
        for (int tag = in.readTag(); tag != 0; tag = in.readTag()) {
          if (WireFormat.getTagFieldNumber(tag) == 1) {
            service = in.readStringRequireUtf8();
          } else {
            in.skipField(tag);
          }
        }
        return service;
      } catch (IOException e) {
        throw Status.INVALID_ARGUMENT
            .withDescription("malformed HealthCheckRequest")
            .withCause(e)
            .asRuntimeException();
      }
    }
  }

  /** HealthCheckResponse: status = 1 (enum). */
  private static final class ResponseMarshaller
      implements MethodDescriptor.Marshaller<ServingStatus> {
    @Override
    public InputStream stream(ServingStatus status) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      CodedOutputStream out = CodedOutputStream.newInstance(bytes);
      try {
        if (status != ServingStatus.UNKNOWN) {
          out.writeEnum(1, status.ordinal());
        }
        out.flush();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      return new ByteArrayInputStream(bytes.toByteArray());
    }

    @Override
    public ServingStatus parse(InputStream stream) {
      try {
        CodedInputStream in = CodedInputStream.newInstance(stream);
        int status = 0;
        for (int tag = in.readTag(); tag != 0; tag = in.readTag()) {
          if (WireFormat.getTagFieldNumber(tag) == 1) {
            status = in.readEnum();
          } else {
            in.skipField(tag);
          }
        }
        ServingStatus[] values = ServingStatus.values();
        return status >= 0 && status < values.length ? values[status] : ServingStatus.UNKNOWN;
      } catch (IOException e) {
        throw new IllegalArgumentException("malformed HealthCheckResponse", e);
      }
    }
  }
}
