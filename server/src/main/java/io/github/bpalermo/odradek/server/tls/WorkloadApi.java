package io.github.bpalermo.odradek.server.tls;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The one call of the SPIFFE Workload API the server makes, {@code FetchX509SVID}, and the wire
 * format of its answer.
 *
 * <p>Written out by hand instead of generated from the Workload API's .proto: it is one empty
 * request and three small messages, and this way the server needs no SPIFFE library and the native
 * image no extra metadata.
 */
final class WorkloadApi {
  /** Every request must carry this header, or the agent refuses it. */
  static final Metadata.Key<String> SECURITY_HEADER =
      Metadata.Key.of("workload.spiffe.io", Metadata.ASCII_STRING_MARSHALLER);

  /** One X.509-SVID with the bundle of its trust domain. All fields are DER. */
  record Svid(String spiffeId, ByteString certificates, ByteString key, ByteString bundle) {}

  /** An update: the workload's SVIDs and the bundles of the trust domains federated with. */
  record Response(List<Svid> svids, Map<String, ByteString> federatedBundles) {}

  static final MethodDescriptor<Void, Response> FETCH_X509_SVID =
      MethodDescriptor.<Void, Response>newBuilder()
          .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName("SpiffeWorkloadAPI/FetchX509SVID")
          .setRequestMarshaller(new EmptyMarshaller())
          .setResponseMarshaller(new ResponseMarshaller())
          .build();

  private WorkloadApi() {}

  /** X509SVIDRequest has no fields. */
  private static final class EmptyMarshaller implements MethodDescriptor.Marshaller<Void> {
    @Override
    public InputStream stream(Void value) {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public Void parse(InputStream stream) {
      return null;
    }
  }

  private static final class ResponseMarshaller implements MethodDescriptor.Marshaller<Response> {
    @Override
    public InputStream stream(Response value) {
      throw new UnsupportedOperationException("the server only reads responses");
    }

    @Override
    public Response parse(InputStream stream) {
      try {
        return parseResponse(CodedInputStream.newInstance(stream));
      } catch (IOException e) {
        throw new IllegalArgumentException("malformed X509SVIDResponse", e);
      }
    }
  }

  /** X509SVIDResponse: svids = 1 (repeated message), federated_bundles = 3 (map). */
  static Response parseResponse(CodedInputStream in) throws IOException {
    List<Svid> svids = new ArrayList<>();
    Map<String, ByteString> federated = new LinkedHashMap<>();
    for (int tag = in.readTag(); tag != 0; tag = in.readTag()) {
      int field = WireFormat.getTagFieldNumber(tag);
      if (field == 1 && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        svids.add(parseSvid(in.readBytes().newCodedInput()));
      } else if (field == 3
          && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
        CodedInputStream entry = in.readBytes().newCodedInput();
        String domain = "";
        ByteString bundle = ByteString.EMPTY;
        for (int entryTag = entry.readTag(); entryTag != 0; entryTag = entry.readTag()) {
          switch (WireFormat.getTagFieldNumber(entryTag)) {
            case 1 -> domain = entry.readStringRequireUtf8();
            case 2 -> bundle = entry.readBytes();
            default -> entry.skipField(entryTag);
          }
        }
        federated.put(domain, bundle);
      } else {
        in.skipField(tag);
      }
    }
    return new Response(svids, federated);
  }

  /** X509SVID: spiffe_id = 1, x509_svid = 2, x509_svid_key = 3, bundle = 4. */
  private static Svid parseSvid(CodedInputStream in) throws IOException {
    String id = "";
    ByteString certificates = ByteString.EMPTY;
    ByteString key = ByteString.EMPTY;
    ByteString bundle = ByteString.EMPTY;
    for (int tag = in.readTag(); tag != 0; tag = in.readTag()) {
      switch (WireFormat.getTagFieldNumber(tag)) {
        case 1 -> id = in.readStringRequireUtf8();
        case 2 -> certificates = in.readBytes();
        case 3 -> key = in.readBytes();
        case 4 -> bundle = in.readBytes();
        default -> in.skipField(tag);
      }
    }
    return new Svid(id, certificates, key, bundle);
  }
}
