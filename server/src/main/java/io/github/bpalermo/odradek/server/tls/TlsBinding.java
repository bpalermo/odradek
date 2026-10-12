package io.github.bpalermo.odradek.server.tls;

/** What the TLS engine factory of one cluster works from. */
public record TlsBinding(CredentialSource source, PeerPolicy policy) {}
