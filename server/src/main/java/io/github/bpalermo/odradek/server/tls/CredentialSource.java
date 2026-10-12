package io.github.bpalermo.odradek.server.tls;

/** Holds the TLS material of the moment. Every new handshake asks for it. */
public interface CredentialSource {
  /** The current set. Never null: a source exists only once it has loaded a first one. */
  Credentials current();
}
