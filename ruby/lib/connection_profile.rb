require "json"

module MisterWebhooks
  # Representation of the contents of a Mister Webhooks connection profile,
  # needed to start a {Consumer}.
  class ConnectionProfile
    # Load a {ConnectionProfile} from a file on the filesystem.
    # @param [String] path the path to the file.
    def self.from_file(path)
      File.open(path) do |file|
        json = JSON.parse(file.read)

        ConnectionProfile.new(
          json["consumer_name"],
          json["auth"]["mechanism"],
          json["auth"]["secret"],
          json["kafka"]["bootstrap"]
        )
      end
    end

    # Render a configuration suitable to create an +Rdkafka::Config+ from.
    def config
      {
        "bootstrap.servers": @bootstrap,
        "group.id": @consumer_name,
        "auto.offset.reset": "earliest",
        "sasl.mechanism": @auth_mechanism.upcase,
        "sasl.username": @consumer_name,
        "sasl.password": @auth_secret,
        "security.protocol": "SASL_SSL",
        "ssl.ca.pem": <<~SSL_CERT
          -----BEGIN CERTIFICATE-----
          MIICuDCCAmqgAwIBAgIURKmZE5o9LPqEQpU6yahiP+TLwpAwBQYDK2VwMIHHMQsw
          CQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZy
          YW5jaXNjbzEYMBYGA1UECgwPTWlzdGVyIFdlYmhvb2tzMRQwEgYDVQQLDAtFbmdp
          bmVlcmluZzErMCkGA1UEAwwiS2Fma2EgQnJva2VyIENlcnRpZmljYXRlIEF1dGhv
          cml0eTEuMCwGCSqGSIb3DQEJARYfZW5naW5lZXJpbmdAbWlzdGVyLXdlYmhvb2tz
          LmNvbTAeFw0yNTA1MjIwNDA0NTZaFw0zNTA1MjAwNDA0NTZaMIHHMQswCQYDVQQG
          EwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNj
          bzEYMBYGA1UECgwPTWlzdGVyIFdlYmhvb2tzMRQwEgYDVQQLDAtFbmdpbmVlcmlu
          ZzErMCkGA1UEAwwiS2Fma2EgQnJva2VyIENlcnRpZmljYXRlIEF1dGhvcml0eTEu
          MCwGCSqGSIb3DQEJARYfZW5naW5lZXJpbmdAbWlzdGVyLXdlYmhvb2tzLmNvbTAq
          MAUGAytlcAMhAE4/M7Qj1+KNtqGdGF7DgAtO+elzPGDHlyCLz1VCvwi+o2YwZDAd
          BgNVHQ4EFgQUVVOr9w+0L3obSHwAx/3DKG+iKOMwHwYDVR0jBBgwFoAUVVOr9w+0
          L3obSHwAx/3DKG+iKOMwEgYDVR0TAQH/BAgwBgEB/wIBATAOBgNVHQ8BAf8EBAMC
          AQYwBQYDK2VwA0EAZlSOhxGZrIK/gUwB6tOKK3S0gvD7a+SoEEkAYVF44AnwvMe0
          5qzICSe+0sFaqLT0CNf2JQo/PSK06e9Lb7zNCw==
          -----END CERTIFICATE-----
        SSL_CERT
      }
    end

    private

    def initialize(consumer_name, auth_mechanism, auth_secret, bootstrap)
      @consumer_name = consumer_name
      @auth_mechanism = auth_mechanism
      @auth_secret = auth_secret
      @bootstrap = bootstrap
    end
  end
end
