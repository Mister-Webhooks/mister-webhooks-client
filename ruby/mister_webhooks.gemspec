Gem::Specification.new do |s|
  s.name = "mister_webhooks"
  s.version = "0.0.0"
  s.summary = "Mister Webhooks Ruby SDK"
  s.description = "Ruby library for consuming Mister Webhooks events"
  s.authors = ["Jesse Kempf"]
  s.email = "jesse@mister-webhooks.com"
  s.files = ["lib/mister_webhooks.rb"]
  s.license = "BSD-2-Clause"
  s.add_dependency "rdkafka", "~> 0.22.2"
  s.add_dependency "avro", "~> 1.12.0"
  s.add_dependency "cbor", "~> 0.5.10.1"
  s.add_dependency "deep-freeze", "~> 1.0", ">= 1.0.1"
end
