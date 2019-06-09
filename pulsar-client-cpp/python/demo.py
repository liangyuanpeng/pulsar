import pulsar

service_url = 'pulsar+ssl://useast1.gcp.kafkaesque.io:6651'

# Use default CA certs for your environment
# RHEL/CentOS:
#trust_certs='/etc/ssl/certs/ca-bundle.crt'
# Debian/Ubuntu:
# trust_certs='/etc/ssl/certs/ca-certificates.crt'
# OSX:
# Export the default certificates to a file, then use that file:
#    security find-certificate -a -p /System/Library/Keychains/SystemCACertificates.keychain > ./ca-certificates.crt
trust_certs='./ca-certificates.crt'

token='eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJsYW4tNWNjMWRhNDlkYmFiMiJ9.CmLlavJLCHq_SdBVsct7b4qGHSqFx8Lc-7_RUblF_tAKq5FXtTJCvbssLmOPGSX8ajF99kAfCElpY7aE6XuT5XSnG1DCWUKAA0MCOZZVaO-sxWvEDGJX13cPmsi30cESCa9VBfXn59nKOgSyn4pGrsjVp7Ii1z-8zJ0hZ-MTUgKfZGqHEAibqMtoS_-GBbTbzpmKsVa5hm7a-lUO1iey8H6SKt3pCUqd8VmF19esiWa9zB5SiHDg2b6EdpB-PBBDRyMDssJPUSL9gRrgKr2SAhrDmhK9VZEiDQwxoXjyq7L63jCUNkImb5RbPuEcKomyg4sREamwr9xFjUQp3lW-mg'

client = pulsar.Client(service_url,
                         authentication=pulsar.AuthenticationToken(token),
                         tls_trust_certs_file_path=trust_certs)
#client = pulsar.Client(service_url,
#                        authentication=pulsar.AuthenticationToken(token))


producer = client.create_producer('persistent://lan/local-useast1-gcp/test-topic')

for i in range(10):
    producer.send(('Hello World! %d' % i).encode('utf-8'))

client.close()

