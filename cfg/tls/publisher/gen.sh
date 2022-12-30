#! /bin/bash

# generate the private key if it doesn't exist
if ! test -f private.key; then
  openssl genrsa -out private.key 4096
fi

# generate a certificate signing request that will be signed by the architect CA
openssl req -new -key ./private.key -sha512 -out request \
  -subj "/CN=publisher.example.com/C=US/ST=State/L=City/O=example"

# sign the certificate request with the CA key and add restrictions to it using x509v3 extentions
openssl x509 -req -in ./request -CA ../ca/certificate -CAkey ../ca/private.key \
  -CAcreateserial -out certificate -days 730 -extfile <(cat <<EOF
basicConstraints=critical, CA:FALSE
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always, issuer:always
keyUsage=nonRepudiation,digitalSignature,keyEncipherment
subjectAltName=DNS:publisher.example.com
EOF
)

 # check it
 openssl verify -trusted ../ca/certificate ./certificate
 