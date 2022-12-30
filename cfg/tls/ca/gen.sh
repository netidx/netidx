#! /bin/bash

# generate the private key if it doesn't exist
if ! test -f private.key; then
  openssl genrsa -out private.key 4096
fi

# generate a self signed certificate for the root authority
openssl req -new -key ./private.key -x509 -sha512 -out certificate -days 7300 \
  -subj "/CN=example.com/C=US/ST=State/L=City/O=example" \
  -addext "basicConstraints=critical, CA:TRUE" \
  -addext "subjectKeyIdentifier=hash" \
  -addext "authorityKeyIdentifier=keyid:always, issuer:always" \
  -addext "keyUsage=critical, cRLSign, digitalSignature, keyCertSign" \
  -addext "subjectAltName=DNS:example.com"
