#!/usr/bin/env bash
#
# Generate self-signed test certificates for nhttp SSL tests.
#
# Usage: ./gen_test_certs.sh [output_dir]
#
# If output_dir is not specified, certificates are generated in the same
# directory as this script.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR}"

# Certificate configuration
DAYS=3650  # 10 years for test certs
KEY_SIZE=2048
COMMON_NAME="localhost"
ORGANIZATION="nhttp Test"

echo "Generating test certificates in: $OUTPUT_DIR"

# Generate CA key and certificate (for future use with verify_peer)
echo "Generating CA certificate..."
openssl req -x509 -newkey "rsa:$KEY_SIZE" \
    -keyout "$OUTPUT_DIR/ca.key" \
    -out "$OUTPUT_DIR/ca.pem" \
    -days "$DAYS" \
    -nodes \
    -subj "/O=$ORGANIZATION/CN=nhttp Test CA" \
    2>/dev/null

# Generate server key
echo "Generating server key..."
openssl genrsa -out "$OUTPUT_DIR/server.key" "$KEY_SIZE" 2>/dev/null

# Generate server CSR
echo "Generating server CSR..."
openssl req -new \
    -key "$OUTPUT_DIR/server.key" \
    -out "$OUTPUT_DIR/server.csr" \
    -subj "/O=$ORGANIZATION/CN=$COMMON_NAME" \
    2>/dev/null

# Create extension file for SAN (Subject Alternative Name)
cat > "$OUTPUT_DIR/server_ext.cnf" << EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature, keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1=localhost
DNS.2=*.localhost
IP.1=127.0.0.1
IP.2=::1
EOF

# Sign server certificate with CA
echo "Signing server certificate..."
openssl x509 -req \
    -in "$OUTPUT_DIR/server.csr" \
    -CA "$OUTPUT_DIR/ca.pem" \
    -CAkey "$OUTPUT_DIR/ca.key" \
    -CAcreateserial \
    -out "$OUTPUT_DIR/server.pem" \
    -days "$DAYS" \
    -extfile "$OUTPUT_DIR/server_ext.cnf" \
    2>/dev/null

# Generate client key (for mutual TLS tests)
echo "Generating client key..."
openssl genrsa -out "$OUTPUT_DIR/client.key" "$KEY_SIZE" 2>/dev/null

# Generate client CSR
openssl req -new \
    -key "$OUTPUT_DIR/client.key" \
    -out "$OUTPUT_DIR/client.csr" \
    -subj "/O=$ORGANIZATION/CN=nhttp Test Client" \
    2>/dev/null

# Create extension file for client cert
cat > "$OUTPUT_DIR/client_ext.cnf" << EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth
EOF

# Sign client certificate with CA
echo "Signing client certificate..."
openssl x509 -req \
    -in "$OUTPUT_DIR/client.csr" \
    -CA "$OUTPUT_DIR/ca.pem" \
    -CAkey "$OUTPUT_DIR/ca.key" \
    -CAcreateserial \
    -out "$OUTPUT_DIR/client.pem" \
    -days "$DAYS" \
    -extfile "$OUTPUT_DIR/client_ext.cnf" \
    2>/dev/null

# Clean up temporary files
rm -f "$OUTPUT_DIR"/*.csr "$OUTPUT_DIR"/*.cnf "$OUTPUT_DIR"/*.srl

# Verify certificates
echo ""
echo "Verifying certificates..."
openssl verify -CAfile "$OUTPUT_DIR/ca.pem" "$OUTPUT_DIR/server.pem"
openssl verify -CAfile "$OUTPUT_DIR/ca.pem" "$OUTPUT_DIR/client.pem"

echo ""
echo "Generated files:"
ls -la "$OUTPUT_DIR"/*.pem "$OUTPUT_DIR"/*.key 2>/dev/null || true

echo ""
echo "Certificate details (server):"
openssl x509 -in "$OUTPUT_DIR/server.pem" -noout -subject -issuer -dates

echo ""
echo "Done! Test certificates generated successfully."
