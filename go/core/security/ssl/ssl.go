/*
	Copyright 2021 SANGFOR TECHNOLOGIES

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package ssl

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"gitee.com/opengauss/ham4db/go/core/log"
	"github.com/howeyc/gopass"

	//"gitee.com/opengauss/ham4db/go/core/log"
	"io/ioutil"
	nethttp "net/http"
)

var cipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_RSA_WITH_AES_256_CBC_SHA,
}

// NewTLSConfig returns an initialized TLS configuration suitable for client
// authentication. If caFile is non-empty, it will be loaded.
func NewTLSConfig(caFile string, verifyCert bool) (*tls.Config, error) {
	var c tls.Config

	// Set to TLS 1.2 as a minimum.  This is overridden for mysql communication
	c.MinVersion = tls.VersionTLS12
	// Remove insecure ciphers from the list
	c.CipherSuites = cipherSuites
	c.PreferServerCipherSuites = true

	if verifyCert {
		log.Info("verifyCert requested, client certificates will be verified")
		c.ClientAuth = tls.VerifyClientCertIfGiven
	}
	caPool, err := ReadCAFile(caFile)
	if err != nil {
		return &c, err
	}
	c.ClientCAs = caPool
	c.BuildNameToCertificate()
	return &c, nil
}

// Returns CA certificate. If caFile is non-empty, it will be loaded.
func ReadCAFile(caFile string) (*x509.CertPool, error) {
	var caCertPool *x509.CertPool
	if caFile != "" {
		data, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool = x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(data) {
			return nil, log.Errorf("No certificates parsed")
		}
	}
	return caCertPool, nil
}

// AppendKeyPair loads the given TLS key pair and appends it to
// tlsConfig.Certificates.
func AppendKeyPair(tlsConfig *tls.Config, certFile string, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	return nil
}

// AppendKeyPairWithPassword read in a keypair where the key is password protected
func AppendKeyPairWithPassword(tlsConfig *tls.Config, certFile string, keyFile string, pemPass []byte) error {

	// Certificates aren't usually password protected, but we're kicking the password
	// along just in case.  It won't be used if the file isn't encrypted
	certData, err := ReadPEMData(certFile, pemPass)
	if err != nil {
		return err
	}
	keyData, err := ReadPEMData(keyFile, pemPass)
	if err != nil {
		return err
	}
	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return err
	}
	tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	return nil
}

// ReadPEMData read a PEM file and ask for a password to decrypt it if needed
func ReadPEMData(pemFile string, pemPass []byte) ([]byte, error) {
	pemData, err := ioutil.ReadFile(pemFile)
	if err != nil {
		return pemData, err
	}

	// We should really just get the pem.Block back here, if there's other
	// junk on the end, warn about it.
	pemBlock, rest := pem.Decode(pemData)
	if len(rest) > 0 {
		//log.Warning("Didn't parse all of", pemFile)
	}

	if x509.IsEncryptedPEMBlock(pemBlock) {

		// Decrypt and get the ASN.1 DER bytes here
		pemData, err = x509.DecryptPEMBlock(pemBlock, pemPass)
		if err != nil {
			return pemData, err
		}

		// Shove the decrypted DER bytes into a new pem Block with blank headers
		var newBlock pem.Block
		newBlock.Type = pemBlock.Type
		newBlock.Bytes = pemData
		// This is now like reading in an uncrypted key from a file and stuffing it
		// into a byte stream
		pemData = pem.EncodeToMemory(&newBlock)
	}
	return pemData, nil
}

// GetPEMPassword print a password prompt on the terminal and collect a password
func GetPEMPassword(pemFile string) []byte {
	fmt.Printf("Password for %s: ", pemFile)
	pass, err := gopass.GetPasswd()
	if err != nil {
		// We'll error with an incorrect password at DecryptPEMBlock
		return []byte("")
	}
	return pass
}

// IsEncryptedPEM determine if PEM file is encrypted
func IsEncryptedPEM(pemFile string) bool {
	pemData, err := ioutil.ReadFile(pemFile)
	if err != nil {
		return false
	}
	pemBlock, _ := pem.Decode(pemData)
	if len(pemBlock.Bytes) == 0 {
		return false
	}
	return x509.IsEncryptedPEMBlock(pemBlock)
}

// ListenAndServeTLS acts identically to http.ListenAndServeTLS, except that it expects TLS configuration.
// TODO: refactor so this is testable?
func ListenAndServeTLS(addr string, handler nethttp.Handler, tlsConfig *tls.Config) error {

	// On unix Listen calls getaddrinfo to parse the port, so named ports are fine as long
	// as they exist in /etc/services
	if addr == "" {
		addr = ":https"
	}

	// listen with tls config
	l, err := tls.Listen("tcp", addr, tlsConfig)
	if err != nil {
		return err
	}
	return nethttp.Serve(l, handler)
}
