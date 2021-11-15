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
package http

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"gitee.com/opengauss/ham4db/go/config"
	nethttp "net/http"
	"testing"
)

func TestStatus(t *testing.T) {
	var validOUs []string
	url := fmt.Sprintf("http://example.com%s", config.Config.StatusEndpoint)

	req, err := nethttp.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	config.Config.StatusOUVerify = false
	if err := Verify(req, validOUs); err != nil {
		t.Errorf("Failed even with verification off")
	}
	config.Config.StatusOUVerify = true
	if err := Verify(req, validOUs); err == nil {
		t.Errorf("Did not fail on with bad verification")
	}
}

func TestVerify(t *testing.T) {
	var validOUs []string

	req, err := nethttp.NewRequest("GET", "http://example.com/foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := Verify(req, validOUs); err == nil {
		t.Errorf("Did not fail on lack of TLS config")
	}

	pemBlock, _ := pem.Decode([]byte(pemCertificate))
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	var tcs tls.ConnectionState
	req.TLS = &tcs

	if err := Verify(req, validOUs); err == nil {
		t.Errorf("Found a valid OU without any being available")
	}

	// Set a fake OU
	cert.Subject.OrganizationalUnit = []string{"testing"}

	// Pretend our request had a certificate
	req.TLS.PeerCertificates = []*x509.Certificate{cert}
	req.TLS.VerifiedChains = [][]*x509.Certificate{req.TLS.PeerCertificates}

	// Look for fake OU
	validOUs = []string{"testing"}

	if err := Verify(req, validOUs); err != nil {
		t.Errorf("Failed to verify certificate OU")
	}
}

const pemCertificate = `-----BEGIN CERTIFICATE-----
MIIDtTCCAp2gAwIBAgIJAOxKC7FsJelrMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTcwODEwMTQ0MjM3WhcNMTgwODEwMTQ0MjM3WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEA12vHV3gYy5zd1lujA7prEhCSkAszE6E37mViWhLQ63CuedZfyYaTAHQK
HYDZi4K1MNAySUfZRMcICSSsxlRIz6mzXrFsowaJgwx4cbMDIvXE03KstuXoTYJh
+xmXB+5yEVEtIyP2DvPqfCmwCZb3k94Y/VY1nAQDxIxciXrAxT9zT1oYd0YWr2yp
J2mgsfnY4c3zg7W5WgvOTmYz7Ey7GJjpUjGdayx+P1CilKzSWH1xZuVQFNLSHvcH
WXkEoCMVc0tW5mO5eEO1aNHo9MSjPF386l1rq+pz5OwjqCEZq2b1YxesyLnbF+8+
iYGfYmFaDLFwG7zVDwialuI4TzIIOQIDAQABo4GnMIGkMB0GA1UdDgQWBBQ1ubGx
Yvn3wN5VXyoR0lOD7ARzVTB1BgNVHSMEbjBsgBQ1ubGxYvn3wN5VXyoR0lOD7ARz
VaFJpEcwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNV
BAoTGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZIIJAOxKC7FsJelrMAwGA1UdEwQF
MAMBAf8wDQYJKoZIhvcNAQEFBQADggEBALmm4Zw/4jLKDJciUGUYOcr5Xe9TP/Cs
afH7IWvaFUDfV3W6yAm9jgNfIy9aDLpuu2CdEb+0qL2hdmGLV7IM3y62Ve0UTdGV
BGsm1zMmIguew2wGbAwGr5LmIcUseatVUKAAAfDrBNwotEAdM8kmGekUZfOM+J9D
FoNQ62C0buRHGugtu6zWAcZNOe6CI7HdhaAdxZlgn8y7dfJQMacoK0NcWeUVQwii
6D4mgaqUGM2O+WcquD1vEMuBPYVcKhi43019E0+6LI5QB6w80bARY8K7tkTdRD7U
y1/C7iIqyuBVL45OdSabb37TfGlHZIPIwLaGw3i4Mr0+F0jQT8rZtTQ=
-----END CERTIFICATE-----`
