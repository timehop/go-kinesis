package kinesis

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	AccessEnvKey       = "AWS_ACCESS_KEY"
	AccessEnvKeyId     = "AWS_ACCESS_KEY_ID"
	SecretEnvKey       = "AWS_SECRET_KEY"
	SecretEnvAccessKey = "AWS_SECRET_ACCESS_KEY"

	AWSMetadataServer = "169.254.169.254"
	AWSIAMCredsPath   = "/latest/meta-data/iam/security-credentials"
	AWSIAMCredsURL    = "http://" + AWSMetadataServer + "/" + AWSIAMCredsPath
	AWSTokenURL       = "http://" + AWSMetadataServer + "/latest/api/token"
	AWSTokenTTL       = "21600" // 6 hours
)

// Auth interface for authentication credentials and information
type Auth interface {
	GetToken() string
	GetExpiration() time.Time
	GetSecretKey() string
	GetAccessKey() string
	HasExpiration() bool
	Renew() error
	Sign(*Service, time.Time) []byte
}

// AuthCredentials holds the AWS credentials and metadata
type AuthCredentials struct {
	// accessKey, secretKey are the standard AWS auth credentials
	accessKey, secretKey, token string

	// expiry indicates the time at which these credentials expire. If this is set
	// to anything other than the zero value, indicates that the credentials are
	// temporary (and probably fetched from an IAM role from the metadata server)
	expiry time.Time
}

// NewAuth creates a *AuthCredentials struct that adheres to the Auth interface to
// dynamically retrieve AWS credentials
func NewAuth(accessKey, secretKey string) *AuthCredentials {
	return &AuthCredentials{
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

// NewAuthFromEnv retrieves auth credentials from environment vars
func NewAuthFromEnv() (*AuthCredentials, error) {
	accessKey := os.Getenv(AccessEnvKey)
	if accessKey == "" {
		accessKey = os.Getenv(AccessEnvKeyId)
	}

	secretKey := os.Getenv(SecretEnvKey)
	if secretKey == "" {
		secretKey = os.Getenv(SecretEnvAccessKey)
	}

	if accessKey == "" {
		return nil, fmt.Errorf("unable to retrieve access key from %s or %s env variables", AccessEnvKey, AccessEnvKeyId)
	}
	if secretKey == "" {
		return nil, fmt.Errorf("unable to retrieve secret key from %s or %s env variables", SecretEnvKey, SecretEnvAccessKey)
	}

	return NewAuth(accessKey, secretKey), nil
}

// NewAuthFromMetadata retrieves auth credentials from the metadata
// server. If an IAM role is associated with the instance we are running on, the
// metadata server will expose credentials for that role under a known endpoint.
//
// TODO: specify custom network (connect, read) timeouts, else this will block
// for the default timeout durations.
func NewAuthFromMetadata() (*AuthCredentials, error) {
	auth := &AuthCredentials{}
	if err := auth.Renew(); err != nil {
		return nil, err
	}
	return auth, nil
}

// HasExpiration returns true if the expiration time is non-zero and false otherwise
func (a *AuthCredentials) HasExpiration() bool {
	return !a.expiry.IsZero()
}

// GetExpiration retrieves the current expiration time
func (a *AuthCredentials) GetExpiration() time.Time {
	return a.expiry
}

// GetToken returns the token
func (a *AuthCredentials) GetToken() string {
	return a.token
}

// GetSecretKey returns the secret key
func (a *AuthCredentials) GetSecretKey() string {
	return a.secretKey
}

// GetAccessKey returns the access key
func (a *AuthCredentials) GetAccessKey() string {
	return a.accessKey
}

// Renew retrieves a new token and mutates it on an instance of the Auth struct.
// Uses IMDSv2 (session-based) to fetch credentials from the EC2 metadata service.
func (a *AuthCredentials) Renew() error {
	// Get IMDSv2 session token
	imdsToken, err := getIMDSv2Token()
	if err != nil {
		return fmt.Errorf("failed to get IMDSv2 token: %w", err)
	}

	role, err := retrieveIAMRole(imdsToken)
	if err != nil {
		return err
	}

	data, err := retrieveAWSCredentials(role, imdsToken)
	if err != nil {
		return err
	}

	// Ignore the error, it just means we won't be able to refresh the
	// credentials when they expire.
	expiry, _ := time.Parse(time.RFC3339, data["Expiration"])

	a.expiry = expiry
	a.accessKey = data["AccessKeyId"]
	a.secretKey = data["SecretAccessKey"]
	a.token = data["Token"]
	return nil
}

// Sign API request by
// http://docs.amazonwebservices.com/general/latest/gr/signature-version-4.html

func (a *AuthCredentials) Sign(s *Service, t time.Time) []byte {
	h := ghmac([]byte("AWS4"+a.GetSecretKey()), []byte(t.Format(iSO8601BasicFormatShort)))
	h = ghmac(h, []byte(s.Region))
	h = ghmac(h, []byte(s.Name))
	h = ghmac(h, []byte(AWS4_URL))
	return h
}

// getIMDSv2Token retrieves a session token for IMDSv2.
// IMDSv2 requires a token obtained via PUT request before accessing metadata.
func getIMDSv2Token() (string, error) {
	req, err := http.NewRequest(http.MethodPut, AWSTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", AWSTokenTTL)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get IMDSv2 token: status %d", resp.StatusCode)
	}

	token, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(token), nil
}

func retrieveAWSCredentials(role, token string) (map[string]string, error) {
	var bodybytes []byte

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", AWSIAMCredsURL, role), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to retrieve credentials: status %d", resp.StatusCode)
	}

	bodybytes, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	jsondata := make(map[string]string)
	err = json.Unmarshal(bodybytes, &jsondata)
	if err != nil {
		return nil, err
	}

	return jsondata, nil
}

func retrieveIAMRole(token string) (string, error) {
	var bodybytes []byte

	req, err := http.NewRequest(http.MethodGet, AWSIAMCredsURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to retrieve IAM role: status %d", resp.StatusCode)
	}

	bodybytes, err = io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// pick the first IAM role
	role := strings.Split(string(bodybytes), "\n")[0]
	if len(role) == 0 {
		return "", errors.New("unable to retrieve IAM role")
	}

	return role, nil
}
