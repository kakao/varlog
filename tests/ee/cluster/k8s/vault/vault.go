package vault

import (
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	addr       = flag.String("vault-addr", lookupEnvOrString("VAULT_ADDR", ""), "vault url")
	token      = flag.String("vault-token", lookupEnvOrString("VAULT_TOKEN", ""), "vault token")
	secretPath = flag.String("vault-secret-path", lookupEnvOrString("VAULT_SECRET_PATH", ""), "vault secret-path")
)

type Response struct {
	Data ClusterConnectionInfo `json:"data"`
}

type ClusterConnectionInfo struct {
	Cluster   string `json:"cluster"`
	Context   string `json:"context"`
	MasterURL string `json:"master-url"`
	User      string `json:"user"`
	Token     string `json:"token"`
}

func lookupEnvOrString(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func GetClusterConnectionInfo(t *testing.T) ClusterConnectionInfo {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, *addr+"/v1/"+*secretPath, nil)
	require.NoError(t, err)

	req.Header.Add("X-Vault-Token", *token)
	rsp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() {
		err := rsp.Body.Close()
		assert.NoError(t, err)
	}()

	body, err := io.ReadAll(rsp.Body)
	require.NoError(t, err)

	var response Response
	err = json.Unmarshal(body, &response)
	require.NoError(t, err)
	require.NotEmpty(t, response.Data.Cluster)
	require.NotEmpty(t, response.Data.Context)
	require.NotEmpty(t, response.Data.MasterURL)
	require.NotEmpty(t, response.Data.User)
	require.NotEmpty(t, response.Data.Token)
	return response.Data
}
