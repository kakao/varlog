package e2e

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/pkg/errors"
)

var vaultAddr = flag.String("vault-addr", lookupEnvOrString("VAULT_ADDR", ""), "vault url")
var vaultToken = flag.String("vault-token", lookupEnvOrString("VAULT_TOKEN", ""), "vault token")
var vaultSecretPath = flag.String("vault-secret-path", lookupEnvOrString("VAULT_SECRET_PATH", ""), "vault secret-path")

func lookupEnvOrString(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getVarlogK8sConnInfo() (map[string]interface{}, error) {
	req, err := http.NewRequest("GET", *vaultAddr+"/v1/"+*vaultSecretPath, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("X-Vault-Token", *vaultToken)
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rsp.Body.Close()
	}()

	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	jsonBody := make(map[string]interface{})
	if err := json.Unmarshal(body, &jsonBody); err != nil {
		return nil, err
	}
	if data, ok := jsonBody["data"]; ok {
		return data.(map[string]interface{}), nil
	}
	return nil, errors.Errorf("no 'data' field: %s", string(body))
}
