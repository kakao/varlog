package e2e

import (
	"flag"
	"os"

	"github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
)

var vaultAddr = flag.String("vault-addr", lookupEnvOrString("VAULT_ADDR", ""), "vault url")
var vaultToken = flag.String("vault-token", lookupEnvOrString("VAULT_TOKEN", ""), "vault token")
var roleID = flag.String("role-id", lookupEnvOrString("VAULT_ROLE_ID", ""), "vault role-id")
var secretID = flag.String("secret-id", lookupEnvOrString("VAULT_SECRET_ID", ""), "vault secret-id")
var secretPath = flag.String("secret-path", lookupEnvOrString("VAULT_SECRET_PATH", ""), "vault secret-path")

func lookupEnvOrString(key string, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getVaultToken(cli *api.Client) (string, error) {
	if *vaultToken != "" {
		return *vaultToken, nil
	}

	data := map[string]interface{}{
		"role_id":   *roleID,
		"secret_id": *secretID,
	}
	resp, err := cli.Logical().Write("auth/approle/login", data)
	if err != nil {
		return "", errors.Wrap(err, "vault")
	}

	if resp.Auth == nil {
		return "", errors.New("no auth info returned")
	}

	return resp.Auth.ClientToken, nil
}

func getVaultSecret(cli *api.Client, path string) (map[string]interface{}, error) {
	resp, err := cli.Logical().Read(path)
	if err != nil {
		return nil, errors.Wrap(err, "vault")
	}

	if resp.Data == nil {
		return nil, errors.New("no secret returned")
	}

	return resp.Data, nil
}

func getVarlogK8sConnInfo() (map[string]interface{}, error) {
	conf := &api.Config{
		Address: *vaultAddr,
	}

	client, err := api.NewClient(conf)
	if err != nil {
		return nil, errors.Wrap(err, "vault")
	}

	token, err := getVaultToken(client)
	if err != nil {
		return nil, err
	}
	client.SetToken(token)

	return getVaultSecret(client, *secretPath /*"secret/varlog/dkosv3/e2e"*/)
}
