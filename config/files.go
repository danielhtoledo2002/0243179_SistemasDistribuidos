package config

import (
	"os"
	"path/filepath"
	"fmt"
	
)

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("Policy.csv")
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
		fmt.Printf("directory: %v\n", homeDir)

		fmt.Printf("directory: %v\n",  filepath.Join("/Users/danielhernandez/reps/0243179_SistemasDistribuidos/cert/", filename))

	// modify this
	return filepath.Join("/Users/danielhernandez/reps/0243179_SistemasDistribuidos/cert/", filename)
}

