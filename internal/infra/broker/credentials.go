package broker

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Credentials é um par de chaves sintético e local do emulador (nunca um segredo real).
type Credentials struct{ AccessKeyID, SecretAccessKey string }

// Provider adapta o par ao SDK.
func (c Credentials) Provider() aws.CredentialsProvider {
	return credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, "")
}

// ReadCredentials lê o arquivo no formato de credenciais compartilhadas da AWS (um perfil por
// usuário). Arquivo inexistente devolve mapa vazio.
func ReadCredentials(path string) (map[string]Credentials, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return map[string]Credentials{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]Credentials{}
	var profile string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]"):
			profile = strings.TrimSpace(line[1 : len(line)-1])
		case profile != "":
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			c := out[profile]
			switch strings.TrimSpace(k) {
			case "aws_access_key_id":
				c.AccessKeyID = strings.TrimSpace(v)
			case "aws_secret_access_key":
				c.SecretAccessKey = strings.TrimSpace(v)
			}
			out[profile] = c
		}
	}
	return out, sc.Err()
}

// ProfileCredentials devolve as credenciais de um perfil.
func ProfileCredentials(path, profile string) (Credentials, error) {
	all, err := ReadCredentials(path)
	if err != nil {
		return Credentials{}, err
	}
	c, ok := all[profile]
	if !ok || c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return Credentials{}, fmt.Errorf("profile %q not found in %s (run the broker provisioning)", profile, path)
	}
	return c, nil
}

// WriteCredentials grava o arquivo de forma atômica. As chaves são sintéticas e locais: o arquivo é
// legível pelos demais contêineres (a aplicação roda com outro usuário), mas fica fora do git.
func WriteCredentials(path string, creds map[string]Credentials) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	profiles := make([]string, 0, len(creds))
	for p := range creds {
		profiles = append(profiles, p)
	}
	sort.Strings(profiles)
	var b strings.Builder
	b.WriteString("# Credenciais SINTÉTICAS do emulador local, geradas pelo provisionamento do broker. Não são segredos reais.\n")
	for _, p := range profiles {
		fmt.Fprintf(&b, "\n[%s]\naws_access_key_id = %s\naws_secret_access_key = %s\n", p, creds[p].AccessKeyID, creds[p].SecretAccessKey)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".credentials-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(b.String()); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	matchDirectoryOwner(tmp.Name())
	return os.Rename(tmp.Name(), path)
}
