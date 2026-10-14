//go:build unix

package broker

import (
	"os"
	"path/filepath"
	"syscall"
)

// matchDirectoryOwner passa o arquivo ao dono do diretório que o contém. No Compose o provisionamento
// roda como root (para escrever no diretório montado); se o diretório pertence ao usuário do host, o arquivo
// também passa a ser dele e pode ser lido e removido sem privilégios. Melhor esforço: se o diretório é do
// root (criado pelo Docker), o arquivo continua do root, legível por todos.
func matchDirectoryOwner(file string) {
	info, err := os.Stat(filepath.Dir(file))
	if err != nil {
		return
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok && os.Geteuid() == 0 {
		_ = os.Chown(file, int(st.Uid), int(st.Gid))
	}
}
