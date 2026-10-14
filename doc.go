// Package wager é a raiz do módulo do serviço de processamento distribuído de apostas.
//
// O código está organizado em:
//
//	cmd/api            servidor HTTP
//	cmd/migrate        migrations e papel de runtime do banco
//	internal/domain    modelo financeiro, sem dependência de infraestrutura
//	internal/app       casos de uso e portas de persistência
//	internal/infra     PostgreSQL, autenticação, HTTP e SQS
//	internal/bootstrap composição com Uber Fx
//
// O enunciado normativo está em descricao-desafio/README.md.
package wager
