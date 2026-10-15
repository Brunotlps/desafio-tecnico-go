//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"desafio-tecnico-go/internal/config"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/inbound"
	"desafio-tecnico-go/internal/infra/auth"
	"desafio-tecnico-go/internal/infra/broker"
	infrasqs "desafio-tecnico-go/internal/infra/sqs"
)

// consumerAppConfig liga o consumidor SQS (4 executores) da aplicação completa às filas e ao principal de runtime do ambiente de
// prova, com o publisher da outbox opcional.
func (c *consEnv) consumerAppConfig(publisher bool, mods ...func(*config.Config)) config.Config {
	return c.appConfig(append([]func(*config.Config){func(cfg *config.Config) {
		cfg.SQSConsumers, cfg.ConsumerName = 4, consumerName
		cfg.SQSWagerQueue, cfg.SQSDLQName = c.proof.res.Names.Input, c.proof.res.Names.DLQ
		cfg.SQSCredentialsFile, cfg.SQSAWSProfile = c.proof.cfg.CredentialsFile, c.proof.res.Profiles[broker.Runtime]
		cfg.ConsumerReceiveWait, cfg.ConsumerPhaseTimeout, cfg.ConsumerIOTimeout = time.Second, 3*time.Second, time.Second
		cfg.ConsumerBackoffInitial, cfg.ConsumerBackoffMax, cfg.ConsumerPauseMax = 200*time.Millisecond, time.Second, 2*time.Second
		if !publisher {
			cfg.OutboxPublishers = 0
		}
	}}, mods...)...)
}

// O produtor interno só envia depois de vincular a origem autenticada ao providerId do envelope.
func TestTheInternalProducerBindsTheAuthenticatedOriginToTheProviderBeforeAnySend(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	v := auth.NewVerifier(auth.Config{Issuer: env.issuer, JWKSURL: env.jwksURL, Audience: "wager-api", ClockSkew: time.Second})
	if err := v.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Stop)
	pub := inbound.NewPublisher(c.prod, func(tok string) (inbound.Origin, error) {
		pr, err := v.Verify(tok)
		if err != nil {
			return inbound.Origin{}, err
		}
		return inbound.Origin{ProviderID: pr.ProviderID, Internal: pr.IsInternal()}, nil
	})
	ctx := context.Background()
	inspector := c.proof.sqs(broker.Inspector)
	queued := func() int { v, f := depth(t, inspector, c.proof.res.InputURL); return v + f }

	bodyA := body("m-pa", "provider-a", "k-pa", "t-pa", w, p, "BET", "10.00", "")
	bodyB := body("m-pb", "provider-b", "k-pb", "t-pb", w, p, "BET", "10.00", "")
	// provider-a não publica como provider-b: recusado ANTES do envio.
	if _, err := pub.Publish(ctx, token(t, "provider-a"), []byte(bodyB)); !errors.Is(err, inbound.ErrOriginMismatch) {
		t.Fatalf("provider-a publishing for provider-b = %v, want ErrOriginMismatch", err)
	}
	if n := queued(); n != 0 {
		t.Fatalf("a refused origin must not reach the queue (%d queued)", n)
	}
	// Tokens falsificados ou lixo: sem envio.
	for name, tok := range forgedTokens(t) {
		if _, err := pub.Publish(ctx, tok, []byte(bodyA)); err == nil {
			t.Errorf("forged token %q was accepted", name)
		}
	}
	if _, err := pub.Publish(ctx, "garbage", []byte(bodyA)); err == nil {
		t.Error("a garbage token was accepted")
	}
	// Um envelope inválido não é enviado nem com origem válida.
	if _, err := pub.Publish(ctx, token(t, "provider-a"), []byte(`{"nope":1}`)); err == nil {
		t.Error("an invalid envelope was sent")
	}
	if n := queued(); n != 0 {
		t.Fatalf("nothing refused may reach the queue (%d queued)", n)
	}
	// A origem correta publica, com grupo = walletId e deduplicação = messageId; o serviço interno publica para qualquer provedor.
	if id, err := pub.Publish(ctx, token(t, "provider-a"), []byte(bodyA)); err != nil || id != "m-pa" {
		t.Fatalf("provider-a for provider-a: %q %v", id, err)
	}
	if id, err := pub.Publish(ctx, token(t, "internal-service"), []byte(bodyB)); err != nil || id != "m-pb" {
		t.Fatalf("internal-service for provider-b: %q %v", id, err)
	}
	got := c.drain(t, c.proof.res.InputURL, 2, 20*time.Second)
	if len(got) != 2 {
		t.Fatalf("expected the two accepted envelopes, got %d", len(got))
	}
	for _, m := range got {
		var env struct {
			MessageID string `json:"messageId"`
		}
		_ = json.Unmarshal([]byte(m.Body), &env)
		if m.Group != w || m.Dedup != env.MessageID {
			t.Errorf("group %q / dedup %q, want the wallet and the messageId %q", m.Group, m.Dedup, env.MessageID)
		}
	}
	// Os principais NÃO autorizados não enviam (AccessDenied do broker), sem efeito financeiro.
	for _, role := range []broker.Role{broker.Runtime, broker.Publisher, broker.Nobody} {
		denied, err := infrasqs.NewProducer(ctx, infrasqs.ProducerConfig{Region: c.proof.cfg.Region, EndpointURL: c.proof.cfg.Endpoint,
			QueueName: c.proof.res.Names.Input, CredentialsFile: c.proof.cfg.CredentialsFile, Profile: c.proof.res.Profiles[role]})
		if err != nil {
			t.Fatal(err)
		}
		requireDenied(t, string(role)+" sending to the input", denied.Send(ctx, []byte(bodyA), w, "m-denied"))
	}
	if n := queued(); n != 0 {
		t.Fatalf("denied principals must have no effect (%d queued)", n)
	}
	if v := c.view(w); v.balance != 10000 || v.inbox != 0 {
		t.Fatalf("producing alone changes nothing: %+v", v)
	}
}

// HTTP e SQS cruzados: duas BETs distintas de 80 sobre 100 (uma PROCESSED, outra REJECTED), a mesma operação por dois caminhos (um
// débito) e a mesma chave com conteúdo diferente (um vence, o outro é conflito) — com o consumidor, o HTTP e a outbox da aplicação real.
func TestHTTPAndSQSRacingOnTheSameWalletsKeepOneFinancialEffect(t *testing.T) {
	c := newConsEnv(t)
	a := startApp(t, c.consumerAppConfig(false))

	t.Run("two distinct BETs of 80 over 100", func(t *testing.T) {
		const n = 6
		type pair struct {
			w    wallet
			http response
		}
		ws := make([]wallet, n)
		for i := range ws {
			ws[i] = openWallet(t, a, "100.00")
		}
		var wg sync.WaitGroup
		https := make([]response, n)
		for i := range ws {
			wg.Add(1)
			go func() {
				defer wg.Done()
				https[i] = submit(t, a, "provider-a", fmt.Sprintf("k-http-%d", i), betBody(ws[i], "provider-a", fmt.Sprintf("http-%d", i), "80.00"))
			}()
			b := body(fmt.Sprintf("m-x-%d", i), "provider-a", fmt.Sprintf("k-sqs-%d", i), fmt.Sprintf("sqs-%d", i), ws[i].ID, ws[i].PlayerID, "BET", "80.00", "")
			c.send(t, b, ws[i].ID, fmt.Sprintf("m-x-%d", i))
		}
		wg.Wait()
		for i, w := range ws {
			eventually(t, "the SQS message of wallet "+w.ID, 40*time.Second, func() bool { return c.inboxRows(w.ID) == 1 })
			v := c.view(w.ID)
			if v.balance != 2000 || v.version != 2 || v.ledger != 2 || v.outbox != 5 || v.txs != 3 {
				t.Fatalf("wallet %d: %+v (want balance 20.00, version 2, ledger 2, outbox 5, 3 transactions)", i, v)
			}
			var processed, rejected int64
			_ = c.owner.QueryRow(context.Background(), `SELECT count(*) FILTER (WHERE status = 'PROCESSED' AND kind = 'BET'),
				count(*) FILTER (WHERE status = 'REJECTED') FROM wager_transactions WHERE wallet_id = $1`, w.ID).Scan(&processed, &rejected)
			if processed != 1 || rejected != 1 {
				t.Fatalf("wallet %d: %d processed, %d rejected, want one each", i, processed, rejected)
			}
			c.assertLedger(w.ID)
		}
		c.inputEmpty(t, 30*time.Second)
	})

	t.Run("the same operation over HTTP and SQS", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		var r response
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			r = submit(t, a, "provider-a", "k-same", betBody(w, "provider-a", "same-1", "25.00"))
		}()
		c.send(t, body("m-same", "provider-a", "k-same", "same-1", w.ID, w.PlayerID, "BET", "25.00", ""), w.ID, "m-same")
		wg.Wait()
		if r.Status != http.StatusOK {
			t.Fatalf("http: %d %s", r.Status, r.Raw)
		}
		eventually(t, "the SQS delivery", 30*time.Second, func() bool { return c.inboxRows(w.ID) == 1 })
		if v := c.view(w.ID); v.balance != 7500 || v.version != 2 || v.ledger != 2 || v.outbox != 4 || v.txs != 2 || v.inbox != 1 {
			t.Fatalf("one debit expected: %+v", v)
		}
	})

	t.Run("the same key with different content", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		var r response
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			r = submit(t, a, "provider-a", "k-clash", betBody(w, "provider-a", "clash-1", "25.00"))
		}()
		c.send(t, body("m-clash", "provider-a", "k-clash", "clash-1", w.ID, w.PlayerID, "BET", "30.00", ""), w.ID, "m-clash")
		wg.Wait()
		// Um dos dois vence; o outro é conflito de idempotência (HTTP 409, ou diagnóstico + DLQ no SQS). Um só débito.
		eventually(t, "the race to settle", 40*time.Second, func() bool {
			return c.view(w.ID).txs == 2 && (c.inboxRows(w.ID) == 1 || c.count(`SELECT count(*) FROM message_diagnostics WHERE code = 'IDEMPOTENCY_KEY_REUSED'`) >= 1)
		})
		v := c.view(w.ID)
		if v.ledger != 2 || (v.balance != 7500 && v.balance != 7000) {
			t.Fatalf("one winner only: %+v", v)
		}
		sqsWon := c.inboxRows(w.ID) == 1
		if sqsWon && r.Status != http.StatusConflict {
			t.Fatalf("the SQS message won, so HTTP must see a conflict, got %d %s", r.Status, r.Raw)
		}
		if !sqsWon && r.Status != http.StatusOK {
			t.Fatalf("HTTP won, got %d %s", r.Status, r.Raw)
		}
		c.assertLedger(w.ID)
		_ = domain.Currency("BRL")
	})
}

// Fluxo completo: mensagem recebida → estado/saldo/ledger/inbox/outbox confirmados → evento publicado pelo publisher e observado no
// assinante, com os identificadores de cada etapa. Nenhuma publicação acontece dentro da transação financeira.
func TestAReceivedMessageEndsAsPublishedEventsObservedBySubscriber(t *testing.T) {
	c := newConsEnv(t)
	a := startApp(t, c.consumerAppConfig(true))
	w := openWallet(t, a, "100.00")
	c.send(t, body("m-flow", "provider-a", "k-flow", "t-flow", w.ID, w.PlayerID, "BET", "25.00", ""), w.ID, "m-flow")
	eventually(t, "the message to be handled", 30*time.Second, func() bool { return c.inboxRows(w.ID) == 1 })
	var txID string
	if err := c.owner.QueryRow(context.Background(), `SELECT transaction_id::text FROM inbox_messages WHERE message_id = 'm-flow'`).Scan(&txID); err != nil {
		t.Fatal(err)
	}
	// 4 eventos da carteira (abertura 2 + BET 2), todos publicados no tópico e entregues à fila assinante.
	eventually(t, "the outbox to be drained", 30*time.Second, func() bool { return c.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0 })
	msgs := c.drain(t, c.proof.res.AuditURL, 4, 30*time.Second)
	if len(msgs) != 4 {
		t.Fatalf("the subscriber received %d events, want 4", len(msgs))
	}
	types := map[string]string{}
	for _, m := range msgs {
		var env struct {
			EventID   string `json:"eventId"`
			EventType string `json:"eventType"`
			Data      struct {
				WalletID      string `json:"walletId"`
				TransactionID string `json:"transactionId"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(m.Body), &env); err != nil {
			t.Fatal(err)
		}
		if m.Group != w.ID || m.Dedup != env.EventID || env.Data.WalletID != w.ID {
			t.Fatalf("routing: %+v", m)
		}
		if env.Data.TransactionID == txID {
			types[env.EventType] = env.EventID
		}
	}
	if types["WagerTransactionProcessed"] == "" || types["WalletBalanceChanged"] == "" {
		t.Fatalf("the BET handled from SQS must have produced WagerTransactionProcessed and WalletBalanceChanged for %s: %v", txID, types)
	}
	if v := c.view(w.ID); v.balance != 7500 || v.version != 2 || v.ledger != 2 || v.outbox != 4 || v.inbox != 1 {
		t.Fatalf("state: %+v", v)
	}
	c.assertLedger(w.ID)
}
