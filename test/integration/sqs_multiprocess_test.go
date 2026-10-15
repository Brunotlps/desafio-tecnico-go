//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"desafio-tecnico-go/internal/infra/broker"
)

// Três processos REAIS do binário da API (compilado com -race), cada um com seu pool, memória, consumidor SQS e publisher, sobre o mesmo
// PostgreSQL, a mesma fila de entrada e o mesmo tópico. Cobre reentregas comprovadas pelos logs de cada entrega, duas BETs de 80 sobre
// 100 cruzando HTTP e SQS em carteiras independentes, a morte (SIGKILL) de um processo no meio do tratamento e o encerramento dos
// demais. Uma corrida ou queda inesperada de qualquer filho falha o teste (ver o cleanup de startProcEnv).

func (c *consEnv) childEnv(name string) []string {
	return []string{
		"DATABASE_URL=" + withAppName(c.db.runtimeURL(), name), "LOG_LEVEL=info", "AWS_ENDPOINT_URL=" + c.proof.cfg.Endpoint,
		"SQS_CONSUMERS=2", "CONSUMER_NAME=" + consumerName, "SQS_WAGER_QUEUE_NAME=" + c.proof.res.Names.Input, "SQS_DLQ_NAME=" + c.proof.res.Names.DLQ,
		"SQS_CREDENTIALS_FILE=" + c.proof.cfg.CredentialsFile, "SQS_AWS_PROFILE=" + c.proof.res.Profiles[broker.Runtime],
		"CONSUMER_RECEIVE_WAIT=1s", "CONSUMER_PHASE_TIMEOUT=5s", "CONSUMER_IO_TIMEOUT=2s", "CONSUMER_BACKOFF_INITIAL=200ms", "CONSUMER_BACKOFF_MAX=1s",
		"CONSUMER_PAUSE_MAX=2s",
		"OUTBOX_PUBLISHERS=1", "SNS_EVENT_TOPIC_ARN=" + c.proof.res.TopicARN, "OUTBOX_CREDENTIALS_FILE=" + c.proof.cfg.CredentialsFile,
		"OUTBOX_AWS_PROFILE=" + c.proof.res.Profiles[broker.Publisher], "OUTBOX_POLL_INTERVAL=50ms", "OUTBOX_PUBLISH_TIMEOUT=2s",
		"OUTBOX_SQL_TIMEOUT=1s", "OUTBOX_LEASE=11s",
	}
}

func countIn(procs []*proc, needle string) int {
	n := 0
	for _, p := range procs {
		n += strings.Count(p.out.String(), needle)
	}
	return n
}

func TestThreeRealProcessesConsumeFromSQSRecoverFromSIGKILLAndShutDown(t *testing.T) {
	c := newConsEnv(t)
	var procs []*proc
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("sqs%d", i)
		procs = append(procs, startProcEnv(t, name, c.childEnv(name)...))
	}
	ctx := context.Background()

	// A. 50 reenvios do mesmo envelope, recebimentos comprovados pelos logs; um efeito, uma inbox.
	w := openWallet(t, procs[0].app(), "100.00")
	same := body("m-50x", "provider-a", "k-50x", "t-50x", w.ID, w.PlayerID, "BET", "25.00", "")
	for i := 0; i < 50; i++ {
		c.adminSend(t, same, w.ID, fmt.Sprintf("bypass-%d", i))
	}
	eventually(t, "50 deliveries handled by the three processes", 90*time.Second, func() bool {
		return countIn(procs, `"msg":"message handled"`) >= 50
	})
	if dup := countIn(procs, `"duplicate":true`); dup < 49 {
		t.Fatalf("49 of the 50 deliveries are inbox duplicates, the logs show %d", dup)
	}
	if v := c.view(w.ID); v.balance != 7500 || v.version != 2 || v.ledger != 2 || v.outbox != 4 || v.inbox != 1 {
		t.Fatalf("50 deliveries, one effect: %+v", v)
	}
	c.assertLedger(w.ID)

	// B. Duas BETs de 80 sobre 100 em 10 carteiras independentes, cruzando HTTP e SQS entre os processos.
	const n = 10
	wallets := make([]wallet, n)
	for i := range wallets {
		wallets[i] = openWallet(t, procs[i%3].app(), "100.00")
	}
	for i, wl := range wallets {
		c.send(t, body(fmt.Sprintf("m-80-%d", i), "provider-a", fmt.Sprintf("k-80s-%d", i), fmt.Sprintf("s80-%d", i), wl.ID, wl.PlayerID, "BET", "80.00", ""), wl.ID, fmt.Sprintf("m-80-%d", i))
		go submit(t, procs[(i+1)%3].app(), "provider-a", fmt.Sprintf("k-80h-%d", i), betBody(wl, "provider-a", fmt.Sprintf("h80-%d", i), "80.00"))
	}
	for i, wl := range wallets {
		eventually(t, fmt.Sprintf("wallet %d to settle", i), 60*time.Second, func() bool {
			v := c.view(wl.ID)
			return v.inbox == 1 && v.txs == 3
		})
		v := c.view(wl.ID)
		var processed, rejected int64
		_ = c.owner.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'PROCESSED' AND kind = 'BET'), count(*) FILTER (WHERE status = 'REJECTED') FROM wager_transactions WHERE wallet_id = $1`, wl.ID).Scan(&processed, &rejected)
		if v.balance != 2000 || v.version != 2 || v.ledger != 2 || v.outbox != 5 || processed != 1 || rejected != 1 {
			t.Fatalf("wallet %d: %+v processed=%d rejected=%d", i, v, processed, rejected)
		}
		c.assertLedger(wl.ID)
	}

	// C. SIGKILL de um processo no meio do tratamento: o que ele tinha em voo é reentregue (visibilidade) a outro; nenhum efeito duplicado.
	const m = 30
	many := make([]wallet, m)
	for i := range many {
		many[i] = openWallet(t, procs[i%2].app(), "100.00")
	}
	for i, wl := range many {
		c.send(t, body(fmt.Sprintf("m-k-%d", i), "provider-a", fmt.Sprintf("k-k-%d", i), fmt.Sprintf("tk-%d", i), wl.ID, wl.PlayerID, "BET", "10.00", ""), wl.ID, fmt.Sprintf("m-k-%d", i))
	}
	eventually(t, "some messages handled before the kill", 60*time.Second, func() bool {
		var done int64
		for _, wl := range many {
			done += c.view(wl.ID).inbox
		}
		return done >= 5
	})
	if err := procs[2].cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	<-procs[2].done
	for i, wl := range many {
		eventually(t, fmt.Sprintf("wallet %d (after the kill)", i), 90*time.Second, func() bool { return c.view(wl.ID).inbox == 1 })
		if v := c.view(wl.ID); v.balance != 9000 || v.version != 2 || v.ledger != 2 || v.txs != 2 {
			t.Fatalf("wallet %d: %+v (one debit of 10.00)", i, v)
		}
		c.assertLedger(wl.ID)
	}
	c.inputEmpty(t, 60*time.Second)

	// D. A saída: tudo o que foi confirmado é publicado (os publishers dos dois processos vivos assumem o que o morto deixou).
	eventually(t, "the outbox to drain", 60*time.Second, func() bool { return c.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0 })

	// E. SIGTERM dos processos vivos: encerramento gracioso, saída 0, drenagem.
	for _, p := range procs[:2] {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
			if p.exit != nil {
				t.Errorf("%s exited with %v after SIGTERM\n%s", p.name, p.exit, p.out.String())
			}
			if !strings.Contains(p.out.String(), "shutdown drained") {
				t.Errorf("%s did not log a drained shutdown:\n%s", p.name, p.out.String())
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not stop after SIGTERM", p.name)
		}
	}
}
