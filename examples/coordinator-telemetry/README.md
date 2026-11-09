# Métricas opcionais de Flush em Go

Este módulo de exemplo separado usa Prometheus `client_golang` 1.25.0 e
OpenTelemetry Go 1.47.0; não adiciona esses pacotes ao módulo principal nem
inicia servidor HTTP ou exporter. Uma aplicação fornece seu próprio registry e
meter, configura a coleta/exportação e define o ciclo de vida do provider:

```go
observer, err := telemetry.NewFlushMetrics(registry, meter)
if err != nil {
    return err
}
config.Metrics = observer
coordinator, err := replicator.NewCoordinator(config)
```

O exemplo registra dois histogramas em segundos, com o único atributo `outcome`.
Geralmente use um backend por vez: exportar ambos para o mesmo destino pode
contar operações em duplicidade. O `context.Background()` no callback de métricas
não é o contexto da operação e não cria correlação de métricas com traces.
Resultados `success`, `failure` e `canceled` refletem o contrato Go; não os
equipare aos rótulos Rust `ReturnedOk`/`ReturnedError`, que têm outra semântica.
O observador síncrono deve ser rápido, seguro para concorrência e não entrar em
panic. Não coloque paths, IDs, credenciais ou mensagens de erro nos atributos.

Execute `go test -race -count=3 ./...` neste diretório. O teste usa um snapshot
SQLite real, um adapter sintético e registradores em memória, e verifica os três
resultados sem abrir endpoint. Este módulo aninhado não é incluído por `go test
./...` do módulo principal, mas é executado pelo alvo `make test-telemetry` e
pelo `make test` na raiz. Seu [inventário separado](../../../docs/sbom/2026-11-05/telemetry-examples/README.md)
é preliminar; dependências precisam de auditoria e notices antes de distribuição. Não é validação
dos adapters de admissão de produção nem dos fluxos Rust.

## Trace explícito

`TracedFlush(ctx, tracer, coordinator)` cria um span filho do contexto recebido,
propaga esse contexto para o `Flush` Go e usa somente `outcome` com valores
fixos. Falhas e cancelamentos têm status de erro com descrição fixa; o erro
original é retornado ao chamador, mas não gravado no span. Uma falha combinada
com cancelamento é categorizada como cancelamento. A aplicação escolhe o tracer,
sampler, processador e exportação; não é criado exporter nem endpoint. Somente
chamadas explícitas ao wrapper são rastreadas: `Run` e seus flushes internos não
são interceptados. O teste com SpanRecorder em memória verifica contexto pai,
propagação até o adapter, sucesso, falha, cancelamento, atributos e ausência de
eventos; não verifica exportação real ou dados adicionados por outros componentes.
