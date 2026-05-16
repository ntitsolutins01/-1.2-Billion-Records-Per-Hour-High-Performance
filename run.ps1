# ==============================================================================
#  run.ps1 — Pipeline de Streaming Financeiro: Kafka + Spark
#  Executa o restart completo e inicializa o pipeline automaticamente
# ==============================================================================

param (
    [switch]$SkipBuild,   # Pula o down/up (infra já está rodando)
    [switch]$OnlyInfra    # Sobe apenas a infra, sem producer nem spark-submit
)

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES
# ------------------------------------------------------------------------------
$BROKER         = "kafka-broker-1"
$BOOTSTRAP      = "kafka-broker-1:19092"
$SPARK_MASTER   = "spark://spark-master:7077"
$SPARK_PACKAGE  = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
$SPARK_JOB      = "/app/jobs/spark_processor.py"
$PRODUCER       = "main.py"
$SLEEP_SECONDS  = 15

# ------------------------------------------------------------------------------
# FUNÇÕES AUXILIARES
# ------------------------------------------------------------------------------
function Write-Step($num, $msg) {
    Write-Host ""
    Write-Host "[$num] $msg" -ForegroundColor Cyan
    Write-Host ("-" * 60) -ForegroundColor DarkGray
}

function Write-Success($msg) {
    Write-Host "  ✔ $msg" -ForegroundColor Green
}

function Write-Info($msg) {
    Write-Host "  → $msg" -ForegroundColor Yellow
}

# ------------------------------------------------------------------------------
# PASSO 1 — Parar containers
# ------------------------------------------------------------------------------
if (-not $SkipBuild) {
    Write-Step 1 "Parando containers existentes..."
    docker compose down
    Write-Success "Containers parados."

    # --------------------------------------------------------------------------
    # PASSO 2 — Limpar volume de checkpoint
    # --------------------------------------------------------------------------
    Write-Step 2 "Removendo volume de checkpoint..."
    docker volume rm kafkasparkarch_spark-checkpoints 2>$null
    Write-Success "Volume removido (ou inexistente)."

    # --------------------------------------------------------------------------
    # PASSO 3 — Subir infraestrutura
    # --------------------------------------------------------------------------
    Write-Step 3 "Subindo infraestrutura (Kafka + Spark)..."
    docker compose up -d
    Write-Success "Containers iniciados."

    # --------------------------------------------------------------------------
    # PASSO 4 — Aguardar inicialização
    # --------------------------------------------------------------------------
    Write-Step 4 "Aguardando inicialização dos brokers ($SLEEP_SECONDS segundos)..."
    Start-Sleep -Seconds $SLEEP_SECONDS
    Write-Success "Pronto."
} else {
    Write-Info "Flag -SkipBuild ativa: pulando down/up da infra."
}

# ------------------------------------------------------------------------------
# PASSO 5 — Criar tópicos Kafka
# ------------------------------------------------------------------------------
Write-Step 5 "Criando tópicos Kafka..."

docker exec $BROKER /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server $BOOTSTRAP --create `
    --topic financial_transactions `
    --partitions 5 --replication-factor 3 --if-not-exists

docker exec $BROKER /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server $BOOTSTRAP --create `
    --topic transaction_aggregates `
    --partitions 3 --replication-factor 2 --if-not-exists

docker exec $BROKER /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server $BOOTSTRAP --create `
    --topic transaction_anomalies `
    --partitions 3 --replication-factor 2 --if-not-exists

Write-Success "Tópicos criados (ou já existentes)."

# ------------------------------------------------------------------------------
# PASSO 6 + 7 — Producer e Spark Job em paralelo
# ------------------------------------------------------------------------------
if (-not $OnlyInfra) {

    Write-Step 6 "Iniciando Producer Python em nova janela..."
    Start-Process powershell -ArgumentList `
        "-NoExit", "-Command", "python $PRODUCER"
    Write-Success "Producer iniciado."

    Write-Step 7 "Submetendo job Spark em nova janela..."
    Start-Process powershell -ArgumentList `
        "-NoExit", "-Command",
        "docker exec spark-master spark-submit ``
            --master $SPARK_MASTER ``
            --packages $SPARK_PACKAGE ``
            $SPARK_JOB"
    Write-Success "Spark job submetido."

} else {
    Write-Info "Flag -OnlyInfra ativa: producer e spark-submit não iniciados."
}

# ------------------------------------------------------------------------------
# RESUMO FINAL
# ------------------------------------------------------------------------------
Write-Host ""
Write-Host ("=" * 60) -ForegroundColor DarkGray
Write-Host "  PIPELINE INICIADO COM SUCESSO!" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Spark Master UI  →  http://localhost:8080" -ForegroundColor White
Write-Host "  Spark Job UI     →  http://localhost:4040" -ForegroundColor White
Write-Host ""
Write-Host "  Para parar tudo:  docker compose down" -ForegroundColor DarkGray
Write-Host ""
