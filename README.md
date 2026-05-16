# 🏦 PulseFinance — Real-Time Financial Intelligence
### 1.2 Billion Records Per Hour | Kafka + Spark Streaming Pipeline

> Pipeline de engenharia de dados de ponta a ponta para processamento de transações
> financeiras em tempo real, com detecção de anomalias, agregações por janela temporal
> e arquitetura distribuída com Redpanda/Kafka e Apache Spark.

---

## 🏗️ Arquitetura Completa

\`\`\`
┌─────────────────────────────────────────────────────────────────┐
│                     Python Producer                             │
│   Thread 0   Thread 1   Thread 2   Thread 3   Thread 4  ...     │
│          └──────────┴──────────┴──────────┴──────────┘          │
│                     producer.produce()                          │
│                     producer.flush()                            │
└─────────────────────────────┬───────────────────────────────────┘
                              │
              ┌───────────────▼──────────────────┐
              │         Redpanda Cluster         │
              │   Broker 1 → 29092 (19092)       │
              │   Broker 2 → 39092 (29092)       │
              │   Broker 3 → 49092 (39092)       │
              │                                  │
              │   Topic: financial_transactions  │
              │   Partitions: 5 | Replication: 3 │
              └───────────────┬──────────────────┘
                              │
              ┌───────────────▼───────────────────┐
              │       Apache Spark Cluster        │
              │   spark-master  :8080 / :7077     │
              │   spark-worker-1  2 cores / 2GiB  │
              │   spark-worker-2  2 cores / 2GiB  │
              │   spark-worker-3  2 cores / 2GiB  │
              │                                   │
              │   Spark Structured Streaming      │
              │   ├── Aggregations (5 min window) │
              │   └── Anomaly Detection           │
              └──────────────┬────────────────────┘
                             │
         ┌───────────────────┼────────────────────┐
         ▼                   ▼                    ▼
  financial_           transaction_        transaction_
  transactions         aggregates          anomalies
  (5 partições)        (3 partições)       (3 partições)
  (repl. 3)            (repl. 2)           (repl. 2)
\`\`\`

---

## ⚙️ Configuração do Cluster

### Kafka / Redpanda

| Parâmetro          | Valor                                      |
|--------------------|--------------------------------------------|
| Brokers            | \`localhost:29092\`, \`39092\`, \`49092\`  |
| Tópico Principal   | \`financial_transactions\`                 |
| Tópico Agregações  | \`transaction_aggregates\`                 |
| Tópico Anomalias   | \`transaction_anomalies\`                  |
| Partições          | \`5\` (principal) · \`3\` (demais)         |
| Replication Factor | \`3\` (principal) · \`2\` (demais)         |
| Compressão         | \`gzip\`                                   |
| Batch messages     | \`1000\`                                   |
| Linger ms          | \`10\`                                     |
| Acks               | \`1\`                                      |

### Apache Spark

| Parâmetro          | Valor                                      |
|--------------------|--------------------------------------------|
| Master             | \`spark://spark-master:7077\`              |
| Workers            | \`3\`                                      |
| Cores por Worker   | \`2\`                                      |
| Memória por Worker | \`2GiB\`                                   |
| Total de Cores     | \`6\`                                      |
| Total de Memória   | \`6GiB\`                                   |
| Spark UI           | \`http://localhost:8080\`                  |
| Trigger            | \`processingTime = 10 seconds\`            |
| Janela Temporal    | \`5 minutos\` com slide de \`1 minuto\`    |

---

## 🧾 Schema da Transação

\`\`\`json
{
  "transactionId":    "uuid-v4",
  "userId":           "user_42",
  "amount":           125432.50,
  "transactionTime":  "2026-05-02 13:01:22",
  "merchantId":       "merchant_1",
  "transactionType":  "purchase",
  "location":         "location_17",
  "paymentMethod":    "pix",
  "isInternational":  false,
  "currency":         "BRL"
}
\`\`\`

---

## 🔎 Lógica de Detecção de Anomalias

| Regra                          | Condição                              |
|--------------------------------|---------------------------------------|
| 💸 Alto Valor                  | \`amount > 10.000\`                   |
| 🌍 Internacional Suspeita      | \`isInternational == true\`           |
| ⚡ Alta Frequência por Usuário | \`count > 100 em janela de 5 minutos\`|

> Transações que atendem a qualquer uma das condições acima são publicadas
> automaticamente no tópico \`transaction_anomalies\`.

---

## 📦 Estrutura do Projeto

\`\`\`
kafka-financial-transactions/
├── main.py                  # Producer Python com threading
├── spark_processor.py       # Spark Structured Streaming job
├── docker-compose.yml       # Cluster Redpanda (3 brokers) + Spark (1 master + 3 workers)
├── requirements.txt         # Dependências Python
└── README.md
\`\`\`

---

## 📥 Instalação e Execução

### Pré-requisitos

- Docker Desktop rodando
- Python instalado no host
- PowerShell (terminal do PyCharm recomendado)

---

### 🚀 Pipeline Completo — Passo a Passo

\`\`\`powershell
# PASSO 1 — Parar containers anteriores
docker compose down

# PASSO 2 — Limpar volumes de checkpoint
docker volume rm kafkasparkarch_spark-checkpoints 2>$null

# PASSO 3 — Subir a infraestrutura
docker compose up -d

# PASSO 4 — Aguardar inicialização dos brokers
Start-Sleep -Seconds 15

# PASSO 5 — Criar os tópicos Kafka

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \`
  --bootstrap-server kafka-broker-1:19092 --create \`
  --topic financial_transactions \`
  --partitions 5 --replication-factor 3 --if-not-exists

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \`
  --bootstrap-server kafka-broker-1:19092 --create \`
  --topic transaction_aggregates \`
  --partitions 3 --replication-factor 2 --if-not-exists

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \`
  --bootstrap-server kafka-broker-1:19092 --create \`
  --topic transaction_anomalies \`
  --partitions 3 --replication-factor 2 --if-not-exists

# PASSO 6 — Rodar o Producer Python (Terminal 1)
python main.py

# PASSO 7 — Submeter o job Spark (Terminal 2)
docker exec spark-master spark-submit \`
  --master spark://spark-master:7077 \`
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \`
  /app/spark_processor.py
\`\`\`

> 💡 Os **Passos 6 e 7** podem rodar simultaneamente em terminais separados no PyCharm.

---

### ⚡ Execução Paralela do Producer

\`\`\`powershell
# Terminal 1        # Terminal 2        # Terminal 3
python main.py      python main.py      python main.py
\`\`\`

> Cada terminal dispara **6 threads simultâneas** → **18 transações por ciclo** com 3 terminais.

---

## 🔍 Monitoramento

| Interface        | URL                      | O que monitorar                   |
|------------------|--------------------------|-----------------------------------|
| Redpanda Console | \`http://localhost:8080\` | Offsets, partições, mensagens     |
| Spark Master UI  | \`http://localhost:8081\` | Jobs, stages, workers ativos      |

✅ Partições balanceadas · ✅ Offsets crescendo · ✅ Compressão gzip · ✅ Workers ativos

---

## 🛠️ Comandos Úteis

\`\`\`powershell
# Ver logs do Kafka Broker 1
docker logs kafka-broker-1 -f

# Ver logs do Spark Master
docker logs spark-master -f

# Listar tópicos existentes
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \`
  --bootstrap-server kafka-broker-1:19092 --list

# Ver containers rodando
docker ps

# Ver volumes Docker ativos
docker volume ls

# Reset total (para e remove volumes)
docker compose down -v

# Remover apenas checkpoints
docker volume rm kafkasparkarch_spark-checkpoints 2>$null

# Remover imagens não utilizadas
docker image prune -f
\`\`\`

---

## 📌 Evolução do Projeto

| Versão  | Descrição                                                                  |
|---------|----------------------------------------------------------------------------|
| **v1**  | Produção sequencial com \`while True\` — Producer rodando por 30+ minutos |
| **v2**  | Produção paralela com \`threading.Thread\` — 6 threads por execução       |
| **v3**  | Integração com Apache Spark — Structured Streaming + 3 workers            |
| **v4**  | Pipeline completo: Producer → Kafka → Spark → Aggregations + Anomalies    |

---

## 🛠️ Dependências

\`\`\`txt
confluent-kafka
authlib
pyspark
\`\`\`

---

## ⚠️ Observações Importantes

- O \`replication-factor\` não pode ser maior que o número de brokers ativos
- Em caso de erro de checkpoint, repita os Passos 1 e 2 antes de subir novamente
- O schema foi corrigido: \`transactioId\` → \`transactionId\` · \`curreency\` → \`currency\`

---

## 👨‍💻 Autor

**Fábio Muniz** · 📍 Brasília, Brasil · 📜 MIT License

🔗 [LinkedIn](https://www.linkedin.com/in/fabiomunizdeveloper/) · High Performance Data Engineering — Kafka + Spark
