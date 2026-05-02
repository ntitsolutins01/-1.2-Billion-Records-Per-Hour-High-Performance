# -1.2-Billion-Records-Per-Hour-High-Performance
1,2 bilhões de registros por hora – Projeto de Engenharia de Dados de ponta a ponta com Kafka e Spark de alto desempenho

# 🏦 Financial Transactions — Kafka Producer

Pipeline de ingestão de dados financeiros em tempo real utilizando **Redpanda/Kafka**, **Python** e **Threading**, com produção paralela de mensagens e monitoramento via Redpanda Console.

---

## 🏗️ Arquitetura

```
┌──────────────────────────────────────────────┐
│              Python Producer                 │
│  Thread 0  Thread 1  Thread 2  Thread 3 ...  │
│        └────────┴────────┴────────┘          │
│              producer.produce()              │
│              producer.flush()                │
└──────────────────────┬───────────────────────┘
                       │
           ┌───────────▼────────────┐
           │     Redpanda Cluster   │
           │  Broker 1 → 29092      │
           │  Broker 2 → 39092      │
           │  Broker 3 → 49092      │
           │                        │
           │  Topic: financial_     │
           │         transactions   │
           │  Partitions: 5         │
           │  Replication: 3        │
           └────────────────────────┘
```

---

## ⚙️ Configuração do Cluster

| Parâmetro | Valor |
|---|---|
| Brokers | `localhost:29092`, `39092`, `49092` |
| Tópico | `financial_transactions` |
| Partições | `5` |
| Replication Factor | `3` |
| Compressão | `gzip` |
| Batch messages | `1000` |
| Linger ms | `10` |
| Acks | `1` |

---

## 🧾 Schema da Transação

```json
{
  "transactioId":     "uuid-v4",
  "userId":           "user_42",
  "amount":           125432.50,
  "transactionTime":  "2026-05-02 13:01:22",
  "merchantId":       "merchant1",
  "transactionType":  "purchase",
  "location":         "location 17",
  "paymentMethod":    "pix",
  "isInternational":  false,
  "curreency":        "BRL"
}
```

---

## 📦 Estrutura

```
kafka-financial-transactions/
├── main.py               # Producer principal
├── docker-compose.yml    # Cluster Redpanda (3 brokers)
├── requirements.txt      # Dependências Python
└── README.md
```

---

## 📥 Instalação e Execução

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/kafka-financial-transactions.git
cd kafka-financial-transactions

# 2. Ambiente virtual
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Suba o cluster Redpanda
docker-compose up -d

# 5. Execute o Producer
python main.py
```

### ⚡ Execução paralela em múltiplos terminais

```bash
# Terminal 1        # Terminal 2        # Terminal 3
python main.py      python main.py      python main.py
```
> Cada terminal dispara **6 threads simultâneas** → **18 transações por ciclo** com 3 terminais.

---

## 🔍 Monitoramento

Acompanhe as transações em tempo real pelo **Redpanda Console**:

```
http://localhost:8080
```

✅ Partições balanceadas · ✅ Offsets crescendo · ✅ Compressão gzip · ✅ Mensagens nos 3 brokers

---

## 📌 Evolução do Projeto

| Versão | Descrição |
|---|---|
| **v1** | Produção sequencial com `while True` — Producer rodando por 30+ minutos contínuos |
| **v2** | Produção paralela com `threading.Thread` — 6 threads por execução, suporte a múltiplos terminais |

---

## ⚠️ Melhorias Futuras

- [ ] Mover `producer.flush()` para fora de cada thread (após o `join()`)
- [ ] Adicionar `while True` externo para execução contínua
- [ ] Implementar **Consumer** com Apache Spark Structured Streaming
- [ ] Detecção de anomalias nas transações em tempo real
- [ ] Corrigir typos no schema (`transactioId` → `transactionId`, `curreency` → `currency`)

---

## 🛠️ Dependências

```txt
confluent-kafka
authlib
```

---

👩‍💻 Desenvolvido por **Fábio Muniz** · 📍 Brasília, Brasil · 📜 MIT License
````_

