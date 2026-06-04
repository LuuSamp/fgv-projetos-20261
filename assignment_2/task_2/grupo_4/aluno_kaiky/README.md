# Assignment 2 — Task 2: ETL incremental, partições e agendamento

Pipeline incremental **classicmodels_sales**: extrai delta do RDS via JDBC (filtro por `etl_watermark`), grava star schema em `s3://<bucket>/analytics/`, particiona `fact_orders` por `order_year` / `order_month`, atualiza watermark após sucesso e agenda execuções com **EventBridge → Glue** (Terraform).

A **Task 1** (`assignment_2/task_1/grupo_4/aluno_kaiky`) não é alterada por este diretório; use-a para `init_watermark`, `simulate_new_orders` e `validate_incremental_source`.

## Arquitetura

```text
RDS (classicmodels + etl_watermark)
  → Glue Job (JDBC, orderDate > watermark)
  → S3 analytics/ (fact_orders particionado + dim_*)
  → Glue Catalog / Athena
EventBridge (cron semanal) → StartGlueJob
```

## Pré-requisitos

- Task 1 concluída: RDS `classicmodels` populado e `etl_watermark` inicializado (`NEVER_RUN` com `last_processed_order_date = MAX(orders.orderDate)`).
- Terraform ≥ 1.5, AWS CLI, Python 3.10+ com `boto3` e `pyarrow` (validação local).
- Credenciais AWS (`aws sts get-caller-identity`).

## Setup único

Cada task mantém **suas próprias** credenciais locais (sem copiar arquivos entre pastas de tasks).

### 1. Variáveis locais (`.env`)

Na raiz desta task:

```powershell
cd assignment_2/task_2/grupo_4/aluno_kaiky
copy .env.example .env
```

Edite `.env` com `DB_PASSWORD`, `DB_USER`, `RDS_DB_INSTANCE_IDENTIFIER` e `AWS_REGION` do seu ambiente/lab. O arquivo `.env` não é commitado.

### 2. Terraform (`terraform.tfvars`)

Transfira os mesmos valores do `.env` para o Terraform (referência local, não script entre tasks):

```powershell
cd terraform
copy terraform.tfvars.example terraform.tfvars
```

Preencha em `terraform.tfvars`, no mínimo:

| Variável Terraform | Origem no `.env` |
|--------------------|------------------|
| `aws_region` | `AWS_REGION` |
| `rds_db_instance_identifier` | `RDS_DB_INSTANCE_IDENTIFIER` |
| `db_name` | `DB_NAME` |
| `db_user` | `DB_USER` |
| `db_password` | `DB_PASSWORD` |

`terraform.tfvars` também está no `.gitignore` — nunca commite senhas.

### 3. Deploy da infraestrutura

```powershell
cd assignment_2/task_2/grupo_4/aluno_kaiky
python main.py deploy
```

Cria: bucket S3, script Glue no S3, Glue Connection/Job, catálogo (`fact_orders` com partition keys), regra **EventBridge** (cron padrão: segunda 12:00 UTC), regra SG **3306** RDS ← Glue.

## Execução direta (recomendado)

| Comando | O que faz |
|---------|-----------|
| `python main.py deploy` | `terraform init && apply` |
| `python main.py run-etl` | Inicia Glue e aguarda `SUCCEEDED` |
| `python main.py demo` | Task 1 simulate → Glue → validação S3 |
| `python main.py full` | deploy + demo |

Ciclo de evidência (enunciado 3.4) — **duas execuções**:

```powershell
# 1ª execução: baseline + primeiro delta
python main.py demo

# 2ª execução: novo simulate + incremental (preencha a seção Evidências abaixo)
python main.py demo
```

Scripts equivalentes:

```powershell
python scripts/run_incremental_cycle.py --count 5
python scripts/run_glue_job.py
python scripts/validate_etl_output.py
```

## Watermark e filtro incremental

- Pipeline: `classicmodels_sales` (tabela `etl_watermark`).
- Filtro: `orders.orderDate > last_processed_order_date` (tipo **DATE** no MySQL).
- Status `NEVER_RUN` na primeira carga incremental **não bloqueia** o filtro; usa a data já gravada pela Task 1.
- Em sucesso: `last_processed_order_date = MAX(orderDate)` do delta, `last_run_status = SUCCEEDED`.
- Em falha: `last_run_status = FAILED`, data **não** avança.
- Run sem pedidos novos: `SUCCEEDED` sem alterar a data.

## Saídas S3 e catálogo

Prefixo: `analytics/` (output `analytics_prefix`).

```text
s3://<bucket>/analytics/fact_orders/order_year=YYYY/order_month=MM/part-*.parquet
s3://<bucket>/analytics/dim_customers/
s3://<bucket>/analytics/dim_products/
s3://<bucket>/analytics/dim_dates/
s3://<bucket>/analytics/dim_countries/
```

Database Glue/Athena (default): `classicmodels_star_g4`.

```sql
SELECT COUNT(*) FROM fact_orders WHERE order_year = 2025 AND order_month = 6;
```

## Star schema (Assignment 1)

Mesmos nomes de tabelas/colunas; `sales_amount = quantity_ordered * price_each`. Dimensões: merge incremental (Opção B) apenas para entidades tocadas pelo delta. Fato: merge por `(order_id, product_id)` nas partições afetadas.

## EventBridge e IAM (3.1.2)

- **Role do target:** `glue_role_name` (default **`LabRole`**), mesma do Glue Job.
- Terraform anexa policy inline `glue:StartJobRun` no job (recurso `aws_iam_role_policy.eventbridge_start_glue`) quando `eventbridge_enabled = true`.
- Se o lab bloquear `iam:PutRolePolicy`, defina `eventbridge_enabled = false`, aplique o restante e anexe manualmente na LabRole:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["glue:StartJobRun"],
    "Resource": ["arn:aws:glue:REGION:ACCOUNT:job/JOB_NAME", "arn:aws:glue:REGION:ACCOUNT:job/JOB_NAME/*"]
  }]
}
```

- A LabRole também precisa de trust para `events.amazonaws.com` se o console exigir (alguns labs já incluem na LabRole).

Registre um disparo na seção [Evidências (3.4)](#evidências-34) deste README.

Desabilitar agendamento:

```hcl
eventbridge_enabled = false
```

## Validação sugerida (pré-Task 3)

| # | Verificação |
|---|-------------|
| 1 | Glue run `SUCCEEDED` |
| 2 | Objetos em `analytics/fact_orders/order_year=…/order_month=…/` |
| 3 | `etl_watermark.last_processed_order_date` avançou após sucesso |
| 4 | Athena `COUNT(*)` com filtro de partição retorna linhas |
| 5 | `sales_amount` coerente (`python scripts/validate_etl_output.py`) |

## Evidências (3.4)

Preencha esta seção após rodar os testes na AWS (entrega / correção). Não é código executável.

### Segunda execução incremental (3.4.2)

Após o **segundo** `python main.py demo` (com simulação de novos pedidos entre a 1ª e a 2ª execução).

**Watermark (RDS)**

| Campo | Antes do 2º Glue run | Depois do 2º Glue run |
|-------|----------------------|------------------------|
| `last_processed_order_date` | | |
| `last_run_at` | | |
| `last_run_status` | | |

```sql
SELECT pipeline_name, last_processed_order_date, last_run_at, last_run_status
FROM etl_watermark
WHERE pipeline_name = 'classicmodels_sales';
```

**Pedidos simulados (Task 1)** — cole a saída do `simulate_new_orders` desta rodada:

```
(preencher)
```

**Coerência delta → fato**

| Métrica | Valor |
|---------|-------|
| Pedidos com `orderDate > watermark` (antes do run) | |
| Linhas em `orderdetails` desses pedidos | |
| Linhas novas/gravadas em `fact_orders` (partições tocadas) | |
| Glue `JobRunId` (2ª execução) | |

**Partições S3**

```powershell
$bucket = terraform -chdir=terraform output -raw s3_bucket_name
aws s3 ls "s3://$bucket/analytics/fact_orders/" --recursive
```

**Observações**

- Apenas pedidos com `orderDate` acima do watermark anterior foram extraídos? (sim/não + nota)
- `sales_amount = quantity_ordered * price_each` no delta? (sim/não)

### Disparo EventBridge (3.4.3)

Registre um disparo agendado (cron) ou teste manual da regra.

| Item | Valor |
|------|-------|
| `eventbridge_rule_name` | |
| `glue_schedule_cron` | |
| Role do target (`glue_role_name`) | |
| Data/hora do disparo (UTC) | |
| Glue `JobRunId` | |
| Estado final | |

```powershell
$job = terraform -chdir=terraform output -raw glue_job_name
aws glue get-job-runs --job-name "$job" --max-results 5
```

**IAM (LabRole)** — se `terraform apply` falhou em `aws_iam_role_policy`, descreva o que foi anexado manualmente (`glue:StartJobRun`, trust `events.amazonaws.com` se necessário):

```
(preencher)
```

## Troubleshooting

### `AccessDenied: iam:CreateRole` / `iam:PutRolePolicy`

Use `glue_role_name = "LabRole"` e, se necessário, `eventbridge_enabled = false` + policy manual (ver acima).

### Glue não conecta ao RDS

O Terraform cria ingress **3306** do SG da Glue Connection para o SG do RDS. Confirme instância `rds_db_instance_identifier` igual à Task 1.

### State Terraform de outra conta

Arquive `terraform.tfstate` antigo e rode `terraform apply` na conta atual.

### `terraform apply` após editar o script

Sempre que alterar código em `glue_jobs/`, rode `terraform apply` (empacota zip + envia `etl_job.py` ao S3) e depois `python main.py run-etl`.

## Estrutura do diretório

```text
glue_jobs/              # Código PySpark do job (fora do Terraform)
  etl_job.py            # Entrypoint Glue
  constants.py          # Contratos do star schema / merge
  pipeline/             # extract | transform | load | validate
terraform/              # Infra (S3, Glue, Catalog, EventBridge, SG, VPC endpoint)
.env.example            # template de credenciais locais (copiar para .env)
scripts/                # run_glue_job, validate, incremental cycle
main.py                 # deploy | run-etl | demo | full
```

O Terraform referencia `../glue_jobs/` com `path.module` (nunca paths locais de máquina). O job usa `--extra-py-files` com `glue_modules.zip` (todos os módulos exceto `etl_job.py`).
