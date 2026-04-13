# Lakehouse Local con Localstack — Portfolio de Data Engineering

## Descripción

Pipeline de datos Medallion (Bronze → Silver → Gold) construido íntegramente en local
usando Localstack para emular AWS sin coste. El objetivo es demostrar experiencia práctica
en Data Engineering con tecnologías del stack AWS real.

---

## Stack tecnológico

| Capa | Tecnología | Emulado con |
|---|---|---|
| Streaming | Amazon Kinesis | Localstack |
| Storage | Amazon S3 | Localstack |
| Processing | Python / pandas / pyarrow | local |
| Formato crudo | JSON Lines | — |
| Formato procesado | Apache Parquet | — |

---

## Arquitectura

```text
data/raw/ecommerce_customer_data.csv
  ↓
src/prod_kinesis.py  →  Kinesis Stream (ecommerce-stream)
  ↓
src/consumer_bronze.py
  ↓
s3://data-lake-bronze  (JSON Lines, inmutable, particionado por fecha)
  ↓
src/transform_silver.py
  ↓
s3://data-lake-silver  (Parquet, limpio, tipado, deduplicado)
  ↓
src/transform_gold.py
  ↓
s3://data-lake-gold    (Parquet, agregaciones de negocio)
```

---

## Estructura del proyecto

```text
lakehouse-local/
├── docker-compose.yml            # Configuración de Localstack
├── README.md
├── src/
│   ├── prod_kinesis.py           # Productor Kinesis
│   ├── consumer_bronze.py        # Consumidor → capa Bronze
│   ├── transform_silver.py       # Transformación → capa Silver
│   └── transform_gold.py         # Agregaciones → capa Gold
├── data/
│   ├── raw/                      # Dataset fuente (Kaggle)
│   ├── bronze/                   # Exportaciones crudas locales (opcional)
│   ├── silver/                   # Artefactos Parquet intermedios
│   └── gold/                     # Artefactos finales de negocio
├── notebooks/
│   ├── gold_analysis.ipynb       # Notebook de análisis en capa Gold
│   ├── marketing_segmentos.png   # Visualización para Marketing
│   ├── retencion_churn.png       # Visualización para Customer Success
│   ├── descuentos_efectividad.png # Visualización para Ventas
│   ├── generacional_gaps.png     # Visualización para Product / UX
│   └── clv_projection.png        # Visualización para C-Level
├── localstack-data/              # Estado local de Localstack (ignorado)
└── venv/                         # Entorno virtual Python (ignorado)
```

---

## Capas del pipeline

### Bronze
- Consume eventos desde Kinesis con `TRIM_HORIZON`
- Guarda los datos crudos e inmutables en S3
- Formato: JSON Lines, una línea por registro
- Particionado: `year=/month=/day=/`
- Metadata añadida: `_ingestion_ts`, `_source`
- Tamaño: ~522 KB para 2000 registros

### Silver
- Lee todos los ficheros Bronze del bucket
- Limpieza: deduplicación por `customer_id`, verificación de nulls en columnas críticas
- Tipado explícito de todas las columnas según esquema definido
- Formato: Apache Parquet (compresión automática)
- Tamaño: ~65 KB — reducción de 8x respecto a Bronze
- Metadata añadida: `_processed_ts`, `_bronze_source`

### Gold
Cinco tablas de negocio, cada una orientada a un caso de uso real:

| Tabla | Audiencia | Descripción |
|---|---|---|
| `marketing_segment_alto_valor` | Marketing | Perfilado de segmentos por spending score |
| `retencion_churn_prevention` | Customer Success | Churn por grupo de edad y antigüedad |
| `promociones_discount_effect` | Ventas | Efectividad de descuentos en retención |
| `product_generational_gaps` | Product / UX | Comportamiento digital por generación |
| `clevel_clv_projection` | C-Level | Proyección de Customer Lifetime Value |

---

## Dataset

- **Fuente**: Kaggle — Ecommerce Customer Data
- **Registros**: 2000 clientes
- **Columnas**: `customer_id`, `age`, `gender`, `annual_income`, `spending_score`,
  `membership_years`, `online_purchases`, `discount_usage`, `churn`

---

## Hallazgos principales

- El segmento de **spending medio tiene menor churn (0.29)** que el alto (0.31).
  Marketing debería priorizar retención en medio antes que captación en alto.
- Los **descuentos no fidelizan**: diferencia de churn entre tier bajo y alto es de solo 0.04.
  Mueven volumen puntual pero no construyen lealtad.
- **GenZ tiene el churn más bajo (0.27)** y el mayor volumen de compras online (99.92).
  Millennials presentan el churn más alto (0.33) sin diferencia en UX ni descuentos —
  el problema es de propuesta de valor, no de producto.
- En CLV, reducir churn en el segmento bajo (churn=0.39) tiene más impacto económico
  que adquirir nuevos clientes de alto valor.

---

## Visualizaciones exportadas (Gold)

Generadas desde `notebooks/gold_analysis.ipynb`:

| Archivo PNG | Audiencia principal | Insight principal |
|---|---|---|
| `notebooks/marketing_segmentos.png` | Marketing | Segmentación de clientes por valor y gasto |
| `notebooks/retencion_churn.png` | Customer Success | Patrones de churn por cohortes de retención |
| `notebooks/descuentos_efectividad.png` | Ventas | Impacto real del uso de descuentos en resultados |
| `notebooks/generacional_gaps.png` | Product / UX | Brechas de comportamiento por generación |
| `notebooks/clv_projection.png` | C-Level | Proyección comparativa de CLV por segmentos |

---

## Cómo ejecutar

```bash
# 1. Arrancar Localstack
cd ~/lakehouse-local && docker compose up -d

# 2. Activar entorno Python
source venv/bin/activate

# 3. Publicar eventos a Kinesis
python src/prod_kinesis.py

# 4. Consumir y guardar en Bronze
python src/consumer_bronze.py

# 5. Limpiar y escribir Silver
python src/transform_silver.py

# 6. Agregar y escribir Gold
python src/transform_gold.py
```

---

## Sincronizar capas a local (`data/`)

Forma recomendada:

```bash
cd ~/lakehouse-local
bash scripts/sync_layers.sh
```

Opcional (si quieres override de credenciales/endpoint):

```bash
cd ~/lakehouse-local

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
bash scripts/sync_layers.sh
```

El script usa `--delete` para mantener `data/` consistente con los buckets.

---

## Conceptos demostrados

- Patrón Producer / Consumer desacoplado con Kinesis
- Arquitectura Medallion (Bronze / Silver / Gold)
- Partition pruning con prefijos S3
- Formato columnar Parquet vs JSON Lines (8x compresión)
- Separación de compute y storage
- Agregaciones orientadas a negocio con casos de uso reales
- Emulación de AWS en local con Localstack para desarrollo sin coste

---

## Próximos pasos

- [ ] Data Quality layer con checks automáticos en S3
- [ ] Catálogo con AWS Glue Data Catalog
- [ ] Queries SQL sobre Gold con Amazon Athena
- [ ] CI/CD del pipeline con GitHub Actions
