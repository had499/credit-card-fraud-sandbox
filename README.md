# Credit Card Fraud Detection Pipeline

This project demonstrates a real-time fraud detection system using machine learning and streaming data. The setup includes model training with MLflow, real-time inference via Kafka, and a WebSocket API for live predictions.

## What's Inside

The system is split into two main workflows: training and serving. Both run in Docker containers to keep dependencies isolated.

### Training Phase

Run `docker compose -f docker-compose-train.yml up` to start the training environment. This brings up:

- **Jupyter notebook server** on port 8888 for exploratory work
- **MLflow tracking server** on port 5000 to log experiments
- Training notebooks in `services/training/notebooks/` with EDA and modeling steps

#### Notebook 1: Exploratory Data Analysis (`01-eda.ipynb`)

The dataset comes from Kaggle with 27 principal components from PCA transformation (anonymised for privacy). Even though we don't have the original feature names, the EDA focuses on:

- **Class imbalance**: Fraud is extremely rare, which affects both visualization and modeling
- **Distribution analysis**: Violin plots show per-class percentiles for each PC to identify which components discriminate fraud vs legitimate transactions
- **Quantile comparisons**: Plotting quantiles side-by-side reveals where fraud and non-fraud diverge most
- **Temporal patterns**: Converting the `Time` column to hour-of-day shows fraud clusters at specific times, particularly during early morning hours
- **Spatial-temporal density**: KDE plots of principal components across different time intervals reveal that fraud patterns shift throughout the day

The key insight: fraud isn't just different in feature space, it has temporal structure. This motivates adding time-based features in the modeling phase.

#### Notebook 2: Modeling (`02-modelling.ipynb`)

The modeling approach makes several deliberate choices:

**Custom business cost function**: Standard metrics like accuracy or F1 don't capture the real cost of fraud detection. The custom scorer:
- Rewards catching fraud (`reward_tp = amount`)
- Penalises false alarms but only slightly (`cost_fp = 0.1 * amount`)
- Heavily penalises missing fraud (`cost_fn = 1.0 * amount`)
- Small reward for correctly identifying legitimate transactions (`reward_tn = 0.01 * amount`)

This weights errors by transaction amount, so missing a $5000 fraudulent charge hurts more than flagging 10 small legitimate ones for review.

**Feature engineering based on EDA findings**:
- `Hour`: Extracted from time since fraud shows temporal clustering
- `delta_time`: Time between transactions (unusual gaps might indicate fraud)
- Rolling statistics for top 5 PCs: mean, std, min, max over previous 5 transactions
- Lag features: Previous transaction's PC values
- `amount_rolling_sum`: Total spend in recent window

These capture behavioral patterns that static PCA features miss.

**Chronological train/test split**: Uses 70/30 split but respects time ordering. Random splitting would leak future information into training, which doesn't reflect real-world deployment where you only know the past.

**Two modeling approaches tested**:

1. **Logistic Regression**: Baseline model with StandardScaler preprocessing. GridSearchCV tunes the regularisation parameter `C` using TimeSeriesSplit (3 folds) to maintain temporal ordering during CV. After finding best C, uses `TunedThresholdClassifierCV` to optimise the decision threshold specifically for the business cost function.

2. **HistGradientBoostingClassifier**: More powerful model that handles the imbalanced data better. Uses `HalvingRandomSearchCV` to efficiently search a large hyperparameter space (500 candidates across learning rate, tree depth, regularization, etc.). The halving strategy progressively eliminates poor candidates by doubling resources (max_iter) for promising ones.

**Threshold tuning**: Both models generate probability scores. Default 0.5 threshold isn't optimal for this business problem. The notebooks explore amount-weighted precision-recall curves to visualise the tradeoff. Tuned thresholds (0.1 for logistic, 0.04 for gradient boosting) catch more fraud dollars at acceptable false positive rates.

**Results tracking**: Every trained model logs to MLflow with:
- All hyperparameters
- Business cost score on test set
- Model artifact with signature for serving
- Tags indicating model type and tuning approach

Models get logged to MLflow with their metrics and artifacts. Pick whichever run performs best for deployment.

### Serving Phase

Once we get a trained model, `docker compose -f docker-compose-serve.yml up` starts the inference pipeline:

- **MLflow serving** (port 1234) hosts the model for predictions
- **Kafka + Zookeeper** for streaming transaction data
- **Producer API** (port 8001) sends test transactions to Kafka
- **Consumer** pulls from Kafka, calls the model, and pushes results to WebSocket
- **WebSocket server** (port 8002) broadcasts predictions to connected clients
- **Dashboard** (port 3000) shows real-time fraud alerts

The producer API (`services/kafka-producer/`) has endpoints to simulate transaction streams with configurable fraud rates. The consumer (`services/kafka-consumer/`) grabs each transaction, gets a prediction from MLflow, and forwards it through WebSocket for anyone listening.

## Data

Uses the standard Kaggle credit card fraud dataset (`data/raw/creditcard.csv`). Features are PCA-transformed, but the notebook adds temporal features like hour of day and rolling statistics over recent transactions.

Test sets (`X_test.csv`, `y_test.csv`) are pre-split and used by the producer to generate realistic transaction streams.

## Quick Start

Training:
```bash
docker-compose -f docker-compose-train.yml up
```

Access notebooks at http://localhost:8888 and MLflow UI at http://localhost:5000.

Serving (after training):
```bash
docker-compose -f docker-compose-serve.yml up
```

Producer API docs at http://localhost:8001/docs. Send transactions via POST to `/produce` with parameters for volume and fraud distribution.

Dashboard at http://localhost:3000 shows incoming predictions in real-time.

## Architecture Notes

The Kafka setup uses a single broker with Zookeeper for simplicity. Topic is `transactions` by default.

WebSocket server routes predictions by MLflow run ID, so multiple model versions can broadcast simultaneously to different dashboard instances.

Memory limits are set in `docker-compose-serve.yml` to keep resource usage reasonable during demos.

## File Layout

```
services/
  training/          # Jupyter notebooks and training dependencies
  mlflow/            # MLflow server config
  mlflow-serve/      # Model serving container
  kafka-producer/    # Transaction generator API
  kafka-consumer/    # Inference pipeline
  websocket/         # Real-time broadcast server
```

Each service has its own Dockerfile and requirements.
