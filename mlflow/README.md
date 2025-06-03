# MLflow Component

This component provides experiment tracking and model management for the bus congestion prediction system using MLflow.

## Overview

The MLflow server is configured to use PostgreSQL as its backend store, providing robust storage for:

- Experiment metadata
- Run data
- Parameters
- Metrics
- Tags

## Configuration

The MLflow server runs with the following settings:

- Host: `0.0.0.0`
- Port: `5001`
- Backend Store: Dedicated PostgreSQL database (mlflow-db)
- Database URI: `postgresql://postgres:example@mlflow-db:5432/mlflow`
- Artifact Root: `/mlflow/artifacts`

### Database Configuration

The dedicated MLflow PostgreSQL instance (mlflow-db) runs with:

- Port: 5432 (internal container port)
- Database Name: mlflow
- Username: postgres
- Health Check: Enabled with pg_isready

## Storage Architecture

### Backend Store (PostgreSQL)

- Uses a dedicated PostgreSQL instance (mlflow-db)
- Stores all metadata about experiments, runs, and metrics
- Provides reliable and scalable storage for experiment tracking
- Enables efficient querying of experiment results
- Runs independently from the main application database

### Artifact Storage

The `artifacts/` directory contains:

- Saved models
- Model checkpoints
- Additional files generated during training
- Evaluation plots and metrics
- Mounted as a volume: `./mlflow/artifacts:/mlflow/artifacts`

## Usage

The MLflow server is automatically started as part of the Docker composition. You can access the MLflow UI at:

```
http://localhost:5001
```

### Tracking Experiments

When running experiments, the following information is tracked:

- Model parameters
- Training metrics
- Model artifacts
- Environment information

## Integration

This MLflow component integrates with:

- The ML model training pipeline
- Model serving infrastructure
- The prediction service for real-time congestion forecasting

## Docker Configuration

The component uses the official MLflow image and includes:

- PostgreSQL support via `psycopg2-binary`
- Automatic database connection setup
- Exposed port 5001 for web interface access
