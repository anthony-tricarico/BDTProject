# Model Evaluation Service

This component is responsible for evaluating machine learning models and managing the champion/challenger model pattern. It receives model metadata from Kafka, evaluates models against the current champion, and promotes better-performing models.

## Overview

The evaluation service:

1. Listens for new model notifications on Kafka
2. Downloads models from MinIO storage
3. Evaluates model performance
4. Promotes better-performing models as champions
5. Maintains model versioning

## Features

- **Model Evaluation**: Compares new models against the current champion
- **Champion Management**: Promotes better-performing models
- **Continuous Monitoring**: Listens for new models in real-time
- **Model Storage**: Manages model versions in MinIO
- **Performance Tracking**: Maintains accuracy metrics for all models

## Technical Details

### Dependencies

- Python 3.10
- Required packages:
  - scikit-learn
  - kafka-python
  - minio
  - joblib
  - boto3

### Configuration

The service connects to:

- Kafka broker (`kafka:9092`)
- MinIO storage (`minio:9000`)

### Model Evaluation Process

1. **Message Reception**:

   - Listens on topic: `model.train.topic`
   - Processes messages containing model metadata

2. **Model Download**:

   - Retrieves model files from MinIO
   - Loads models using joblib

3. **Evaluation**:
   - Compares model accuracy
   - Promotes better-performing models
   - Maintains champion status

## Usage

The service runs automatically in the Docker environment. It:

1. Connects to Kafka and MinIO
2. Listens for new model notifications
3. Evaluates and promotes models as needed

### Manual Testing

To test the service manually:

```bash
docker-compose up kafka-consumer-model
```

### Logs

Monitor the service logs:

```bash
docker-compose logs -f kafka-consumer-model
```

## Error Handling

The service includes robust error handling:

- Manages Kafka connection issues
- Handles MinIO storage errors
- Processes malformed messages
- Recovers from evaluation failures

## Integration

This component works in conjunction with:

- `ml-model`: Source of new models
- `minio`: Model storage
- `kafka`: Message broker for model notifications

## Model Management

### Champion Model

- Stored in MinIO at: `models/champion/champion.pkl`
- Represents the best-performing model
- Used as the baseline for new model evaluation

### Challenger Models

- Stored in MinIO at: `models/challengers/challenger_{timestamp}.pkl`
- Evaluated against the champion
- Promoted if performance exceeds champion

## Performance Metrics

The service tracks:

- Model accuracy
- Evaluation timestamps
- Champion promotion history
- Model versioning
