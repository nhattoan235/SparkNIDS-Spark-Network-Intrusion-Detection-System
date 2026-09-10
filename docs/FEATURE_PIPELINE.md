# Spark ML feature pipeline — Phase 4

Sinh lúc: `2026-09-10T20:22:27+07:00`.

## Chia dữ liệu và chống leakage

- Silver train gốc: **175341** dòng.
- Modeling train: **140471** dòng.
- Validation: **34870** dòng.
- Designated test giữ nguyên **82332** dòng và không được transform/evaluate.
- Giao nhau `id` train/validation: **0**.
- Cách chia: `pmod(xxhash64(id, seed), hash_buckets)`, seed `42`.

Mọi estimator (`StringIndexer`, `OneHotEncoder`, `StandardScaler`, classifier) chỉ được `fit()` trên modeling train. Validation chỉ đi qua `PipelineModel.transform()`; `attack_cat` và `id` không nằm trong feature vector.

## Pipeline smoke

- Số stages: **8**.
- Stages: `StringIndexerModel`, `StringIndexerModel`, `StringIndexerModel`, `OneHotEncoderModel`, `VectorAssembler`, `StandardScalerModel`, `VectorAssembler`, `LogisticRegressionModel`.
- Feature vector dimension: **199**.
- Validation transformed: **34870** dòng.
- Thời gian fit smoke LR 1 iteration: **14.967 giây**.

Smoke model chỉ chứng minh pipeline kỹ thuật và chưa phải baseline được đánh giá. Hyperparameter, threshold và metrics sẽ được xử lý ở Phase 5.
