# Stage 5 Recovery & Improvement Plan

## 1. Objectives
- Restore predictive power of Stage 5 models after Stage 4 metric expansion.
- Prevent silent regressions (zero trades / zero recall) via automated QA.
- Establish a repeatable tuning workflow for thresholds, features, and hyperparameters.

## 2. Workstreams

### A. Diagnostics & Data Integrity
- Audit label balance, feature distributions, and probability outputs per split.
- Generate confusion matrices and score histograms to understand threshold behaviour.
- Document findings and feed them into model/QA adjustments.

### B. Feature Engineering & Transforms
- Review scaling/normalisation for the new metrics; ensure diffs/lags are configured appropriately.
- Run permutation importance to confirm the added Stage 4 features contribute signal.
- Update the feature allowlist or transformations as required.

### C. Modeling & Threshold Tuning
- Introduce cost-sensitive training or class weighting where needed.
- Sweep decision thresholds per model (train/val) and persist chosen thresholds.
- Re-run hyperparameter optimisation for logreg, XGBoost, and CNN-LSTM.

### D. QA Automation & Reporting
- Add hard guards for zero recall, zero trades, or minimal exposure in evaluation/backtest outputs.
- Emit summary reports comparing current vs. previous runs.
- Integrate QA checks into the CLI so failing criteria stop the pipeline.

### E. Documentation & Governance
- Capture diagnostics, QA thresholds, and tuning decisions in SOT/Stage 5 notes.
- Ensure reproducibility: commit configs, random seeds, and tuning logs.

## 3. Milestones & Sequencing
1. **Week 1 (current)**: Complete diagnostics (Workstream A) and implement QA guards (Workstream D).
2. **Week 2**: Feature transform review + threshold sweeps (Workstreams B & C).
3. **Week 3**: Hyperparameter tuning, retrain models, and update docs (Workstreams C & E).

## 4. Immediate Next Actions
1. Add QA checks that raise exceptions when recall/exposure criteria are violated.
2. Export confusion matrix data per split to support diagnostics.
3. Schedule probability calibration review once QA is in place.
