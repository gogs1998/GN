from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

from .backtest import persist_backtest, register_backtest, run_backtest
from .baselines_core import load_predictions, train_model
from .boruta import BorutaRunner, load_selected_features
from .config import ModelConfig, load_model_config
from .eval import compute_split_metrics, generate_plots, save_metrics
from .frame import FrameArtifactError, FrameBuilder

app = typer.Typer(help="Stage 5 modeling CLI", add_completion=False)

DEFAULT_CONFIG_PATH = Path("config/models.yaml")


def _load_config(path: Optional[Path]) -> ModelConfig:
    config_path = path or DEFAULT_CONFIG_PATH
    try:
        return load_model_config(config_path)
    except Exception as exc:  # pragma: no cover - defensive
        typer.echo(f"Failed to load model config: {exc}")
        raise typer.Exit(code=1) from exc


def _ensure_frame(config: ModelConfig) -> FrameBuilder:
    builder = FrameBuilder(config)
    try:
        builder.load()
    except FileNotFoundError:
        typer.echo("Frame artifacts missing; building frame first.")
        bundle = builder.build()
        builder.persist(bundle)
        typer.echo(f"Persisted frame with {len(bundle.tabular)} rows.")
    except FrameArtifactError as exc:
        typer.echo(f"{exc}. Rebuilding frame artifacts.")
        bundle = builder.build()
        builder.persist(bundle)
        typer.echo(f"Persisted frame with {len(bundle.tabular)} rows.")
    return builder


@app.command()
def build_frame(config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path)) -> None:
    """Construct the modeling frame and persist artifacts."""

    config = _load_config(config_path)
    builder = FrameBuilder(config)
    try:
        bundle = builder.build()
    except Exception as exc:
        typer.echo(f"Frame build failed: {exc}")
        raise typer.Exit(code=2) from exc
    builder.persist(bundle)
    typer.echo(
        f"Frame persisted with {len(bundle.tabular)} rows, features={len(bundle.feature_columns)}."
    )


@app.command()
def run_boruta(config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path)) -> None:
    """Execute Boruta feature selection on the training split."""

    config = _load_config(config_path)
    builder = _ensure_frame(config)
    frame = builder.load()
    runner = BorutaRunner(config, frame)
    try:
        result = runner.run()
    except Exception as exc:
        typer.echo(f"Boruta run failed: {exc}")
        raise typer.Exit(code=3) from exc
    typer.echo(
        f"Boruta selected {len(result.selected_features)} features (hash={result.feature_hash})."
    )


@app.command()
def train(
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
    model: List[str] = typer.Option([], "--model", "-m"),
) -> None:
    """Train baseline models using the prepared frame."""

    config = _load_config(config_path)
    builder = _ensure_frame(config)
    frame = builder.load()
    selected = load_selected_features(config) if config.boruta.enabled else None
    if selected:
        typer.echo(f"Using {len(selected)} Boruta-selected features.")
    targets = model or config.models.enabled
    for name in targets:
        try:
            result = train_model(name, config, frame, selected_features=selected)
        except Exception as exc:
            typer.echo(f"Training {name} failed: {exc}")
            raise typer.Exit(code=4) from exc
        typer.echo(
            f"Trained {name}; model -> {result.model_path}, predictions -> {result.predictions_path}, signals -> {result.signals_path}."
        )


@app.command()
def evaluate(
    model: str = typer.Argument(..., help="Model name to evaluate"),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Compute evaluation metrics and diagnostic plots for a trained model."""

    config = _load_config(config_path)
    try:
        predictions = load_predictions(model, config)
    except Exception as exc:
        typer.echo(f"Failed to load predictions for {model}: {exc}")
        raise typer.Exit(code=5) from exc
    metrics = compute_split_metrics(predictions, config.decision.prob_threshold)
    out_dir = config.data.out_root / model / "eval"
    generate_plots(predictions[predictions["split"] != "train"], out_dir, model)
    metrics_path = out_dir / "metrics.json"
    save_metrics(metrics, metrics_path)
    typer.echo(f"Saved evaluation metrics to {metrics_path}")


@app.command()
def backtest(
    model: str = typer.Argument(..., help="Model name to backtest"),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Run the trading backtest and append results to the registry."""

    config = _load_config(config_path)
    try:
        predictions = load_predictions(model, config)
    except Exception as exc:
        typer.echo(f"Failed to load predictions for {model}: {exc}")
        raise typer.Exit(code=6) from exc
    try:
        result = run_backtest(model, predictions, config)
    except Exception as exc:
        typer.echo(f"Backtest failed: {exc}")
        raise typer.Exit(code=7) from exc
    result = persist_backtest(result, config)
    register_backtest(result, config)
    typer.echo(
        f"Backtest complete; equity saved to {result.equity_path}, summary -> {result.summary_path}."
    )


@app.command()
def full_run(
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Execute frame build, Boruta (if enabled), training, evaluation, and backtest sequentially."""

    config = _load_config(config_path)
    builder = _ensure_frame(config)
    frame = builder.load()
    selected = None
    if config.boruta.enabled:
        typer.echo("Running Boruta feature selection...")
        runner = BorutaRunner(config, frame)
        result = runner.run()
        selected = result.selected_features
        typer.echo(f"Boruta selected {len(selected)} features.")
    targets = config.models.enabled
    for name in targets:
        typer.echo(f"Training {name}...")
        run = train_model(name, config, frame, selected_features=selected)
        typer.echo(
            f"Artifacts: model -> {run.model_path}, predictions -> {run.predictions_path}, signals -> {run.signals_path}."
        )
        typer.echo("Evaluating...")
        metrics = compute_split_metrics(run.predictions, config.decision.prob_threshold)
        out_dir = config.data.out_root / name / "eval"
        generate_plots(run.predictions[run.predictions["split"] != "train"], out_dir, name)
        save_metrics(metrics, out_dir / "metrics.json")
        typer.echo("Running backtest...")
        backtest_result = run_backtest(name, run.predictions, config)
        backtest_result = persist_backtest(backtest_result, config)
        register_backtest(backtest_result, config)
        typer.echo(f"Pipeline complete for {name}.")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
