"""Model metrics, each computed in one pass over the predictions."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def regression(pred: DataFrame, label: str, prediction: str = "prediction") -> dict[str, float]:
    """MAE, RMSE and R² = 1 - MSE / Var(label) (both over the same rows)."""
    err = F.col(prediction) - F.col(label)
    row = pred.agg(
        F.avg(F.abs(err)).alias("mae"),
        F.avg(err * err).alias("mse"),
        F.var_pop(label).alias("var"),
        F.count(F.lit(1)).alias("n"),
    ).first()
    assert row is not None
    mse, var = float(row["mse"]), float(row["var"])
    return {
        "mae": round(float(row["mae"]), 5),
        "rmse": round(mse**0.5, 5),
        "r2": round(1.0 - mse / var, 5) if var > 0 else 0.0,
        "rows": int(row["n"]),
    }


def confusion(
    pred: DataFrame, label: str, classes: Sequence[int], prediction: str = "prediction"
) -> list[list[int]]:
    """matrix[actual][predicted], in the order of `classes`."""
    counts = {
        (int(r[label]), int(r[prediction])): int(r["count"])
        for r in pred.groupBy(label, prediction).count().collect()  # bounded: classes x classes
    }
    return [[counts.get((a, p), 0) for p in classes] for a in classes]


def classification(matrix: list[list[int]], classes: Sequence[int]) -> dict[str, object]:
    """Accuracy, macro-F1 and per-class precision, recall and F1 from a confusion matrix."""
    total = sum(map(sum, matrix))
    per_class = []
    for i, c in enumerate(classes):
        tp = matrix[i][i]
        predicted = sum(row[i] for row in matrix)
        actual = sum(matrix[i])
        precision = tp / predicted if predicted else 0.0
        recall = tp / actual if actual else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_class.append(
            {
                "class": c,
                "precision": round(precision, 5),
                "recall": round(recall, 5),
                "f1": round(f1, 5),
                "support": actual,
            }
        )
    return {
        "accuracy": round(sum(matrix[i][i] for i in range(len(classes))) / total, 5),
        "macroF1": round(sum(p["f1"] for p in per_class) / len(per_class), 5),
        "perClass": per_class,
        "rows": total,
    }
