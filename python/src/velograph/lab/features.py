"""The ML table, its splits and its feature dictionary, as FeatureBuildJob wrote them.

See ARCHITECTURE §8.2 (windows) and §8.3 (features).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from pyspark.ml import Estimator, Transformer
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from velograph.common.settings import data_paths

Stage = Estimator | Transformer  # a Pipeline stage


@dataclass(frozen=True)
class Feature:
    name: str
    group: str
    kind: str  # numeric | categorical | flag
    description: str


@dataclass(frozen=True)
class Dictionary:
    """The feature dictionary of ``serving/ml-features.json``, plus what the job decided (tau, windows)."""

    features: tuple[Feature, ...]
    tau: float
    report: dict[str, Any]

    def names(self, *, kinds: Iterable[str] | None = None, drop_groups: Iterable[str] = ()) -> list[str]:
        """Feature names in dictionary order, optionally only some kinds and without some groups."""
        wanted = set(kinds) if kinds is not None else None
        dropped = set(drop_groups)
        return [
            f.name for f in self.features if f.group not in dropped and (wanted is None or f.kind in wanted)
        ]

    @property
    def groups(self) -> list[str]:
        return list(dict.fromkeys(f.group for f in self.features))


def load_dictionary() -> Dictionary:
    report = json.loads((data_paths().serving / "ml-features.json").read_text(encoding="utf-8"))["data"]
    features = tuple(Feature(**f) for f in report["features"])
    return Dictionary(features=features, tau=float(report["tau"]), report=report)


@dataclass(frozen=True)
class Splits:
    """The time-based splits. Models are chosen on `validation`; `test` is only ever reported."""

    train: DataFrame  # fit rows before the validation week
    validation: DataFrame  # the last fit week
    fit: DataFrame  # all fit rows, for the final refit of each chosen configuration
    test: DataFrame  # September


def load_splits(spark: SparkSession) -> Splits:
    rows = spark.read.parquet((data_paths().lake / "gold" / "ml_features").as_uri())
    fit = rows.where(F.col("split") == "fit").persist(StorageLevel.MEMORY_AND_DISK)
    test = rows.where(F.col("split") == "test").persist(StorageLevel.MEMORY_AND_DISK)
    return Splits(
        train=fit.where(~F.col("is_validation")),
        validation=fit.where(F.col("is_validation")),
        fit=fit,
        test=test,
    )


def prep_stages(d: Dictionary, *, linear: bool, drop_groups: Iterable[str] = ()) -> list[Stage]:
    """Feature preparation as Pipeline stages ending in a ``features`` vector.

    Categorical columns go through StringIndexer (unseen values get their own index). Tree models read
    the indices directly: the indexer's metadata marks them as categorical, so a split can group
    categories. Linear models need one-hot vectors instead, or a district id would be read as a
    quantity. Numeric and flag columns are assembled as they are; LinearRegression, LogisticRegression
    and LinearSVC standardise internally.
    """
    dropped = tuple(drop_groups)
    categorical = d.names(kinds={"categorical"}, drop_groups=dropped)
    plain = d.names(kinds={"numeric", "flag"}, drop_groups=dropped)
    stages: list[Stage] = []
    inputs = list(plain)
    if categorical:
        indexed = [f"{c}_idx" for c in categorical]
        stages.append(StringIndexer(inputCols=categorical, outputCols=indexed, handleInvalid="keep"))
        if linear:
            encoded = [f"{c}_ohe" for c in categorical]
            stages.append(OneHotEncoder(inputCols=indexed, outputCols=encoded, handleInvalid="keep"))
            inputs += encoded
        else:
            inputs += indexed
    stages.append(VectorAssembler(inputCols=inputs, outputCol="features"))
    return stages
