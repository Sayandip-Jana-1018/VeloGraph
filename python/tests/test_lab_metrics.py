"""The lab's classification metrics from a confusion matrix (as ModelMetrics in Scala)."""

import pytest

from velograph.lab.metrics import classification

CLASSES = [0, 1, 2]


def test_perfect_predictions_score_one():
    m = classification([[5, 0, 0], [0, 7, 0], [0, 0, 3]], CLASSES)
    assert m["accuracy"] == 1.0
    assert m["macroF1"] == 1.0
    assert [c["support"] for c in m["perClass"]] == [5, 7, 3]


def test_macro_f1_weighs_every_class_alike():
    # actual 0: one right, one taken for 1; actual 1: all three right; actual 2: taken for 1
    m = classification([[1, 1, 0], [0, 3, 0], [0, 1, 0]], CLASSES)
    assert m["accuracy"] == pytest.approx(4 / 6, abs=1e-5)
    f1_draining = 2 * 1.0 * 0.5 / 1.5  # precision 1, recall 1/2
    f1_stable = 2 * 0.6 * 1.0 / 1.6  # precision 3/5, recall 1
    assert m["macroF1"] == pytest.approx((f1_draining + f1_stable + 0.0) / 3, abs=1e-5)


def test_always_predicting_the_majority_is_accurate_but_has_low_macro_f1():
    m = classification([[0, 14, 0], [0, 72, 0], [0, 14, 0]], CLASSES)
    assert m["accuracy"] == 0.72
    assert m["macroF1"] < 0.3
