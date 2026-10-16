// Display names of the lab's algorithms and baselines, shared by the server portal and the client charts.
const MODEL_LABEL: Record<string, string> = {
  "linear-regression": "Linear regression",
  "logistic-regression": "Logistic regression",
  "decision-tree": "Decision tree",
  "random-forest": "Random forest",
  gbt: "Gradient-boosted trees",
  "naive-bayes": "Naive Bayes",
  "linear-svc-ovr": "Linear SVM (one-vs-rest)",
  majority: "Majority class",
  "seasonal-naive": "Seasonal naive",
  "global-mean": "Global mean",
};

export const modelLabel = (id: string) => MODEL_LABEL[id] ?? id;
