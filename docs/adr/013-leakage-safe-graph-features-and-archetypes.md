# ADR 013 — Leakage-safe graph features and archetypes

- **Status:** accepted, 16 Oct 2026. Refines ARCHITECTURE §8.2–§8.3 (it was written before the features existed).

## Context

ARCHITECTURE §8.3 lists GraphX features (PageRank, AM/PM PageRank, strengths, imbalance, clustering
coefficient, community) and a station archetype among the model inputs. §8.2 requires every profile window to
end before the rows it feeds.

Two things clashed with that:

- **The P3 graph covers June to September.** `graph/station_metrics` was built from every trip of the
  four months, including the test month. Joining it onto September rows would hand the model a station's
  September popularity, measured from September itself.
- **Arbitrary labels.** KMeans cluster numbers and label-propagation district numbers mean nothing outside the run
  that made them. Computed separately for the fit and test windows, "archetype 1" or "district 3" could name
  different things in August and September rows, and a model trained on one would misread the other.

## Decision

1. **Graph features are recomputed for each profile window** by `GraphFeatures`. It uses the same algorithms as
   the P3 suite, on the routes of that window only:
   - **Fit rows:** profile A, 6 Jun – 31 Jul.
   - **Test rows:** profile B, 7 Jul – 31 Aug.
   - **Strong-tie threshold:** "≥ 20 trips" was set for four months, so it is scaled to the window's length
     (≥ 9 trips over 8 weeks).
2. **Districts are aligned across windows.** Each test-window district takes the id of the fit-window district
   holding at least half of its stations, and otherwise a new id. In the run, 14 of 14 matched.
3. **Archetypes are learned once and applied twice.** KMeans, the cluster names and the PCA view are fitted
   on profile A. The same model then assigns every station for profile A and for profile B, so an archetype id
   means the same in both splits.
4. **How the clusters are found:**
   - **Representation:** the 96 hourly shares (departures and arrivals, on working days and days off) are summed
     into 3-hour bins. Eight weeks give only eight samples per weekday hour, which makes single hours noisy.
   - **Restarts:** each k takes the best of 5 KMeans runs by within-cluster cost.
   - **Choice of k:** the best silhouette among the k whose smallest cluster holds at least 2 % of stations.
   - **Names:** given by stated rules (`ArchetypeTransforms.name`) applied to the clusters' mean hourly curves.
5. **Two leakage checks, two levels.**
   - `MlConfig` refuses to construct a profile window that does not end before its rows start.
   - `FeatureBuildJob` checks the same on the data it actually read: the last profile day against the first row
     day.

## Consequences

- **Cost.** The feature build runs GraphX twice: 7.2 min in all.
- **Honest ablation.** The graph features in the ablation (Lab 04) are themselves leakage-free, so what they add
  is real.
- **Silhouette stays low.** Station rhythms form a continuum rather than tight groups, so the best silhouette
  is 0.21 (k = 3). The archetypes are a useful summary, not natural kinds, and the portal says so.

## Alternatives considered

- **Use `graph/station_metrics` as it is.** Rejected: September would leak into the test features.
- **One graph window for both splits (June–August).** Rejected: the fit rows' features would then include the
  fit rows' own month.
- **Clustering the 96 hourly slots directly.** Tried first. Its silhouette was 0.13 at best, and with one seed it
  could isolate a sliver of 11 outlier stations.
