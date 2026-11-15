# Exploratory analysis: validated flights (gold_flights)

Source: `databricks/lab6/04_inspect_flights.py`, outputs exported to `evidence/lab6/04_inspect_flights.html`.
**No sample was used.** Every figure below is computed over the full validated table (3,944,915 flights) or the full bronze table (4,078,318 rows).

## Population counts

| Stage | Rows | Share of raw |
|---|---|---|
| Raw CSV = bronze | 4,078,318 | 100% |
| Excluded by design (cancelled or diverted) | 133,402 | 3.27% |
| Rejected as invalid | 0 | 0% |
| Duplicate copies removed | 0 | 0% |
| Quarantined (no usable scheduled elapsed time) | 1 | 0.00002% |
| **Gold (validated, published)** | **3,944,915** | **96.73%** |

The accounting balances: 4,078,318 = 133,402 + 0 + 0 + 1 + 3,944,915 (run output, `4.1`).

| Month | Bronze | Gold | Kept |
|---|---|---|---|
| Jan | 563,737 | 526,893 | 93.5% |
| Feb | 519,952 | 495,651 | 95.3% |
| Mar | 590,542 | 579,953 | 98.2% |
| Apr | 580,290 | 565,501 | 97.5% |
| May | 602,950 | 589,376 | 97.8% |
| Jun | 602,057 | 581,836 | 96.6% |
| Jul | 618,790 | 605,705 | 97.9% |

| Split | Flights | Delay rate |
|---|---|---|
| train (Jan to Apr) | 2,167,998 | 20.70% |
| validation (May) | 589,376 | 21.03% |
| test (Jun) | 581,836 | 23.90% |
| deploy (Jul) | 605,705 | 23.46% |

## Target

Overall, 21.65% of flights arrive 15 or more minutes late (about 1 in 4.6). By month the rate ranges from 19.46% (Jan) to 23.90% (Jun).
The rate rises over the period, so the test month (23.9%) is harder to beat than the training months (20.7%) in absolute terms.

## What drives delays (rates are share of flights arriving 15+ minutes late)

* **Scheduled departure hour is the clearest pattern.** The rate climbs through the day from 9.0% at 05:00 to 31.7% at 21:00, then falls (29.1% at 22:00, 23.5% at 23:00). Hours 0 to 4 hold only 12,135 flights in total (between 340 and 8,009 each), so their rates are noisy.
* **Carrier matters.** Among the 15 largest carriers the rate ranges from 16.5% (9E) to 35.8% (G4). Southwest has the most flights (712,246, 24.9% late); B6 (33.6%), F9 (32.2%) and NK (26.8%) are high; OO (17.1%), DL (17.4%) and MQ (16.8%) are low.
* **Day of week:** lowest on Tuesday (17.4%) and Wednesday (18.3%), highest on Friday (25.4%).
* **Distance:** 18.8% under 250 miles, rising to 24.6% at 1,000 to 1,999 miles, then 20.7% beyond 2,000. The relation is not a straight line.

## Correlation with the target

| Known two hours before departure | Corr | Known only after departure (leakage) | Corr |
|---|---|---|---|
| CRSDepTime | 0.159 | DepDel15 | 0.746 |
| DayOfWeek | 0.041 | DepDelay | 0.511 |
| Month | 0.034 | DepDelayMinutes | 0.495 |
| Distance | 0.026 | TaxiOut | 0.235 |
| CRSElapsedTime | 0.021 | | |

## Outliers and data risks

* **Delay minutes have an extreme tail:** median 0, 90th percentile 45, 99th percentile 200, maximum 7,232 minutes (about 5 days). The label is unaffected (any delay of 15 or more is 1), but a delay-minutes regression would be. Tail values from `approxQuantile` are approximate.
* Distance (31 to 5,095 miles) and scheduled elapsed time (17 to 690 minutes) look plausible.
* **Who is lost in cleaning:** the share of each carrier's flights that reach gold ranges from 92.8% (GoJet) to 99.1% (Hawaiian). Cancelled flights are not predicted by this model, so it describes completed flights only. The lower share in January and February may reflect more winter cancellations; the data here does not prove that.
* The dataset has no weather, no time zones, and covers January to July 2022 only.

## What this means for modeling

1. **Use PR-AUC, not accuracy.** About 1 in 5 flights is late. The no-skill PR-AUC equals the delay rate of the split being scored (about 0.24 on the test month), and that value should be reported next to the baseline.
2. **Expect a modest gain from schedule-only features.** The strongest linear correlation is 0.16. The hour pattern is not a straight line, which favours a tree model, but the logistic baseline should also be given the hour as a category, otherwise the comparison is unfair.
3. **Do not use post-departure columns.** `DepDel15` alone correlates 0.75 with the target. This is why they are excluded (see `FEATURES.md`).
4. **The test month differs from training** (23.9% against 20.7% late), so a small drop or rise in a metric may come from the shift, not the model.
5. **Cold-start and sparse groups:** early-morning hours and small routes have few flights, so history features are empty for them. Report this when the model is trained.
