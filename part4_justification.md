# Part 4.1 - Performance Monitoring Flag System: Rationale

We chose the three common countermovement jump (CMJ) measurements to monitor using our flagging system: Modified Reactive Strength Index (mRSI), Jump Height (m), and Propulsive Net Impulse (N·s). We chose these metrics because they can be found frequently in the body of force plate based measurement studies on the CMJ, and there is high quality research that shows them to have good reliability.

All thresholds were set at levels that exceeded known variability in test/re-test and reflect meaningful changes in performance (i.e., changes that would be caused by fatigue, sub-optimal recovery, etc.) rather than the day-to-day "noise" that may occur due to factors like training load, nutrition, sleep, etc.

## 1. mRSI Change ≥10% vs Baseline (mRSI_flag)

Our first threshold looks for meaningful changes within each athlete. mRSI is functionally identical to reactive strength index - modified (RSImod), which has established reference values for Division I athletes (Sole et al., 2018) that range from low (<~0.20) to elite (>~0.60+). Suchomel et al. (2015) reported typical errors (TE) for RSImod of approximately 7.5-9.3%, indicating that changes greater than ~8% would likely be too large to be considered random/normal variations. Therefore, a 10% decrease from an athlete's median baseline is a reasonable indicator of potential neuromuscular fatigue. The median is used to reduce the impact of poor jumps occurring in isolation.

## 2. mRSI >15% Deviation from Team Average (mRSI_team_flag)

In addition to the within-athlete flag, we created a between-athlete flag that looks for deviations of more than 15% from the team's average mRSI. This will allow us to look for values that are significantly lower (or higher) than those of their teammates. This threshold was intentionally set larger than the TE for RSImod so we could be confident that the values we identified were true deviations from the rest of the team.

## 3. ≥7% Decrease in Jump Height (jh_flag)

Jump Height is one of the most widely used CMJ outcome measures. Anićic et al. (2023) showed that jump height has excellent reliability (CV ~3–5%) and that RSImod also demonstrates acceptable reliability (CV ~8–9%), making both appropriate for monitoring changes in neuromuscular performance. Since the normal day-to-day variation in jump height is very small, a ≥7% decrease from an athlete's median baseline is a meaningful decrease in an athlete's ability to explosively jump, and is therefore well beyond what you would expect as normal variation.

## 4. ≥7% Decrease in Propulsive Net Impulse (pni_flag)

Anićic et al. (2023) reported that impulse-related and propulsive-phase variables are among the most reliable CMJ force-time metrics and contribute strongly to the ‘performance’ and ‘concentric’ components in their factor analysis. Propulsive Net Impulse is a key factor in determining vertical jump performance and represents how effectively an athlete produces force over time. By applying the same ≥7% decrease in threshold, we can capture decreases in mechanical output that may occur before or after a decrease in jump height, thereby providing an additional indicator of decreasing neuromuscular performance.

## Practical Application

The script will create a file titled `part4_flagged_athletes.csv` containing all test events that have been flagged, along with columns for `playername`, `team`, `flag_reason`, `metric_value`, and `last_test_date`. Collectively, these thresholds provide a structured, evidence-based method for detecting significant changes that may indicate fatigue, suboptimal recovery, or decreased neuromuscular performance.

## References

* Anićic, Z., Janicijevic, D., Knezevic, O. M., Garcia-Ramos, A., Petrovic, M. R., Cabarkapa, D., & Mirkov, D. M. (2023). Assessment of countermovement jump: What should we report? Life, 13(1), 190. https://pubmed.ncbi.nlm.nih.gov/36676138/

* Sole, C. J., Mizuguchi, S., Sato, K., Moir, G. L., & Stone, M. H. (2018). Preliminary scale of reference values for evaluating reactive strength index-modified in male and female NCAA Division I athletes. Sports, 6(4), 133. https://pubmed.ncbi.nlm.nih.gov/30380639/

* Suchomel, T. J., Bailey, C. A., Sole, C. J., Grazer, A. M., Beckham, G. K., & Grazer, J. L. (2015). Using reactive strength index-modified as an explosive performance measurement tool in Division I athletes. Journal of Strength and Conditioning Research, 29(4), 899–904. https://pubmed.ncbi.nlm.nih.gov/25426515/