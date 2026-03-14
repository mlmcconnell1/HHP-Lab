# Claims Audit Plan for the Glynn-Fox Homelessness Model

## Purpose

This document defines the claims-audit strategy for evaluating the substantive
conclusions of the Glynn and Fox (2019) Bayesian state-space model of
homelessness dynamics. It complements `background/audit_panel_specs.md`, which
governs panel construction and structural validation.

This plan covers:

- the research claims under audit;
- the inference framework and sampler settings;
- count-accuracy trajectory assumptions;
- posterior diagnostics thresholds;
- claim-classification rules after fitting.

## Research Claims Under Audit

The audit evaluates five claim categories derived from the paper:

### Q1: Trend Claim

**Claim:** Adjusting for count accuracy reveals a different trajectory of
homelessness rates than the raw PIT counts suggest.

**Evaluation:** Compare posterior estimates of true homelessness rate
trajectories against raw PIT-derived rates. Report the direction, magnitude,
and statistical certainty of discrepancies.

### Q2: Rent-Association Claim

**Claim:** Year-over-year increases in the Zillow Rent Index predict increases
in homelessness.

**Evaluation:** Examine the posterior distribution of the rent coefficient
(`phi_i`) across units. Report the proportion of units with credibly positive
rent effects and the aggregate (`phi_bar`) estimate.

### Q3: Uncertainty Quantification Claim

**Claim:** PIT counts carry substantial measurement uncertainty that is
quantifiable through the state-space framework.

**Evaluation:** Report the width and coverage of posterior credible intervals
for count-accuracy parameters across units and years.

### Q4: Latent Total Claim

**Claim:** The model produces expected ranges for the unobserved true homeless
population.

**Evaluation:** Report posterior predictive intervals for latent totals and
compare against observed PIT counts. Flag units where the latent total implies
implausible count-accuracy ratios.

### Q5: Forecast Claim

**Claim:** One-year-ahead forecasts from the model are informative.

**Evaluation:** Hold out the final year of the panel, fit on the remainder, and
compare posterior predictive distributions against held-out observations.

## Inference Framework

### Model Specification

The audit uses the Glynn-Fox Bayesian state-space model as described in the
original paper (Glynn and Fox 2019, Section 3). The model relates observed PIT
counts to a latent true homeless population through a count-accuracy parameter.

### Sampler Settings

| Setting | Value | Rationale |
|---------|-------|-----------|
| Sampler | NUTS (via NumPyro or Stan) | Standard for continuous state-space models |
| Warmup iterations | 1000 | Standard NUTS default |
| Sampling iterations | 2000 | Sufficient for convergence diagnostics |
| Chains | 4 | Minimum for R-hat diagnostics |
| Target accept | 0.90 | Moderately conservative for hierarchical models |
| Max tree depth | 10 | Default; increase if divergences appear |

These settings should be treated as defaults. If diagnostics indicate problems
(divergences, low ESS, high R-hat), adjustments should be documented in the
run manifest.

## Count-Accuracy Trajectory

### Current Configuration

The audit panels use a constant count-accuracy trajectory:

| Parameter | Value | Source |
|-----------|-------|--------|
| `expected_pi` | 0.60 | Glynn and Fox (2019), reflecting literature consensus that PIT counts capture roughly 60% of the true homeless population |
| `beta_variance` | 0.01 | Moderately informative prior; allows unit-level variation |

### Derived Beta Prior

From the configured `expected_pi` and `beta_variance`:

```
common = expected_pi * (1 - expected_pi) / beta_variance - 1
alpha  = expected_pi * common  = 14.4
beta   = (1 - expected_pi) * common = 9.6
```

This yields a Beta(14.4, 9.6) prior centered at 0.60 with moderate
concentration. The prior is the same for all unit-years under the constant
trajectory.

### Alternative Trajectories

Future audit iterations may explore:

- **Trending trajectory:** `expected_pi` increasing over time as counting
  methodology improves.
- **Unit-varying trajectory:** Different `expected_pi` by geography type
  (e.g., metros with strong HMIS integration vs. those without).

Any alternative trajectory must be documented in the run manifest and compared
against the constant-trajectory baseline.

## Posterior Diagnostics Thresholds

These thresholds govern whether a completed inference run is considered
diagnostically valid for claim evaluation. They are applied *after* the
structural gate defined in `background/audit_panel_specs.md`.

### Required Diagnostics

| Diagnostic | Threshold | Action on Failure |
|------------|-----------|-------------------|
| R-hat (split) | < 1.05 for all parameters | Flag run as not converged; do not use for claims |
| Bulk ESS | > 400 per parameter | Re-run with more iterations |
| Tail ESS | > 400 per parameter | Re-run with more iterations |
| Divergences | 0 post-warmup | Increase target_accept or reparameterize |
| Max treedepth warnings | < 1% of transitions | Increase max_tree_depth |

### Recommended Diagnostics

| Diagnostic | Purpose |
|------------|---------|
| Posterior predictive checks | Visual and quantitative fit assessment |
| LOO-CV (PSIS) | Model comparison across workloads |
| Prior-posterior overlap | Identify parameters dominated by the prior |

## Claim-Classification Rules

After a diagnostically valid inference run, each claim is classified using
these categories:

| Classification | Definition |
|----------------|------------|
| **Supported** | Posterior evidence strongly favors the claim with high certainty |
| **Weakly Supported** | Posterior evidence favors the claim but with meaningful uncertainty |
| **Inconclusive** | Posterior evidence does not clearly favor or contradict the claim |
| **Weakly Contradicted** | Posterior evidence leans against the claim |
| **Contradicted** | Posterior evidence strongly contradicts the claim |

### Decision Criteria

- **Trend claim (Q1):** Supported if the posterior difference between
  adjusted and raw trajectories excludes zero in the 90% credible interval
  for a majority of units.
- **Rent-association claim (Q2):** Supported if the aggregate rent coefficient
  `phi_bar` has a 90% credible interval entirely above zero.
- **Uncertainty claim (Q3):** Supported if count-accuracy posterior intervals
  are meaningfully narrower than the prior and vary across units.
- **Latent total claim (Q4):** Supported if posterior predictive intervals
  cover observed counts at the expected rate (e.g., 90% of held-in
  observations fall within 90% intervals).
- **Forecast claim (Q5):** Supported if held-out observations fall within
  posterior predictive intervals at rates consistent with nominal coverage.

## Workload Mapping

| Workload | Primary Claims | Notes |
|----------|---------------|-------|
| A (Broad Metro) | Q1, Q2, Q3, Q4 | Primary claims-audit panel |
| B (Headline Metro) | Q1, Q2, Q4 | Narrative-specific metros |
| C (CoC Robustness) | Q1, Q2 | Robustness check at CoC level |
| D (Cross-Method Reuse) | All | Backend sensitivity comparison |

## Interpretation Limits

- The audit uses ZORI as the rent proxy, not the paper's original historical
  Zillow series. Results are not expected to reproduce the paper's exact
  numerical outputs.
- Count-accuracy priors are treated as an assumption, not an observation.
  Sensitivity to prior specification should be reported.
- Metro definitions follow the Glynn-Fox Table 1 mapping (`glynn_fox_v1`),
  which may differ from current CBSA boundaries.
- The audit is designed to evaluate whether the paper's *substantive
  conclusions* hold under modern data, not whether specific numerical values
  are reproduced.

## References

- Glynn, C. and Fox, E. B. (2019). "Dynamics of Homelessness in Urban America."
  *Annals of Applied Statistics*, 13(1), 573-605.
- Panel contract specifications: `background/audit_panel_specs.md`
- Panel implementation: `coclab/audit_panels.py`
