# Rent-decline asymmetry in the top-150 MSA panel

Run the tracked analysis with:

```bash
uv run hhplab build result core-rent-shock-state-year-fe --json
```

The headline annual-change model uses one linear rent coefficient, so it
implicitly assumes that rent increases and decreases have equal-and-opposite
effects. This extension replaces that coefficient with two hinges: positive
rent changes and negative rent changes. It retains population change, year or
state-by-year fixed effects, and MSA-clustered standard errors.

## How much decline evidence exists?

The rank-51--150 cohort adds 17 annual rent declines to the top 50's 10. The
pooled complete-case sample therefore contains 27 declines across 20 MSAs,
versus 1,063 flat-or-rising observations. Declines remain only 2.5% of the
1,090-row sample. Requiring Zillow coverage of at least 80% leaves 18 decline
observations.

## Results

| Cohort | Fixed effects | Rise slope (p) | Fall slope (p) | Equal-slope p |
| --- | --- | ---: | ---: | ---: |
| Top 50 | Year | +1.91 (0.002) | -3.85 (0.022) | 0.004 |
| Ranks 51--150 | Year | +1.77 (0.002) | +6.88 (0.089) | 0.214 |
| Pooled top 150 | Year | +1.89 (<0.001) | +2.67 (0.426) | 0.822 |
| Pooled top 150 | State x year | +1.95 (0.008) | -1.30 (0.659) | 0.308 |
| Pooled, Zillow coverage >=80% | Year | +2.23 (<0.001) | -0.04 (0.989) | 0.471 |

A positive fall slope means that a negative rent change is associated with a
negative homelessness change. The extra cohort therefore rejects the striking
top-50-only suggestion that homelessness rises when rents fall: that sign does
not replicate. In the pooled year-FE model, the fall slope points toward the
same reversible response as the rise slope, but its confidence interval is far
too wide to distinguish it from zero. It changes sign under state-by-year fixed
effects and under the higher-coverage restriction.

## Conclusion

The larger sample gives a firmer **null** result, not firm evidence of reversal:
rent increases robustly predict greater unsheltered homelessness, while the
data do not establish what happens after rents fall. Nor do the pooled data
show statistically detectable asymmetry; the equality test is p=0.82 with year
fixed effects. Both statements can be true because only 27 rent declines exist,
most are about 1% rather than large housing-market corrections, and they cluster
in a few years and MSAs. A longer panel containing a broad rent downturn is
needed to distinguish rapid reversal, delayed/sticky reversal, and no reversal.
