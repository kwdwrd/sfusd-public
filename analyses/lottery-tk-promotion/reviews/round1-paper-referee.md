# Round 1 — paper-referee (2026-10-18)

**Recommendation: Reject**, with an invitation to resubmit as a new paper once
individual-level data arrive. Major revision judged insufficient because no
revision of the current data can produce a second identifying moment.

## Summary of the referee's reading
Stripped of apparatus, the informational content is: (a) 73% of families took a
free guaranteed seat; (b) take-up is uncorrelated with every available quality
measure (rho = -0.05 with theta, 0.16/0.12 with SBAC); (c) most takers would
have ranked their own school first anyway. The headline c-hat is an algebraic
restatement of (a) plus a chosen EU: logit(0.73) + E[EU - theta_k]
= 0.99 + 1.24 = 2.24, against the reported 2.41.

Also flagged as a desk-reject risk independent of the economics: the "XX%"
placeholder (:30), two undefined cite keys, empty Literature (:42-43), the
inline note at :36, "Now that we actually plot it..." (:138), and §Other
concerns as four bullets (:334-340).

## Major concerns

### M1. The estimand is a relabeling of the take-up rate; the one validating restriction is decisively rejected.
One moment, one free parameter. c-hat = logit(r-bar) + (EU - theta-bar). Only
the cross-school slope restriction beta = 1 could make this structural, and the
paper reports beta-hat = 0.02 (s.e. 0.09), i.e. t ~ 11 against beta = 1 (:255),
with the free slope never distinguishable from zero across all seven robustness
rows (0.02, 0.04, 0.02, 0.07, 0.01, 0.02, -0.01).
`promotion_rate_vs_quality.png` is "damning on its face": model-implied curve
runs 0.27 to 0.96 across the theta range while the data are a flat cloud at 0.73.
**Core logical objection:** if beta != 1, theta_k is the wrong index; EU is built
from the same thetas so it is also the wrong index; and the *level* of c-hat
(= EU - theta-bar + const) has no claim to being "in theta units". "You cannot
reject the mapping from theta to choices and simultaneously report a number
denominated in theta." The §"A problem" retreat (:255-261) "concedes the model
while keeping the number."
Would change its mind: a genuine second moment — the 2026-27 cohort's take-up by
school; EU variation not deterministic in theta_k; or a pre-specified test on the
6th/9th-grade transition cohorts.

### M2. EU is not identified separately from theta, and the "bracket" is not a bracket.
`src/capacity.py:9-10`: P_k = min(1, capacity_k/demand_k) with demand_k the SAME
all-rank counts A_kt used to estimate theta_k. So P_k is a deterministic
decreasing function of theta-hat_k given capacity, and EU is a pure functional of
(theta-hat, capacity). No independent information enters.
1. **Correlated measurement error.** Noise in A_kt raises theta-hat_k and
   mechanically lowers P-hat_k. Neither the 0.08 s.e. (which :253 concedes
   ignores sampling error in theta and EU) nor any robustness row accounts for
   it. Demands a bootstrap over the whole chain: resample applicants/years ->
   re-estimate theta-hat -> P-hat -> EU -> c-hat. Expects the CI to be several
   times wider than +/-0.16.
2. **P_k is falsifiable TODAY and is the cheapest decisive test available.**
   A rank-5 request does not compete on equal terms with a rank-1 request under
   DA; collapsing ranks understates P for first choices. SFUSD publishes the
   share of K applicants receiving their first choice / a top-three choice. The
   model's implied first-choice assignment rate (single digits to ~17%) should be
   compared to the published figure. If the model implies ~10% and the district
   reports 55-70%, the baseline EU is simply wrong and so is c-hat = 2.41.
   Requires no new data and is pre-specifiable.
3. **EU's support contradicts the stated model.** :111 sums over i's rank list;
   `src/capacity.py:171-172` sums over all 71 schools with independent draws.
   With median P = 17% and 71 schools, Pi(1-P_j) ~ 0, so the baseline family is
   effectively guaranteed a seat near the 7th-best school — hence "only 7 of 71
   schools have theta_k above the baseline EU" (:225). A 5-rank EU is neither
   reported nor bounded, so the "bracket" claim (:231) is unsupported. The 0.81
   spread is 34% of the headline; the paper should not be able to move its
   headline by a third by choosing a nuisance assumption.

### M3. The lottery is valued at theta of the assigned school while the certain seat carries the family's own epsilon. Believed to be a technical error, not just an unshown claim.
:229 dismisses this because "the same idiosyncratic term attaches to the certain
TK seat." The referee argues this is wrong: the certain seat delivers one draw
eps_{i,k(i)}; the lottery delivers the eps of the highest-ranked school that
admits the family, where the family ordered schools by u_ik = theta_k + eps_ik —
so the assignment is **selected on epsilon**. Under unit-scale Gumbel with a
5-school list and 77% placement from list, the expected eps of the assignment
exceeds the unconditional mean by order ln(5) ~ 1.6 — the same order as the
entire headline c-hat = 2.41. Valuing the lottery at seat-weighted mean theta
throws away exactly the selection gain that makes a lottery attractive.
Fix is two lines in code the authors already have: the RSD simulation already
draws u_ik, so report E[theta_a(i) + eps_{i,a(i)}] - E[theta_k(i) + eps_{i,k(i)}]
for TK schools rather than the theta-only difference. If c-hat survives near 2,
the concern dies; if it falls toward zero, the headline is an artifact.

### M4. The event study shows no break — a wandering series passing through zero in the normalization year.
First-choice pre-period coefficients run about -0.17 (12-13), -0.34, -0.33,
-0.45, -0.37, -0.48 (17-18), -0.39, -0.25, -0.36, -0.27, -0.17, -0.13, 0 (24-25);
post is -0.33 (25-26) and -0.29 (26-27). **The post-treatment value sits inside
the pre-period range and is smaller in magnitude than six pre-period years.**
2024-25 is the series maximum and is the chosen reference year. The paper's
pre-trend story (growing TK classes, :199) cannot explain a trend beginning in
2017-18, years before the 2023-24 TK expansion. The alternative — eligible
schools are differentially declining-demand schools, which is why they got TK
programs (:49) — is a parallel-trends violation, not a nuisance.
Implied f moves 0.30 (pooled) -> 0.77 (pooled + group trend), a 2.5x range, purely
from a linear trend on four pre-periods. ":199's 'f between roughly 0.6 and 0.8'
silently discards column (2)"; the text should say why col (2) is wrong rather
than report its implied f and ignore it.
Wants: honest-DiD / Rambachan-Roth sensitivity with a breakdown value; a placebo
on the 2023-24 TK expansion cohort; the estimate with 2023-24 as reference.
Judges column (4)'s cross-school intensity design (0.63, s.e. 0.31) the most
credible in the table because it uses within-treated variation, and would build
the reduced form around it.
**Also:** `figures/first_rankings.png` is "the clearest reduced-form evidence in
the manuscript" — adding the 601 promoted families back gives 2,007 first
rankings against 1,855 in 2024-25, "fully (indeed over-) explained by f ~ 1" —
and it appears at :66-70 captioned "Mean..." and is NEVER DISCUSSED. "A
self-inflicted wound."

### Can the reduced form carry the paper alone? No.
The DiD identifies f, an accounting parameter about where requests went, not a
preference parameter about lotteries. A family with c = 0 that loves its TK
school would also rank it first. The all-ranked results are null (-0.067 s.e.
0.076; -0.060 s.e. 0.065; -0.100 s.e. 0.063). Reduced-form-only is "a
well-executed district memo, not an AEJ: Applied paper." Moving it earlier is
good exposition but "may actually make the thinness more visible."

### M5. The estimation sample is entirely families who entered the lottery a year earlier.
`src/capacity.py:58-66` shows standalone TK sites appear as rankable programs, and
:36 notes there are not enough TK seats. If TK is allocated by the same lottery,
every family in the sample revealed 12 months earlier that it would participate
in the lottery — so with heterogeneous c the sample is selected toward LOW c.
Meanwhile the paper's lead policy claim (:32, the $200M private-school
counterfactual) is about families who never enter the system — exactly the
highest-c families, who are not in the sample. Never mentioned.
**The TK allocation mechanism is never described — "the most surprising omission
in the manuscript."** If TK placement is substantially lottery-determined, that
is a gift: it partly neutralizes the match-value selection story.

### M6. "Not identifiable, and anyway it's good news" will not survive. (Referee was asked to attack or endorse; it attacks.)
1. "Not identifiable" is doing work it shouldn't. Learning, endowment effects,
   switching costs and match-value selection are **observationally equivalent**
   to the headline on a single moment, and beta-hat ~ 0 is the signature expected
   if theta_k is the wrong index for these families. With the one testable
   implication failed, "we cannot distinguish them" means the parameter is not
   identified, full stop. The paper should state c-hat is an **UPPER BOUND**:
   TK match-value selection (up), learning/endowment/switching (up),
   epsilon-selection per M3 (up), generous baseline admission model per M2 (up).
   Only families exiting SFUSD pushes down (:259), and that is boundable from
   district exit rates. "An upper-bound framing with signed biases is defensible
   and might be publishable; the current framing is not."
2. "If true, the system is working" is a non sequitur that REVERSES the policy
   conclusion: if families stay because a year taught them to like the school,
   the enrollment-raising policy is **expand TK**, not abolish the lottery — which
   collapses the intro's motivation at :32. "A referee will read this as a dodge,
   and will be right to."
3. **An available test the paper doesn't run:** if the lottery panel reports TK
   request counts by site, estimate a TK-demand quality theta^TK_k the same way
   and regress the promotion rate r_k on theta^TK_k. Under match-value selection
   r_k should relate to theta^TK_k even though it is flat in theta^K_k; under pure
   flat distaste both are flat. "Do not say 'not identifiable' until this has
   been tried."
Also :36 ("A family which remains at a 'low-quality' school **must** do so
because they dislike lottery participation") asserts exactly what §promotion-rates
admits is not identified. "Delete or qualify it; a referee will circle it and
never recover."

### M7. The abstract contradicts the paper on four counts (:26).
- "relatively MORE applicants" — the paper shows first-choice requests at eligible
  schools FELL 27% relative to others.
- "SFUSD's 2023-2024 TK promotion priority" — **misdates the experiment.** The
  promotion applied to 2024-25 TK students entering K in 2025-26; 2023-24 is the
  year TK expanded (:34).
- "best and MEDIAN schools" — headline c-hat 2.41 is Clarendon vs Bryant, #55 of
  71. Only the capacity c-hat 1.60 (Lakeshore, #32) is near the median.
- "broadly consistent with the experimental findings of risk aversion" — the
  paper's own appendix says implied CRRA is 100-125 at D = $50k, that Rabin's
  calibration argument applies (:311), and r-hat moves 5.0 -> 21 on dropping one
  outcome. "This sentence must go."

### M8. Inference.
The 0.08 s.e. treats 820 families as i.i.d. Bernoulli, but the regressor varies
at the school (40 buildings, take-up 0.41-1.00). Cluster at the building at
minimum; better, bootstrap the full chain. With one treated period, 40 treated
and 30 control clusters and a trend on four pre-periods, column (3)'s DiD s.e.
are optimistic — wild cluster bootstrap is standard and cheap.
Separately, :134's "IIA in the logit model implies that, empirically, N_t should
be effectively independent of theta_0t" — **IIA implies nothing of the sort**;
N_t is by construction the outside-option margin. And N_t as implemented
(`src/data.py:63-64`) counts lottery APPLICANTS, so in ln A_kt - ln(N_t - A_kt)
the outside option is "applied but didn't rank k", not "didn't apply to SFUSD" as
the model at :91-95 defines it. Since the unit-scale epsilon normalization is the
ONLY thing giving c-hat its units, the claim that c-hat is "measured in the same
units as delta_k" (:117) needs re-derivation.

## Minor concerns
- Unfinished text: :30 "XX%", :34 "(cite the simple descriptions paper)",
  :38 "(cite Thayer...)", :36 inline note, :138, :203, empty §Literature,
  §Other concerns as bullets (:334-340).
- Undefined cites `lagosfranciscoEffect...` and `cabalinEducacion...`; the
  bibliography header admits it was "compiled from memory" (:342). Verify the
  Harrison & Rutstrom (2008) volume (Research in Experimental Economics v12).
- **Missing standard literature:** Abdulkadiroglu, Agarwal & Pathak (2017 AER);
  Agarwal & Somaini (2018 Ecta); Fack, Grenet & He (2019 AER); Burgess et al.
  (verify); Hastings, Kane & Staiger (2009, verify); Bergman, Chan & Kapor
  (verify); **Kapor, Neilson & Zimmerman (2020 AER)** — directly relevant since
  the paper imputes P_k rather than measuring beliefs; Narayan (verify) and
  Allende/Gallego/Neilson on Chile; Samuelson & Zeckhauser (1988) and Madrian &
  Shea (2001) for status-quo/default effects, since "families default to the free
  guaranteed seat" is a live reading of 73% take-up with a zero quality gradient.
- Figure captions: :69 is literally "Mean..." for the paper's best figure.
  `application_effects.png` left panel uses the same marker glyph for
  first-choice and all-rank eligible series; solid vs dashed is hard to read.
- 5.79 requests per applicant (:169) vs "the observed 5.0" (:227). Reconcile.
- Notation drift: both delta_k/xi_t and theta_k/theta_0t (:86, :120); both r for
  promotion rate (:238) and r for CARA (:296).
- "Strategy-proof" asserted at :98 and :211 with "to a first approximation" (:34).
  If submitted lists are length-capped, DA is NOT strategy-proof and the truthful
  ranking assumption fails — the issue filed as bullet four of §Other concerns
  (:339) is a first-order problem for theta-hat: if families omit hopeless
  schools, theta-hat is compressed at the top, EU is too low, c-hat too high.
- **Negative forgone option value for 38% of accepters:** under EU^cap, 13 schools
  accounting for 227 of 601 acceptances have negative FOV — the model says
  accepting was optimal at c = 0, so these are near-uninformative about c. Report
  c-hat on the complement.
- ":207 says theta_k measures the value of APPLYING to a school, not attending
  it" — stated, then not acted on; theta_k is used as the cardinal value of
  attending throughout EU, FOV and the CARA appendix.
- Spearman stability (:281) is about RANK stability; the model needs CARDINAL
  stability of theta differences. Report correlation of levels and mean absolute
  change in theta.

## What the paper does well
- Unusually honest about its own failures: §"A problem" reports a test that kills
  the structural reading; §"Fragility in the tail" shows r-hat 5.0 -> 21 -> >100;
  the EU discussion explains why the baseline formula is wrong before using it.
  "This is better practice than most submissions and should be protected, not
  softened."
- **The mechanical model of application effects (:148-156) is "exactly right"** —
  deriving the predicted DiD from the accounting identity before estimating it,
  then using the model-predicted effect as a cross-school intensity regressor
  (column 4), is "the most credible part of the empirical work and the right way
  to use aggregate data."
- The robustness table is thorough on its dimension; the simulation-based
  validation of the risk-aversion estimators (30 checks) is "good practice rarely
  seen."
- The macro-from-CSV pipeline discipline and the candid §"Schools versus
  programs" disclosure of the DeAvila/Huerta double-counting bug are "exemplary."

## Route (b): the publishable short paper
Reframe as descriptive: "820 families were offered a free guaranteed seat; 73%
took it; take-up is unrelated to school quality by every available measure; most
takers would have chosen that school anyway." Report c-hat as a signed UPPER
BOUND with a full-chain bootstrap CI, drop the $200M claim, and let the
contribution be that a flat quality gradient is itself informative — it rules out
the models under which distaste is quality-denominated at all. "Route (b) is a
short paper in a good field journal; route (a) is the paper the authors clearly
want."

## On the individual-data section
Keep it short and bias-specific, not aspirational. Writing the full model with
"every family-level term below is empty" (:77) advertises that the paper is a
placeholder. Be concrete: z_ik identifies match value off WITHIN-school variation
in distance/attendance-area/sibling status among TK families — the only credible
way to separate "likes this school" from "dislikes the lottery"; individual P_ik
replaces the falsifiable proxy of M2; x_i gives heterogeneity and lets you check
whether the 820 are representative (M5).
**Two warnings on claims as written:** (1) :302's "families with different
priorities face lotteries with different spreads" — priority is a function of
z_ik, which shifts u_ik directly, so the spread variation is NOT exogenous to
preferences; risk aversion and distaste are still separated only under an
exclusion restriction the paper has not stated. (2) The distance-based dollar
conversion (:88, :311) gives $/utile only with an independent $/mile valuation
imported from outside, and the distance coefficient in a rank-list model is
notoriously contaminated by attendance-area priority and neighborhood
unobservables. "Promising 'a dollar conversion via distance' as if it were
mechanical will draw fire."

## Concerns NOT in CLAUDE.md's known-weaknesses list
1. epsilon-selection in the lottery assignment (M3) — sign-known upward bias on
   c-hat of plausibly the same order as c-hat; :229 affirmatively dismisses it.
2. P_k is a deterministic function of theta-hat (M2) — circularity, correlated
   measurement error, no s.e. reflects it. CLAUDE.md flags only the capacity LEVEL.
3. The baseline EU is externally falsifiable against SFUSD's published
   first-choice assignment rates, and the check has not been run.
4. EU's support contradicts the stated model; a 5-rank EU is neither computed nor
   bounded, so "bracket" is unsupported.
5. The event study does not support a break (M4), yet CLAUDE.md treats the
   reduced form as settled evidence to summarize early.
6. The sample is 100% prior lottery participants (M5); TK allocation never
   described; breaks the link to the $200M counterfactual.
7. The abstract's sign, date and magnitude errors (M7).
8. No building-level clustering, no full-chain bootstrap, no wild bootstrap (M8).
9. The estimating equation's outside option differs from the model's.
10. An available aggregate test of the selection story: r_k on theta^TK_k (M6.3).

## CLAUDE.md items the text UNDERSTATES
- **"The slope test fails"** is presented at :255-261 as one of three readings,
  with the abstract and intro unchanged. beta-hat = 0.02 (s.e. 0.09) against
  beta = 1 is a rejection of the mapping that gives c-hat its meaning. The text
  never states the t-statistic or draws the consequence for the LEVEL of c-hat.
- **"Non-accepters left SFUSD"** is one bullet (:259) with no magnitude, though
  SFUSD's October enrollment file would show how many of the 219 non-accepters
  enrolled in SFUSD K. Report the bound. It is also the ONLY channel pushing
  c-hat down, against four or five pushing up — the text should say so.
- **"Capacity uses CDE occupied seats"** is noted but never bounded. The 9% of
  simulated families who find no seat is a direct symptom, and EU^cap excludes
  them, which :294 concedes "understates the lottery's downside". A sensitivity
  scaling capacities by 1.05 and 1.10 would settle it.
- **"c not separately identified from risk aversion"** — the proposed fix is not
  a valid exclusion restriction as written.
- **"Aggregate data / selection into TK"** is handled best in §promotion-rates,
  but :36 still asserts the opposite tautology.
