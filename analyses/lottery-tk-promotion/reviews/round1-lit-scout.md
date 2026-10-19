# Round 1 citation audit and literature plan: "How Much Do Families Dislike School Lotteries?"

I did not edit `main.tex`. There is no `.bib` file in the paper directory (Glob found none); the bibliography is the inline `thebibliography` at main.tex:343-352.

**How to read the verification labels.**
- **[F]** means I fetched and read the page myself.
- **[S]** means a publisher, IDEAS/RePEc, NBER or similar URL came back in search results with matching authors, title, volume and pages, but I did not open it.
- Neither label means I read the paper's full text. Several publisher pages returned 403 and PDFs came back as unreadable binary, because I have no PDF text extractor in this session.
- Anything marked UNVERIFIED is not confirmed. Details I recalled from memory and could not find are marked as such, never filled in.

---

## JOB 1: Verify the citations

### 1A. Bibliography entries (main.tex:344-351)

| Key | Corrected reference | Confirming URL | Does it support the claim in main.tex? |
|---|---|---|---|
| apesteguia2018 | **Title is wrong in the draft.** Correct: Apesteguia, J. and Ballester, M. Á. (2018), "Monotone Stochastic Choice Models: The Case of Risk and Time Preferences," *JPE* 126(1), 74-106. DOI 10.1086/695504. [F] | https://bse.eu/research/publications/monotone-stochastic-choice-models-case-risk-and-time-preferences | **Partly.** Sentence (main.tex:298): "noise-in-certainty-equivalent specification (which, unlike utility-space noise, keeps choice probabilities monotone in r; Apesteguia and Ballester 2018)". The paper shows that standard random-utility (Fechner-style) models can violate monotonicity and that random-parameter models are always monotone. That supports the "utility-space noise is bad" half. I could not confirm that it endorses noise on the certainty equivalent specifically. Their positive recommendation is the random-parameter model, which is the paper's own heterogeneous-agent probit. Check the full text before keeping the parenthetical as worded. |
| barseghyan2013 | Barseghyan, L., Molinari, F., O'Donoghue, T. and Teitelbaum, J. C. (2013), "The Nature of Risk Preferences: Evidence from Insurance Choices," *AER* 103(6), 2499-2529. DOI 10.1257/aer.103.6.2499. [S] | search-returned: https://www.openicpsr.org/openicpsr/project/116116/version/V1/view and https://cae.economics.cornell.edu/11.03.pdf | **Yes.** Sentence (main.tex:306): "consistent with the probability distortions found in field insurance choices (Barseghyan et al. 2013)". The abstract summaries say substantial overweighting of small probabilities, with standard risk aversion small. |
| chetty2006 | Chetty, R. (2006), "A New Method of Estimating Risk Aversion," *AER* 96(5), 1821-1834. [S] | https://ideas.repec.org/p/nbr/nberwo/9988.html (NBER version) | **Partly.** Sentence (main.tex:311): "ρ = 2, the upper end of the macro and labor-supply range (Chetty 2006)". Chetty bounds relative risk aversion using labor-supply elasticities and reports a central estimate near 1. I did not confirm that 2 is the upper end of his range. Soften to "central estimates near 1" or check the paper. DOI not retrieved. |
| harrison2008 | Harrison, G. W. and Rutström, E. E. (2008), "Risk Aversion in the Laboratory," in Cox, J. C. and Harrison, G. W. (eds.), *Risk Aversion in Experiments*, *Research in Experimental Economics* vol. 12, Emerald, pp. 41-196. DOI 10.1016/S0193-2306(08)00003-3. [S] | https://www.doi.org/10.1016/S0193-2306(08)00003-3 and https://www.emerald.com/books/edited-volume/15811/chapter/87454628/Risk-Aversion-in-the-Laboratory | **Citation yes; claim unverified.** Sentence (main.tex:310): "close to the 0.3-0.7 typical of small-stakes laboratory choices (Holt and Laury 2002; Harrison and Rutström 2008)". I confirmed the chapter exists. I did not confirm the 0.3-0.7 range from either source. |
| holt2002 | Holt, C. A. and Laury, S. K. (2002), "Risk Aversion and Incentive Effects," *AER* 92(5), 1644-1655. [S] | search-returned: JSTOR stable 3083270 and a PDF copy at www2.um.edu.uy | **Partly.** The paper exists and finds risk aversion even at small stakes. I did not verify the "$3.85 or $0.10" row or the 0.3-0.7 range. Note that the Holt-Laury design has a known order-effect critique (Harrison, Johnson, McInnes and Rutström 2005; Holt and Laury 2005, *AER* 95(3), 902-912), which a referee may raise. |
| rabin2000 | Rabin, M. (2000), "Risk Aversion and Expected-Utility Theory: A Calibration Theorem," *Econometrica* 68(5), 1281-1292. [S] | https://ideas.repec.org/a/ecm/emetrp/v68y2000i5p1281-1292.html | **Yes, with a caveat.** Sentence (main.tex:311): "Rabin's (2000) calibration argument applies: no reasonable curvature over lifetime wealth produces premia this large on stakes this size." The theorem is about modest-stakes money gambles. Applying it needs the θ-to-dollar conversion the authors say they lack, so the argument is by analogy for now. The Cox-Sadiraj critique (income vs terminal wealth) is the obvious referee rejoinder. |
| sydnor2010 | Sydnor, J. (2010), "(Over)insuring Modest Risks," *AEJ: Applied Economics* 2(4), 177-199. DOI 10.1257/app.2.4.177. [S] | https://ideas.repec.org/a/aea/aejapp/v2y2010i4p177-99.html | **Yes.** Sentence (main.tex:311): "implausibly high coefficients implied by insurance deductible choices (Sydnor 2010)". Home-insurance deductible choices imply implausibly large risk-aversion parameters. |
| wilcox2011 | Wilcox, N. T. (2011), "'Stochastically More Risk Averse:' A Contextual Theory of Stochastic Discrete Choice Under Risk," *Journal of Econometrics* 162(1), 89-104. DOI 10.1016/j.jeconom.2009.10.012. [S] | https://ideas.repec.org/a/eee/econom/v162y2011i1p89-104.html | **Yes.** Sentence (main.tex:304): "range-normalized (contextual; Wilcox 2011) noise specifications". The paper proposes the heteroscedastic "contextual utility" model. |

**In-text mentions with no bib key.** Every in-text author-year mention maps to a bibitem: Apesteguia and Ballester, Wilcox, Barseghyan, Holt and Laury, Harrison and Rutström, Chetty, Sydnor, Rabin. The `\cite` commands themselves are used only for the two undefined keys below. The eight bibitems are never `\cite`d, only mentioned by name, so they would not render as numbered cites. That is a style issue, not an error.

Other items in the draft:
- The abstract says "the 2023--2024 TK promotion priority", but the body and CLAUDE.md describe the 2024-25 TK class entering K in 2025-26. Reconcile.
- The abstract's "broadly consistent with the experimental findings of risk aversion" is unsupported. The body says the CRRA implications are implausibly large (ρ ≈ 100-125 at $50k per θ unit).

### 1B. The two undefined cite keys

**`lagosfranciscoEffectCentralizedAdmissionSchool`.** Real, correct reference:

> Lagos, F., Saltmarsh, J. and Liu, J. (2025), "The Effect of Centralized-Admission School Lotteries on Between-School Segregation: Evidence from 300 Largest School Districts in the United States," IZA Discussion Paper No. 18306 (Dec. 2025); also EdWorkingPaper 25-1351, DOI 10.26300/1mfx-cc03. [F]

- IZA: https://www.iza.org/publications/dp18306 (BibTeX fetched from https://www.iza.org/publications/dp18306/1/citations/bibtex)
- RePEc: https://ideas.repec.org/p/iza/izadps/dp18306.html [F]
- EdWorkingPapers: https://edworkingpapers.com/ai25-1351 [F]
- Later SSRN version, retitled "The Effect of Centralized School Assignment Systems on Between-School Segregation..." (and a "JUE Insight" version): https://doi.org/10.2139/ssrn.7396035. This is the version to cite if it has been published.
- The paper is a difference-in-differences study with staggered adoption. It finds lotteries raise White enrollment in district schools and modestly raise intergroup exposure, but do not significantly reduce overall segregation.
- Lagos is at Maryland. The "Chile" and "roulette" links in the draft's sentence do not come from this paper as far as I can tell.
- The citation has nothing on "families dislike lotteries." It is a segregation paper, so it supports only the count of districts using lotteries.

**The "71 of the 300 largest districts, as of 2022" claim (main.tex:30): UNVERIFIED.**
- The text of the Lagos et al. PDFs (IZA and EdWorkingPapers) came back as unreadable compressed binary. I could not read the sentence or its reference list.
- The abstract pages (IZA/RePEc, EdWorkingPapers, EDSI Maryland) do not contain the 71 figure.
- One search-tool summary of the IZA PDF said "out of the 300 largest U.S. school districts in the sample, 71 had adopted the approach." That is an unconfirmed, machine-generated paraphrase, not a quote. It says nothing about "as of 2022."
- The paper counts adopters of "centralized-admission lotteries" over a panel, and the retitled version says "centralized school assignment systems." Those may not be the same set. The draft's phrase "some form of lottery system" may therefore overstate or mis-scope the number.
- To close this, someone should run `pdftotext` on the IZA PDF and grep for "71" and "2022". The saved copy is at `/Users/kylew/.claude/projects/-Users-kylew-git-sfusd-public/61b16a11-dcf1-4d5d-b8e0-f31011d570c0/tool-results/webfetch-1792392131282-1wd5ex.pdf` (the EdWorkingPapers PDF). If the number is not in the paper's text, remove "and references therein".

**`cabalinEducacionPortadaVisualizacion2016`.** The key matches this real article:

> Cabalin, C. and Antezana, L. (2016), "Education cover page: the visualization of educational policy in the press" (Spanish title begins "La educación en portada…"), *Cuadernos.info* no. 39, pp. 195-207. DOI 10.7764/cdi.39.914. [S]

- URLs: https://redae.uc.cl/index.php/cdi/article/view/24373 (403 when fetched; the abstract came through search) and https://www.redalyc.org/journal/270/27062001007/27062001007.pdf
- It is a visual-framing analysis of *Las Últimas Noticias* front pages during the 2014-2015 education-reform debate. The paper finds the paper framed it through a human-capital lens and criticized the Bachelet government's initiatives.
- **Problem for the draft's claim.** The draft says "changes to Chile's Sistema de Admisión Escolar were covered with a newspaper illustration of a child strapped to a roulette wheel." I could not read the article body, so I cannot confirm that any cover shows a child on a roulette wheel, or that it concerns admissions. The 2014-15 reform debate was about the Ley de Inclusión. The SAE, the "tómbola", rolled out in 2016. So the claim is **UNVERIFIED**. Also, a single front page from a tabloid is thin evidence that "the popular conception is that families dislike them."
- Cheap fix: open the Redalyc PDF and check for the image. If it is not there, cite a direct source on the "tómbola" controversy instead. Cristián Cabalin is the likely academic voice on it: https://radio.uchile.cl/2026/07/26/cristian-cabalin-por-reforma-al-sae-la-eleccion-mutua-arriesga-desmontar-el-sistema-y-reinstalar-discriminaciones-arbitrarias/ (2026 interview) and the BioBio column https://www.biobiochile.cl/noticias/opinion/columnas-bbcl/2026/06/25/chao-a-la-tombola-en-el-sistema-educacional.shtml [S]. I did not read the interview text.

### 1C. "Thayer": UNRESOLVED

I could not identify a "Thayer" who wrote about revealed-preference school quality in SFUSD or anywhere else. About six searches across rank-list, SFUSD and school-choice phrasing found nothing. Do not guess a citation; ask the authors for Thayer's first name or a link. It may be a Substack or blog author or a thesis, which would be cited differently.

Meanwhile, these are the real works the footnote likely wants, by role:
- **Aggregate or rank-list school-demand models using SFUSD data:** Awadelkarim, Seshadri, Ashlagi, Lo and Ugander (2023). They use SFUSD household rank data, mostly multinomial-logit extensions. It is the closest academic neighbor.
- **Quality as a school fixed effect from rank lists:** Abdulkadiroğlu, Pathak, Schellenberg and Walters (AER 2020), and Agarwal and Somaini (Econometrica 2018).
- **Descriptive SFUSD demand analysis, not a formal model:** the SFEDup Substack by Paul Gardiner (https://sfeducation.substack.com/p/sfusd-enrollment-demand [F]). It uses first-choice counts and yield with no formal estimation.

### 1D. "(cite the simple descriptions paper)" (main.tex:34)

The sentence is: the district computes a market priority "such that students above this priority are admitted." That is the cutoff / supply-and-demand description of deferred acceptance. Candidates, best first:
1. **Azevedo and Leshno (2016)**, "A Supply and Demand Framework for Two-Sided Matching Markets," *JPE*. Stable matchings are the solution to supply and demand equations with school cutoffs as prices. This is the most likely target. [S]
2. **Ashlagi and Shi (2014)**, *Operations Research* 62(6), 1247-1264. They show that in large markets a broad class of mechanisms reduces to "lottery plus cutoff." It was done in the context of Boston school choice data. [S]
3. Pathak and Sethuraman (2011), "Lotteries in Student Assignment: An Equivalence Result," *Theoretical Economics* 6(1), 1-17. This is a candidate if the authors mean the lottery tie-break rather than the cutoff. [S]
4. Abdulkadiroğlu and Sönmez (2003). This is the original mechanism-design framing, not a "simple description." [S]

I cannot tell which one the authors meant. Please ask them.

### 1E. "Own calculations" footnote (main.tex:32)

Claim: the private rate falls from 30% to the national 15%, adding about $200M per year. No code or output backs it. External sources needed:
- **National private share.** NCES puts private enrollment at about **9%** of combined public and private K-12 enrollment in fall 2021 (and 9% in 2019), roughly 4.7M students. [S] Pointer: https://www.cato.org/blog/federal-data-support-our-findings-k-12-private-school-enrollment-rose-during-pandemic-schools and the NCES Condition of Education (the search returned a UCSB mirror of NCES 2024-144). **The draft's "national average of 15%" does not match NCES's 9%.** Check what the authors meant, or fix it.
- **San Francisco private share.** The best local estimate I found is ~31% (23,964 private vs 52,715 public students in 2023-24), from SFEDup: https://sfeducation.substack.com/p/what-percentage-of-kids-attend-private [S]. It notes that some private students live outside SF, so the resident share is lower, and ACS-based estimates are unreliable. A citable source would be the CDE private school affidavit data plus the NCES Private School Universe Survey, or ACS for San Francisco County.
- **Per-pupil revenue** from CDE LCFF apportionment and SFUSD adopted budget. The 30%→15% shift also needs a student count. Whichever number is right, the footnote needs a script or a source, not "own calculations." CLAUDE.md's exempt-numbers rule requires a citation or a footnote saying where it comes from.

### 1F. Parent-complaint survey (the one gap you flagged): primary SFUSD source found

SFUSD's own report is posted: **"Fall 2026 Family Survey and Focus Groups on SFUSD's Student Assignment Policy"**, https://www.sfusd.edu/school-reorganization/fall-2026-family-survey-and-focus-groups-sfusds-student-assignment-policy [F], last updated Oct 9, 2026. Survey open Sept 11 to Oct 9; focus groups Sept 25 to Oct 9.

What it says, which differs from your scratchpad notes:
- **n = 2,978** responses: 1,871 SFUSD families and 1,107 not in SFUSD. Your notes had 2,725 from the press. The press figure is wrong or from an earlier cut.
- **"Meet needs":** Options A and B are **72% each** (your notes had 73/71-72). Option C is **48%**.
- **82% picked a guaranteed spot near home** when asked to choose. This is the most direct "predictability" number, and a better headline for the "XX%" placeholder than the "meet needs" figures, which are not about predictability.
- 96% agreed it is important to pick the school that best fits their child.
- Respondents skew White (26% vs 15% of SFUSD kindergartners) and higher income (25% on FRPM/CalFresh/Medi-Cal vs 52% of SFUSD elementary students). Reweighting to neighborhood and income moved the option ratings by 2 points or less and the guaranteed-spot preference by 3 points.
- Not a random sample. SFUSD says so explicitly and makes no policy recommendation.
- Detailed responses are in a Google Drive file: https://drive.google.com/file/d/1fQfpEtkRRV_lROTN2Tt0nZkFo4Kh11GH/view?usp=sharing (I did not open it).
- The Oct 6 study-session deck is linked from https://www.sfusd.edu/school-reorganization but returned 403, so I did not check it. Link: https://go.boarddocs.com/ca/sfusd/Board.nsf/pfiles/DYH299010E2A/$file/School%20Reorg%20Study%20Session%20%233%20(Final%2010.2.2026).pdf
- March 2026 context from the same page: 96% of assigned kindergartners got one of their choices and 71% got their first choice.
- **Discrepancy to resolve.** One news summary said the board chose "Option C," described as guaranteeing a neighborhood seat with the option to apply citywide. Your notes call Option C "choice-preserving" at 48%. The survey page does not describe the options. Confirm what C is before writing the text. If it is the policy the board adopted, the survey's 48% and the board's choice point in opposite directions, which matters for how the draft presents "revealed-preference corroboration."

---

## JOB 2: What the Literature section should engage with

Each entry gives a one-line finding, how this paper relates, and a verification label. The papers a referee would most expect are marked **[TOP]**.

### Headline question: has anyone estimated distaste for lotteries or a certainty premium in school assignment?

**I found no paper that does.** Searches for willingness to pay for certainty in school assignment, aversion to lotteries in school choice, and structural risk-aversion estimates from rank lists returned nothing direct. Closest neighbors:
- **School-choice papers where admission risk shapes behavior.** Calsamiglia-Fu-Güell (Barcelona), Kapor-Neilson-Zimmerman, Arteaga et al., Ajayi-Sidibé. They treat beliefs about admission risk and its effect on rank lists, not a utility cost of participating.
- **Behavioral work on randomization.** Agranov-Ortoleva, Dwenger-Kübler-Weizsäcker, the "uncertainty effect," Meisner-von Wangenheim, Dreyfuss-Heffetz-Rabin.
- **Stated-preference survey.** SFUSD's fall 2026 survey (above), plus the SF Parents Coalition survey in your notes.

I cannot rule out something unpublished or in a field I did not search. The claim to make in the paper is "to our knowledge, no prior estimate of this object." A referee will probe it, so it should be stated carefully.

**Distinction the paper should make explicit.** What is estimated here is a certainty premium that works as a reduced-form wedge in a binary choice. The literature on school choice and risk (Calsamiglia et al.; Kapor et al.; Agarwal-Somaini) treats admission uncertainty as something families manage by choosing portfolios of schools, which a utility-maximizing, risk-neutral family does. Separating "dislike of participating" from "risk aversion" is the identification point the paper already acknowledges (c vs CARA r are one moment).

### Strand 1: School choice mechanism design and reforms

1. **[TOP]** Abdulkadiroğlu, A. and Sönmez, T. (2003), "School Choice: A Mechanism Design Approach," *AER* 93(3), 729-747. DOI 10.1257/000282803322157061. Frames school assignment as mechanism design and proposes deferred acceptance and TTC as fixes for Boston-style mechanisms. Relation: the paper's DA/strategy-proofness assumption rests on this. [S] https://ideas.repec.org/a/aea/aecrev/v93y2003i3p729-747.html
2. **[TOP]** Azevedo, E. M. and Leshno, J. D. (2016), "A Supply and Demand Framework for Two-Sided Matching Markets," *JPE* 124(5), 1235-1268 (volume and issue not independently shown in my results; pages and DOI 10.1086/687476 were). The cutoff characterization of DA. Relation: the draft's "market priority" sentence and the P_k = min(1, capacity/demand) proxy are this logic. [S] https://ideas.repec.org/a/ucp/jpolec/doi10.1086-687476.html
3. Abdulkadiroğlu, A., Pathak, P. A. and Roth, A. E. (2009), "Strategy-Proofness versus Efficiency in Matching with Indifferences: Redesigning the NYC High School Match," *AER* 99(5), 1954-1978. Single tie-breaking is preferred. Relation: ties the lottery's mechanics (RSD in your simulation) to the real-world design. [S] https://dukespace.lib.duke.edu/items/102c418a-8972-4bc2-a222-c004ce17aa86/full
4. Pathak, P. A. and Sönmez, T. (2008), "Leveling the Playing Field: Sincere and Sophisticated Players in the Boston Mechanism," *AER* 98(4), 1636-1652. DOI 10.1257/aer.98.4.1636. Families differ in sophistication. Relation: relevant to the "rank list = true preference" assumption. [S] https://ideas.repec.org/a/aea/aecrev/v98y2008i4p1636-52.html
5. Pathak, P. A. and Sethuraman, J. (2011), "Lotteries in Student Assignment: An Equivalence Result," *Theoretical Economics* 6(1), 1-17. Single and multiple lotteries are equivalent. Relation: justifies modeling SFUSD's lottery as one lottery number. [S] https://www.econometricsociety.org/publications/theoretical-economics/2011/01/23/Lotteries-in-student-assignment-An-equivalence-result
6. Ashlagi, I. and Shi, P. (2014), "Improving Community Cohesion in School Choice via Correlated-Lottery Implementation," *Operations Research* 62(6), 1247-1264. DOI 10.1287/opre.2014.1319. Lottery-plus-cutoff form in large markets. Relation: a "simple description" of the lottery. [S] https://pubsonline.informs.org/doi/10.1287/opre.2014.1319
7. Abdulkadiroğlu, A., Agarwal, N. and Pathak, P. A. (2017), "The Welfare Effects of Coordinated Assignment: Evidence from the New York City High School Match," *AER* 107(12), 3635-3689. DOI 10.1257/aer.20151425. Empirical evaluation of an assignment reform, with preferences estimated from rank lists. Relation: the template for evaluating a mechanism change. Natural comparison for the Chile SAE and SF neighborhood-assignment reforms. [S] https://scholars.duke.edu/publication/1294997
8. Lagos, Saltmarsh and Liu (2025) (above). Relation: the only cite for the policy-environment claim; the paper reports what lotteries do (small redistribution, no significant effect on dissimilarity), which is relevant to the paper's "ambiguous ability of lotteries to achieve their aims."

### Strand 2: Estimating preferences from rank-ordered lists; aggregate vs individual data

9. **[TOP]** Agarwal, N. and Somaini, P. (2018), "Demand Analysis Using Strategic Reports: An Application to a School Choice Mechanism," *Econometrica* 86(2), 391-444. DOI 10.3982/ECTA13615. Preferences are identified under strategic reporting; Cambridge MA. Relation: the framework the paper's "model written for individual data" should cite. [S] https://econometricsociety.org/publications/econometrica/2018/03/01/demand-analysis-using-strategic-reports-application-school
10. Agarwal, N. and Somaini, P. (2020), "Revealed Preference Analysis of School Choice Models," *Annual Review of Economics* 12, 471-501. DOI 10.1146/annurev-economics-082019-112339. The survey. Relation: the one cite that covers what is and is not identified from rank lists; the aggregate-vs-individual discussion belongs here. [S] https://nber.org/papers/w26568
11. **[TOP]** Fack, G., Grenet, J. and He, Y. (2019), "Beyond Truth-Telling: Preference Estimation with Centralized School Choice and College Admissions," *AER* 109(4), 1486-1529. Stability can be a weaker, more plausible assumption than truth-telling. Relation: the strongest challenge to the paper's "submitted list = true preference" assumption. [S] https://par.nsf.gov/servlets/purl/10200905
12. **[TOP]** Abdulkadiroğlu, A., Pathak, P. A., Schellenberg, J. and Walters, C. R. (2020), "Do Parents Value School Effectiveness?" *AER* 110(5), 1502-1539. DOI 10.1257/aer.20172040. Rank lists are used to estimate demand; parents' preferences track peer quality rather than causal effectiveness. Relation: directly relevant to reading θ_k as "quality". θ_k here is demand, not value-added. [S] https://www.nber.org/papers/23912
13. Calsamiglia, C., Fu, C. and Güell, M. (2020), "Structural Estimation of a Model of School Choices: The Boston Mechanism versus Its Alternatives," *JPE* 128(2), 642-680. Structural model of a household's choice under admission risk, Barcelona. Relation: the closest structural treatment of uncertainty in school choice. [S] https://bse.eu/research/publications/structural-estimation-model-school-choices-boston-mechanism-vs-its
14. Awadelkarim, A., Seshadri, A., Ashlagi, I., Lo, I. and Ugander, J. (2023), "Rank-heterogeneous Preference Models for School Choice," *Proceedings of KDD '23*. DOI 10.1145/3580305.3599484. Uses SFUSD household rank data; models rank-dependent choice. Relation: **only academic estimation I found on SFUSD**. [S/F] https://arxiv.org/abs/2306.01801 (arXiv page fetched [F]; DOI and venue from par.nsf.gov search result)
15. Burgess, S., Greaves, E., Vignoles, A. and Wilson, D. (2015), "What Parents Want: School Preferences and School Choice," *Economic Journal* 125(587), 1262-1289. DOI 10.1111/ecoj.12153. Conditional logit on rank lists; strong preference for academic performance, with distance and composition. Relation: the closest match for a simple conditional logit on aggregated or accessible-school data. [S] https://researchportal.bath.ac.uk/en/publications/what-parents-want-school-preferences-and-school-choice/
16. Hastings, J. S., Kane, T. J. and Staiger, D. O. (2006), "Preferences and Heterogeneous Treatment Effects in a Public School Choice Lottery," NBER Working Paper 12145 (the title I found in search results; a published version was not confirmed). Charlotte-Mecklenburg. Relation: logit demand plus lottery. [S] https://nber.org/papers/w12145

### Strand 3: Risk preferences from the field; probability weighting; the small-stakes move

For the Sydnor-style reading (ρ ≈ 100-125 is implausible so something other than curvature is going on), these are the right cites. Several are already in the bibliography.

17. **[TOP]** Barseghyan, Molinari, O'Donoghue and Teitelbaum (2013), AER (above): probability distortion can explain deductible choices. The draft already cites it.
18. **[TOP]** Barseghyan, L., Molinari, F., O'Donoghue, T. and Teitelbaum, J. C. (2018), "Estimating Risk Preferences in the Field," *Journal of Economic Literature* 56(2), 501-564. DOI 10.1257/jel.20161148. Survey of field estimation, with identification issues. Relation: the right overview cite for the paper's move from CARA to "probability weighting or ambiguity". [S] https://ideas.repec.org/a/aea/jeclit/v56y2018i2p501-64.html
19. Cohen, A. and Einav, L. (2007), "Estimating Risk Preferences from Deductible Choice," *AER* 97(3), 745-788. Most people are near risk-neutral over $100 stakes but a significant fraction are risk averse even over small bets. Relation: heterogeneity argument and the Israeli auto-insurance benchmark. [S] https://www.nber.org/papers/w11461
20. Kahneman, D. and Tversky, A. (1979), "Prospect Theory: An Analysis of Decision under Risk," *Econometrica* 47(2), 263-291 (the Econometric Society page gives 263-292 and Princeton gives 263-291; check). Certainty effect, probability weighting. Relation: the original "certainty effect." [S] https://www.econometricsociety.org/publications/econometrica/1979/03/01/prospect-theory-analysis-decision-under-risk
21. Kőszegi, B. and Rabin, M. (2006), "A Model of Reference-Dependent Preferences," *QJE* 121(4), 1133-1165. Reference points set by expectations. Relation: ties to the status-quo and TK-seat-as-reference-point story. [S] https://rabin.scholars.harvard.edu/publications/model-reference-dependent-preferences
22. Dimmock, S. G., Kouwenberg, R. and Wakker, P. P. (2016), "Ambiguity Attitudes in a Large Representative Sample: Measurement and Implications," *Management Science* 62(5), 1363-1380. DOI 10.1287/mnsc.2015.2198. Field evidence on ambiguity. Relation: cite for the "ambiguity aversion" reading the draft invokes without a citation (main.tex:306). [S] https://repub.eur.nl/pub/88479
23. Gneezy, U., List, J. A. and Wu, G. (2006), "The Uncertainty Effect: When a Risky Prospect Is Valued Less than Its Worst Possible Outcome," *QJE* 121(4), 1283-1309. People price a lottery below its worst outcome. **This is highly relevant to the paper's "fragility in the tail" finding**, where the lottery's certainty equivalent is below nearly all of its mass. The effect is contested (Ortmann et al. CERGE-EI WP 334; Ahler and Sood 2023 replication). [S] https://rady.ucsd.edu/_files/faculty-research/uri-gneezy/uncertainty-effect.pdf

### Strand 4: Distaste for randomization / preference for certainty

As noted, nothing directly on point in school choice. Related:
24. Agranov, M. and Ortoleva, P. (2017), "Stochastic Choice and Preferences for Randomization," *JPE* 125(1), 40-68. DOI 10.1086/689774. Many subjects deliberately randomize. **Opposite direction** to this paper (they prefer randomization), which makes it a useful contrast. [S] https://ideas.repec.org/a/ucp/jpolec/doi10.1086-689774.html
25. Dwenger, N., Kübler, D. and Weizsäcker, G. (2018), "Flipping a Coin: Evidence from University Applications," *Journal of Public Economics* 167, 240-250. DOI 10.1016/j.jpubeco.2018.09.014. Applicants in the German clearinghouse appear to delegate choice to a coin flip. Relation: field evidence from an assignment system *in favor of* randomization. A direct contrast to "lottery distaste." [S, via secondary pages; the publisher page was not opened]
26. Meisner, V. and von Wangenheim, J. (2023), "Loss aversion in strategy-proof school-choice mechanisms," *Journal of Economic Theory* 207, 105588. DOI 10.1016/j.jet.2022.105588. Expectation-based loss aversion generates non-truthful lists under DA. Relation: a competing explanation for why risk matters to families beyond curvature. [F, arXiv: https://arxiv.org/abs/2207.14666; journal details S]
27. Dreyfuss, B., Heffetz, O. and Rabin, M. (2022), "Expectations-Based Loss Aversion May Help Explain Seemingly Dominated Choices in Strategy-Proof Mechanisms," *AEJ: Microeconomics* 14(4), 515-555. DOI 10.1257/mic.20200259. Same idea, with experimental data on random serial dictatorship. [S] https://www.aeaweb.org/doi/10.1257/mic.20200259
28. Experimental evidence on willingness to pay to avoid a lottery procedure: "Rituals of reason: Experimental evidence on the social acceptability of lotteries in allocation problems," *Games and Economic Behavior*, 2025. UNVERIFIED beyond the title and journal; the ScienceDirect page returned 403, so I have no authors, volume or pages. Found via https://www.sciencedirect.com/science/article/pii/S0899825625000478. Check before citing.

### Strand 5: Status quo bias, endowment, switching costs, "learning to love your TK school"

29. Samuelson, W. and Zeckhauser, R. (1988), "Status Quo Bias in Decision Making," *Journal of Risk and Uncertainty* 1(1), 7-59. Experiments and field data (health plans) show a disproportionate tendency to keep the status quo. [S] https://ideas.repec.org/a/kap/jrisku/v1y1988i1p7-59.html
30. Kahneman, D., Knetsch, J. L. and Thaler, R. H. (1990), "Experimental Tests of the Endowment Effect and the Coase Theorem," *JPE* 98(6), 1325-1348. DOI 10.1086/261737. [S] https://ideas.repec.org/a/ucp/jpolec/v98y1990i6p1325-48.html
31. **[TOP]** Handel, B. R. (2013), "Adverse Selection and Inertia in Health Insurance Markets: When Nudging Hurts," *AER* 103(7), 2643-2682. DOI 10.1257/aer.103.7.2643. Estimates switching costs or inertia and risk preferences jointly from field data, separating them from preference for the status-quo plan. Relation: the best model for separating "inertia / learning" from "distaste for choosing" with individual panel data. [S] https://ideas.repec.org/a/aea/aecrev/v103y2013i7p2643-82.html
32. Hastings, J. S. and Weinstein, J. M. (2008), "Information, School Choice, and Academic Achievement: Evidence from Two Experiments," *QJE* 123(4), 1373-1414. DOI 10.1162/qjec.2008.123.4.1373. Parents respond to information; relevant to "learning" alternatives. [S] https://ideas.repec.org/a/oup/qjecon/v123y2008i4p1373-1414..html
33. Arteaga, F., Kapor, A. J., Neilson, C. A. and Zimmerman, S. D. (2022), "Smart Matching Platforms and Heterogeneous Beliefs in Centralized School Choice," *QJE* 137(3), 1791-1848. DOI 10.1093/qje/qjac013 (the issue number was not shown in my result; Arteaga is thanked in the draft). Beliefs about admission chances drive choice even under a strategy-proof mechanism. Relation: very close to the "learning" and beliefs alternative, from a Chile setting. [S] https://www.nber.org/papers/w28946
34. Kapor, A. J., Neilson, C. A. and Zimmerman, S. D. (2020), "Heterogeneous Beliefs and School Choice Mechanisms," *AER* 110(5), 1274-1315. DOI 10.1257/aer.20170129. [S] https://www.aeaweb.org/doi/10.1257/aer.20170129

### Strand 6: TK and pre-K program effects

35. Gray-Lobe, G., Pathak, P. A. and Walters, C. R. (2023), "The Long-Term Effects of Universal Preschool in Boston," *QJE* 138(1), 363-411. Lottery-based estimates. Relation: why district interest in pre-K feeding into K is policy-relevant. [S] https://www.nber.org/papers/w28756
36. Cascio, E. U. (2023), "Does Universal Preschool Hit the Target? Program Access and Preschool Impacts," *Journal of Human Resources* 58(1), 1-42. Universal pre-K and low-income gains. [S] https://jhr.uwpress.org/content/58/1/1
37. Manship, K., et al. (2017), *The Impact of Transitional Kindergarten on California Students: Final Report from the Study of California's Transitional Kindergarten Program*, American Institutes for Research. Regression discontinuity; TK students enter K more ready (language, literacy, math, executive function). Relation: the one evaluation of California's TK. Author list is "Manship and colleagues"; I did not retrieve the full author list. [S] https://cached.air.org/sites/default/files/2021-07/Transitional-Kindergarten-Final-Report-June-2017-rev.pdf

I found no academic paper on TK→K "promotion" or feeder priority in SFUSD or elsewhere.

### Strand 7: SFUSD's assignment system specifically

38. Awadelkarim et al. (2023) (above).
39. Awadelkarim, A., Ugander, J., Ashlagi, I. and Lo, I., "Designing Defaults for School Choice," NeurIPS 2021 workshop. SFUSD 2018-19 data; 11% of 4,713 students assigned using distance-based defaults. [S] https://nips.cc/virtual/2021/34475 (workshop paper, not peer-reviewed in a journal)
40. Allman, M., Ashlagi, I., Lo, I., Love, J., Mentzer, K., O'Connell, L. and Ruiz-Setz, "Designing School Choice for Diversity in the San Francisco Unified School District." Working paper, preliminary draft (not for distribution as marked), no journal. [S] https://www.uts.edu.au/sites/default/files/2022-06/SFUSD_Policy_Paper_Draft.pdf. Check with the authors before citing a draft; Stanford pages put the zone policy's start at 2022-23 or 2023-24 and the draft says 2024-25. I did not resolve this. Dissertation: Student assignment design for SFUSD, Stanford, https://purl.stanford.edu/ct364nd4124 [S].
41. District primary sources: the fall 2026 survey page (above) and SFUSD's Annual Assignment Highlights, https://www.sfusd.edu/schools/enroll/student-assignment-policy/annual-assignment-highlights [S]. Also a 2007 Williams College thesis on SFUSD's 2001 move to choice (Fernandez), found in a search result but not read.

### Three-to-five a referee would most expect

1. Agarwal and Somaini (2018), and the 2020 review.
2. Abdulkadiroğlu, Pathak, Schellenberg and Walters (2020).
3. Fack, Grenet and He (2019).
4. Barseghyan et al. (2013, 2018) with Sydnor (2010), for the risk-aversion reading.
5. Azevedo and Leshno (2016). Handel (2013) if the learning/inertia discussion is developed.

---

## BibTeX (verified or search-confirmed entries only)

Fields I could not confirm (for example DOI for Chetty, issue number for Arteaga) are omitted rather than guessed.

```bibtex
@article{apesteguia2018,
  author  = {Apesteguia, Jose and Ballester, Miguel {\'A}ngel},
  title   = {Monotone Stochastic Choice Models: The Case of Risk and Time Preferences},
  journal = {Journal of Political Economy},
  year    = {2018}, volume = {126}, number = {1}, pages = {74--106},
  doi     = {10.1086/695504}
}
@article{barseghyan2013,
  author  = {Barseghyan, Levon and Molinari, Francesca and O'Donoghue, Ted and Teitelbaum, Joshua C.},
  title   = {The Nature of Risk Preferences: Evidence from Insurance Choices},
  journal = {American Economic Review},
  year    = {2013}, volume = {103}, number = {6}, pages = {2499--2529},
  doi     = {10.1257/aer.103.6.2499}
}
@article{barseghyan2018,
  author  = {Barseghyan, Levon and Molinari, Francesca and O'Donoghue, Ted and Teitelbaum, Joshua C.},
  title   = {Estimating Risk Preferences in the Field},
  journal = {Journal of Economic Literature},
  year    = {2018}, volume = {56}, number = {2}, pages = {501--564},
  doi     = {10.1257/jel.20161148}
}
@article{chetty2006,
  author  = {Chetty, Raj},
  title   = {A New Method of Estimating Risk Aversion},
  journal = {American Economic Review},
  year    = {2006}, volume = {96}, number = {5}, pages = {1821--1834}
}
@incollection{harrison2008,
  author    = {Harrison, Glenn W. and Rutstr{\"o}m, E. Elisabet},
  title     = {Risk Aversion in the Laboratory},
  booktitle = {Risk Aversion in Experiments},
  editor    = {Cox, James C. and Harrison, Glenn W.},
  series    = {Research in Experimental Economics},
  volume    = {12},
  publisher = {Emerald},
  year      = {2008}, pages = {41--196},
  doi       = {10.1016/S0193-2306(08)00003-3}
}
@article{holt2002,
  author  = {Holt, Charles A. and Laury, Susan K.},
  title   = {Risk Aversion and Incentive Effects},
  journal = {American Economic Review},
  year    = {2002}, volume = {92}, number = {5}, pages = {1644--1655}
}
@article{rabin2000,
  author  = {Rabin, Matthew},
  title   = {Risk Aversion and Expected-Utility Theory: A Calibration Theorem},
  journal = {Econometrica},
  year    = {2000}, volume = {68}, number = {5}, pages = {1281--1292}
}
@article{sydnor2010,
  author  = {Sydnor, Justin},
  title   = {(Over)insuring Modest Risks},
  journal = {American Economic Journal: Applied Economics},
  year    = {2010}, volume = {2}, number = {4}, pages = {177--199},
  doi     = {10.1257/app.2.4.177}
}
@article{wilcox2011,
  author  = {Wilcox, Nathaniel T.},
  title   = {`Stochastically More Risk Averse:' A Contextual Theory of Stochastic Discrete Choice Under Risk},
  journal = {Journal of Econometrics},
  year    = {2011}, volume = {162}, number = {1}, pages = {89--104},
  doi     = {10.1016/j.jeconom.2009.10.012}
}
@techreport{lagos2025,
  author      = {Lagos, Francisco and Saltmarsh, Jason and Liu, Jing},
  title       = {The Effect of Centralized-Admission School Lotteries on Between-School Segregation: Evidence from 300 Largest School Districts in the United States},
  institution = {Institute of Labor Economics (IZA)},
  type        = {IZA Discussion Paper},
  number      = {18306},
  year        = {2025}, month = dec,
  url         = {https://www.iza.org/publications/dp18306}
}
@article{cabalin2016,
  author  = {Cabalin, Cristian and Antezana, Lorena},
  title   = {Education cover page: the visualization of educational policy in the press},
  journal = {Cuadernos.info},
  year    = {2016}, number = {39}, pages = {195--207},
  doi     = {10.7764/cdi.39.914},
  note    = {Spanish title begins ``La educaci{\'o}n en portada''; roulette-cover claim UNVERIFIED}
}
@article{azevedo2016,
  author  = {Azevedo, Eduardo M. and Leshno, Jacob D.},
  title   = {A Supply and Demand Framework for Two-Sided Matching Markets},
  journal = {Journal of Political Economy},
  year    = {2016}, pages = {1235--1268},
  doi     = {10.1086/687476}
}
@article{abdulkadiroglu2003,
  author  = {Abdulkadiro{\u{g}}lu, Atila and S{\"o}nmez, Tayfun},
  title   = {School Choice: A Mechanism Design Approach},
  journal = {American Economic Review},
  year    = {2003}, volume = {93}, number = {3}, pages = {729--747},
  doi     = {10.1257/000282803322157061}
}
@article{abdulkadiroglu2009,
  author  = {Abdulkadiro{\u{g}}lu, Atila and Pathak, Parag A. and Roth, Alvin E.},
  title   = {Strategy-proofness versus Efficiency in Matching with Indifferences: Redesigning the {NYC} High School Match},
  journal = {American Economic Review},
  year    = {2009}, volume = {99}, number = {5}, pages = {1954--1978}
}
@article{pathak2008,
  author  = {Pathak, Parag A. and S{\"o}nmez, Tayfun},
  title   = {Leveling the Playing Field: Sincere and Sophisticated Players in the {Boston} Mechanism},
  journal = {American Economic Review},
  year    = {2008}, volume = {98}, number = {4}, pages = {1636--1652},
  doi     = {10.1257/aer.98.4.1636}
}
@article{pathak2011,
  author  = {Pathak, Parag A. and Sethuraman, Jay},
  title   = {Lotteries in Student Assignment: An Equivalence Result},
  journal = {Theoretical Economics},
  year    = {2011}, volume = {6}, number = {1}, pages = {1--17}
}
@article{ashlagi2014,
  author  = {Ashlagi, Itai and Shi, Peng},
  title   = {Improving Community Cohesion in School Choice via Correlated-Lottery Implementation},
  journal = {Operations Research},
  year    = {2014}, volume = {62}, number = {6}, pages = {1247--1264},
  doi     = {10.1287/opre.2014.1319}
}
@article{abdulkadiroglu2017,
  author  = {Abdulkadiro{\u{g}}lu, Atila and Agarwal, Nikhil and Pathak, Parag A.},
  title   = {The Welfare Effects of Coordinated Assignment: Evidence from the {New York City} High School Match},
  journal = {American Economic Review},
  year    = {2017}, volume = {107}, number = {12}, pages = {3635--3689},
  doi     = {10.1257/aer.20151425}
}
@article{abdulkadiroglu2020,
  author  = {Abdulkadiro{\u{g}}lu, Atila and Pathak, Parag A. and Schellenberg, Jonathan and Walters, Christopher R.},
  title   = {Do Parents Value School Effectiveness?},
  journal = {American Economic Review},
  year    = {2020}, volume = {110}, number = {5}, pages = {1502--1539},
  doi     = {10.1257/aer.20172040}
}
@article{agarwal2018,
  author  = {Agarwal, Nikhil and Somaini, Paulo},
  title   = {Demand Analysis Using Strategic Reports: An Application to a School Choice Mechanism},
  journal = {Econometrica},
  year    = {2018}, volume = {86}, number = {2}, pages = {391--444},
  doi     = {10.3982/ECTA13615}
}
@article{agarwal2020,
  author  = {Agarwal, Nikhil and Somaini, Paulo},
  title   = {Revealed Preference Analysis of School Choice Models},
  journal = {Annual Review of Economics},
  year    = {2020}, volume = {12}, pages = {471--501},
  doi     = {10.1146/annurev-economics-082019-112339}
}
@article{fack2019,
  author  = {Fack, Gabrielle and Grenet, Julien and He, Yinghua},
  title   = {Beyond Truth-Telling: Preference Estimation with Centralized School Choice and College Admissions},
  journal = {American Economic Review},
  year    = {2019}, volume = {109}, number = {4}, pages = {1486--1529}
}
@article{calsamiglia2020,
  author  = {Calsamiglia, Caterina and Fu, Chao and G{\"u}ell, Maia},
  title   = {Structural Estimation of a Model of School Choices: The {Boston} Mechanism versus Its Alternatives},
  journal = {Journal of Political Economy},
  year    = {2020}, volume = {128}, number = {2}, pages = {642--680}
}
@inproceedings{awadelkarim2023,
  author    = {Awadelkarim, Amel and Seshadri, Arjun and Ashlagi, Itai and Lo, Irene and Ugander, Johan},
  title     = {Rank-heterogeneous Preference Models for School Choice},
  booktitle = {Proceedings of the 29th ACM SIGKDD Conference on Knowledge Discovery and Data Mining (KDD '23)},
  year      = {2023},
  doi       = {10.1145/3580305.3599484}
}
@article{burgess2015,
  author  = {Burgess, Simon and Greaves, Ellen and Vignoles, Anna and Wilson, Deborah},
  title   = {What Parents Want: School Preferences and School Choice},
  journal = {Economic Journal},
  year    = {2015}, volume = {125}, number = {587}, pages = {1262--1289},
  doi     = {10.1111/ecoj.12153}
}
@techreport{hastings2006,
  author      = {Hastings, Justine S. and Kane, Thomas J. and Staiger, Douglas O.},
  title       = {Preferences and Heterogeneous Treatment Effects in a Public School Choice Lottery},
  institution = {NBER},
  type        = {Working Paper},
  number      = {12145},
  year        = {2006}
}
@article{cohen2007,
  author  = {Cohen, Alma and Einav, Liran},
  title   = {Estimating Risk Preferences from Deductible Choice},
  journal = {American Economic Review},
  year    = {2007}, volume = {97}, number = {3}, pages = {745--788}
}
@article{kahneman1979,
  author  = {Kahneman, Daniel and Tversky, Amos},
  title   = {Prospect Theory: An Analysis of Decision under Risk},
  journal = {Econometrica},
  year    = {1979}, volume = {47}, number = {2}, pages = {263--291}
}
@article{koszegi2006,
  author  = {K{\H{o}}szegi, Botond and Rabin, Matthew},
  title   = {A Model of Reference-Dependent Preferences},
  journal = {Quarterly Journal of Economics},
  year    = {2006}, volume = {121}, number = {4}, pages = {1133--1165}
}
@article{dimmock2016,
  author  = {Dimmock, Stephen G. and Kouwenberg, Roy and Wakker, Peter P.},
  title   = {Ambiguity Attitudes in a Large Representative Sample: Measurement and Implications},
  journal = {Management Science},
  year    = {2016}, volume = {62}, number = {5}, pages = {1363--1380},
  doi     = {10.1287/mnsc.2015.2198}
}
@article{gneezy2006,
  author  = {Gneezy, Uri and List, John A. and Wu, George},
  title   = {The Uncertainty Effect: When a Risky Prospect Is Valued Less than Its Worst Possible Outcome},
  journal = {Quarterly Journal of Economics},
  year    = {2006}, volume = {121}, number = {4}, pages = {1283--1309}
}
@article{agranov2017,
  author  = {Agranov, Marina and Ortoleva, Pietro},
  title   = {Stochastic Choice and Preferences for Randomization},
  journal = {Journal of Political Economy},
  year    = {2017}, volume = {125}, number = {1}, pages = {40--68},
  doi     = {10.1086/689774}
}
@article{dwenger2018,
  author  = {Dwenger, Nadja and K{\"u}bler, Dorothea and Weizs{\"a}cker, Georg},
  title   = {Flipping a Coin: Evidence from University Applications},
  journal = {Journal of Public Economics},
  year    = {2018}, volume = {167}, pages = {240--250},
  doi     = {10.1016/j.jpubeco.2018.09.014}
}
@article{meisner2023,
  author  = {Meisner, Vincent and von Wangenheim, Jonas},
  title   = {Loss Aversion in Strategy-Proof School-Choice Mechanisms},
  journal = {Journal of Economic Theory},
  year    = {2023}, volume = {207}, pages = {105588},
  doi     = {10.1016/j.jet.2022.105588}
}
@article{dreyfuss2022,
  author  = {Dreyfuss, Bnaya and Heffetz, Ori and Rabin, Matthew},
  title   = {Expectations-Based Loss Aversion May Help Explain Seemingly Dominated Choices in Strategy-Proof Mechanisms},
  journal = {American Economic Journal: Microeconomics},
  year    = {2022}, volume = {14}, number = {4}, pages = {515--555},
  doi     = {10.1257/mic.20200259}
}
@article{samuelson1988,
  author  = {Samuelson, William and Zeckhauser, Richard},
  title   = {Status Quo Bias in Decision Making},
  journal = {Journal of Risk and Uncertainty},
  year    = {1988}, volume = {1}, number = {1}, pages = {7--59}
}
@article{kahneman1990,
  author  = {Kahneman, Daniel and Knetsch, Jack L. and Thaler, Richard H.},
  title   = {Experimental Tests of the Endowment Effect and the Coase Theorem},
  journal = {Journal of Political Economy},
  year    = {1990}, volume = {98}, number = {6}, pages = {1325--1348},
  doi     = {10.1086/261737}
}
@article{handel2013,
  author  = {Handel, Benjamin R.},
  title   = {Adverse Selection and Inertia in Health Insurance Markets: When Nudging Hurts},
  journal = {American Economic Review},
  year    = {2013}, volume = {103}, number = {7}, pages = {2643--2682},
  doi     = {10.1257/aer.103.7.2643}
}
@article{hastings2008,
  author  = {Hastings, Justine S. and Weinstein, Jeffrey M.},
  title   = {Information, School Choice, and Academic Achievement: Evidence from Two Experiments},
  journal = {Quarterly Journal of Economics},
  year    = {2008}, volume = {123}, number = {4}, pages = {1373--1414},
  doi     = {10.1162/qjec.2008.123.4.1373}
}
@article{arteaga2022,
  author  = {Arteaga, Felipe and Kapor, Adam J. and Neilson, Christopher A. and Zimmerman, Seth D.},
  title   = {Smart Matching Platforms and Heterogeneous Beliefs in Centralized School Choice},
  journal = {Quarterly Journal of Economics},
  year    = {2022}, volume = {137}, pages = {1791--1848},
  doi     = {10.1093/qje/qjac013}
}
@article{kapor2020,
  author  = {Kapor, Adam J. and Neilson, Christopher A. and Zimmerman, Seth D.},
  title   = {Heterogeneous Beliefs and School Choice Mechanisms},
  journal = {American Economic Review},
  year    = {2020}, volume = {110}, number = {5}, pages = {1274--1315},
  doi     = {10.1257/aer.20170129}
}
@article{graylobe2023,
  author  = {Gray-Lobe, Guthrie and Pathak, Parag A. and Walters, Christopher R.},
  title   = {The Long-Term Effects of Universal Preschool in {Boston}},
  journal = {Quarterly Journal of Economics},
  year    = {2023}, volume = {138}, number = {1}, pages = {363--411}
}
@article{cascio2023,
  author  = {Cascio, Elizabeth U.},
  title   = {Does Universal Preschool Hit the Target? Program Access and Preschool Impacts},
  journal = {Journal of Human Resources},
  year    = {2023}, volume = {58}, number = {1}, pages = {1--42}
}
@techreport{manship2017,
  author      = {Manship, Karen and others},
  title       = {The Impact of Transitional Kindergarten on {California} Students: Final Report from the Study of {California}'s Transitional Kindergarten Program},
  institution = {American Institutes for Research},
  year        = {2017}, month = jun,
  note        = {Full author list not retrieved}
}
```

**Not included in BibTeX, UNVERIFIED:** "Thayer", the Cabalin roulette image, the "71 of 300" figure, the "Rituals of reason" paper, and the unpublished Allman et al. draft.

## Suggested next steps for the authors
1. Run `pdftotext` on the Lagos IZA PDF; resolve the "71" and "as of 2022" claim.
2. Get Thayer's first name or a link from the authors.
3. Open the Cabalin and Antezana PDF and look for the roulette cover, or replace the citation.
4. Replace the "XX%" with the SFUSD fall 2026 survey figures (2,978 respondents; 82% guaranteed spot near home), and update the scratchpad's 73%/71%/2,725 numbers.
5. Fix "national average of 15%" against the NCES 9% before the next round.

All paths and URLs are as given above. The paper directory is `/Users/kylew/git/sfusd-public/analyses/lottery-tk-promotion`.