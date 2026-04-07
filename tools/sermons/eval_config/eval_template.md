# Speaker Evaluation Template — v3.2

This file defines the required structure for all speaker evaluations (v3.1 and beyond).
It is used as a reference when generating evaluations via Claude.

---

## Rules (baked into every evaluation)

- **No named comparisons.** Never say "unlike Nathan Ekama" or "compared to Robin Webber." Always use generic phrasing: "compared to others in this group," "the most passage-based of the three," "above average for this corpus."
- **All statistics and metrics must be cited inline in the prose** — not just listed in a table. If a number appears in the Metrics Dashboard, it must also appear in the relevant section narrative.
- **Quote specific sermon titles and sermon phrases** to support every major claim. Bare assertions without textual evidence are not acceptable.
- **Every section must be present in every evaluation — no skipping.** If data is insufficient for a section, note the limitation explicitly and provide the best available analysis.
- **No "congregation" or "pastor" as a role title.** These speakers are not clergy in an official pastoral role. Use "audience," "the room," or "listeners" instead of "congregation." Use "speaker" or "teacher" instead of "pastor" as a title. "Pastoral warmth," "pastoral voice," and "pastoral consequence" are acceptable as descriptions of communication quality, not job role.
- **No arrow notation for emotional register.** Do not write "warmth → urgency → awe → humor" — this implies a sequence or causation. Write the emotional palette as prose: "The emotional register spans warmth, urgency, awe, and humor."
- **UCG doctrinal context (for Theological Guardrails section):** UCG's distinctive doctrines include seventh-day Sabbath and annual holy day observance, kingdom theology (theocratic millennium, believers as future rulers), conditional human immortality (no immortal soul — humans do not have an inherently immortal soul; they become living souls), clean and unclean meats, tithing, and a binitarian Christology (Father and Son, distinct persons — not a Trinity). When evaluating Theological Guardrails, assess whether the speaker engages the full complexity of their own tradition — not whether they are doctrinally correct, but whether handling is clear, nuanced, and pastorally honest. Note areas that are thin, potentially one-sided, or where a thoughtful listener within UCG might wish for more depth.

---

## Structure (in order)

---

### SNAPSHOT

Immediately after the document title, before the Executive Summary.
Rendered as a navy blue box in the .docx output.

Contains four labeled subsections in this exact order:

**SPEAKER** — One-line characterization. Three evocative words or short phrases (bold), one per dimension: Voice, Relationship, Calling. No labels — just the words.

**CORE MESSAGE** — One sentence. The irreducible thesis this speaker returns to across the corpus.

**CORPUS SNAPSHOT** — One data line: [N] sermons · [YYYY–YYYY] · [Primary location(s) with %] · [WPM] WPM · FK Grade [X.X] · [Primary style]

**EVALUATED** — One sentence: how many sermons were sampled, date range, and method (Grok structural extraction + Claude synthesis, or direct close read).

Example format — output EXACTLY this heading structure (the `## SNAPSHOT` line is required):
```
## SNAPSHOT

#### SPEAKER
**Older Brother. Trusted Neighbor. Steward.**

#### CORE MESSAGE
His central conviction is that daily faithfulness — in speech, dignity, mercy, and service — is vocational preparation for governing cities in the coming kingdom.

#### CORPUS SNAPSHOT
83 sermons · 2011–2026 · Spokane (primary) · 126 WPM · FK Grade 6.6 · Topical

#### EVALUATED
15 sermons, 2011–2026 · Grok structural extraction + Claude synthesis · 5 most recent read directly
```

---

### EXECUTIVE SUMMARY

Immediately after the SNAPSHOT.

**Block 1 — Three Characterizations (no analytical labels)**
Three short paragraphs capturing Voice, Relationship, and Calling.
Written as recognitions, not definitions. The subject should read these and immediately say "yes, that's right."
- Bold the three key words (one per paragraph) at first use.
- No headers, no labels, no "At the voice level:" framing. Just three flowing paragraphs.
- Each paragraph is 2–3 sentences.

**Block 2 — Six Conversational Bullets**
Written as short prose sentences, not data labels:
- "His central conviction across [N] sermons is..."
- "He speaks at [WPM] WPM in a [style] style, readable at roughly grade [FK]..."
- "His strongest moves are..."
- "His primary growth edge is..."
- "Corpus: [N] sermons, [YYYY–YYYY], [locations]"
- (Optional sixth bullet for anything distinctive)

---

### KEY TAKEAWAYS

Written after completing the full analysis. A one-page distillation for quick orientation before the full depth read. Every item must be specific and evidence-based — no generic homiletical observations.

**5 Strengths** (one sentence each — name the strength concretely, not generically):
1.
2.
3.
4.
5.

**3 Growth Edges** (one sentence each — specific, constructive, grounded in observed patterns):
1.
2.
3.

**Single Highest-Leverage Recommendation:**
[One sentence — the one change that would most transform this speaker's work, drawn from the full analysis. This must be identical to the recommendation made in the Conclusion.]

---

### SERMONS ASSESSED

Immediately after the Executive Summary, before the Metrics Dashboard.
A simple list of the 15 sermons used in this evaluation (title, date, source).
Purpose: makes the evidence base transparent to the reader.

- *[Sermon Title]* — [YYYY-MM-DD] · [TXT/MD]
- ...

---

### METRICS DASHBOARD

After Sermons Assessed, before PART ONE.
Three panels of data pulled from the speaker dossier. Each table has three columns: Metric, What It Measures, Value. Interpretation goes in the "In Context" paragraph beneath each panel.

Based on [X] full sermons (sermonettes and Bible studies excluded from NLP/scripture aggregates). Pipeline run: [YYYY-MM-DD].

#### Panel 1 — Delivery Metrics

| Metric | What It Measures | Value |
|---|---|---|
| Words per minute (WPM) | Delivery pace. 120–140 = deliberate; 150–170 = conversational; 180+ = fast. UCG median ~150. | |
| Flesch-Kincaid grade level | Reading complexity. Grade 6–8 = accessible; 10–12 = educated adult; 15+ = academic density. | |
| Flesch Reading Ease | Readability score (inverse of FK). 60–70 = conversational; below 50 = dense; above 70 = plain. | |
| Type-token ratio (TTR) | Vocabulary richness. Below 0.45 = repetitive; 0.50–0.55 = average; above 0.60 = unusually varied. | |
| Filler word rate (per 1,000w) | Verbal hesitation density (um, uh, you know). Under 2 = clean; 2–5 = moderate; above 5 = distracting. | |
| Lowry Loop usage (% of sermons) | Whether the sermon builds emotional tension before resolution. High % = intentional arc; low % = linear. | |
| Question rate (per 1,000w) | How often the speaker invites the audience to think rather than receive. Higher = more dialogic. | |
| Pronoun — I (per 1,000w) | Self-reference rate. How much personal disclosure and experience drives the teaching. | |
| Pronoun — We (per 1,000w) | Communal voice rate. Higher = fellow traveler; lower = more directive or authoritative. | |
| Pronoun — You (per 1,000w) | Direct address rate. Higher = application-heavy, audience-facing style. | |
| Pronoun — God/He/Lord (per 1,000w) | Theocentric reference rate. The most reliable signal of whether God or the audience is the center of the sermon. | |
| Dominant pronoun | The center of gravity of the speaker's discourse — whose story is most prominently being told. | |
| Average sermon length (words) | Raw output volume. UCG full sermons average 7,000–9,000 words; sermonettes 2,000–3,500. | |
| Average sentiment score | Aggregate emotional tone. -1 = uniformly negative; +1 = uniformly positive. Most sermons cluster +0.85 to +0.98. | |

#### Year-over-Year Trends

| Year | Sermons | WPM | FK Grade | Filler/1k | Lowry Loop % | Sentiment |
|---|---|---|---|---|---|---|
| [YYYY] | | | | | | |

*(Fill with available yearly data from dossier YoY section. Omit years with fewer than 3 sermons.)*

**In Context — Panel 1:**
[Write 3–5 sentences as a single flowing paragraph. Answer: what do these numbers reveal about this speaker as a communicator? Name at least two metrics by specific value. Identify the most interesting or unexpected signal relative to a typical UCG speaker. Explain what the YoY trend line shows — is the speaker getting faster, slower, more precise, more scattered? This should read like interpretation by someone who knows what the numbers mean, not a list of observations.]

#### Panel 2 — Scripture & Preaching Style

| Metric | What It Measures | Value |
|---|---|---|
| OT/NT citation ratio | Theological anchoring across the canon. Heavier OT = covenantal/legal/prophetic; heavier NT = grace/ecclesiology/epistles. | |
| Preaching style classification | Structural approach. Topical = argument from multiple texts; Passage-based = works through one text; Proof-text = Scripture as decoration. | |
| Citations per sermon (average) | Volume of Scripture engagement. Under 10 = light; 10–20 = moderate; 30+ = Scripture-dense. | |
| Unique books cited per sermon (average) | Canonical breadth. 3–5 = focused; 6–9 = ranging; 10+ = panoramic. | |
| Exposition depth score | How deeply individual passages are worked. 0 = verse grab; 1 = verse-by-verse. Most preachers fall 0.25–0.55. | |

**Preaching Style Breakdown:**

| Style | Sermons | % |
|---|---|---|
| | | |

**Top 15 Books Cited:**

| Book | Citations | % of total |
|---|---|---|
| | | |

**In Context — Panel 2:**
[Write 3–5 sentences as a single flowing paragraph. What does the scripture profile reveal about this speaker's theological instincts and habits? Name the OT/NT ratio and depth score by value. Comment on the Top Books list — what does it tell you about the conceptual world this speaker lives in? Is there a notable absence (e.g., the Gospels barely appearing in someone who claims to preach Christ)? This should go beyond the numbers to say something about how this speaker thinks theologically.]

#### Panel 3 — Topic & Occasion

**Top Topics** (BERTopic or synthesized, [N] clusters):

| Topic | Sermons |
|---|---|
| | |

**Occasion Distribution:**

| Occasion | Count | % |
|---|---|---|
| Sermon | | |
| Holy Day | | |
| Bible Study | | |
| Other | | |

**Additional Corpus Details:**
- Total sermons in corpus:
- Transcripts processed: (% of total)
- Date range:
- Major Series: [list if applicable]

**In Context — Panel 3:**
[1–3 sentences maximum. Only include if there is something non-obvious to say — something the tables do not make immediately clear. If the topic and occasion distribution is self-explanatory, omit this paragraph entirely.]

---

### PART ONE — PATTERN-LEVEL ANALYSIS

#### 1. The Central Thesis — What He Is Always Saying

What is this speaker's irreducible thesis? Identify the single idea that recurs across the corpus, stated or unstated. Cite at least two sermon titles and one direct phrase as evidence.

#### 2. What He Assumes His Audience Struggles With

What does this speaker assume about the internal condition of the people in the room? What failures, doubts, temptations, or misconceptions does he address most frequently? Cite specific sermon examples. (Note: the secondary assumption — the underlying model of the listener — often clarifies the primary one.)

#### 3. The Images and Metaphors He Reaches For

What images, analogies, and metaphor clusters recur across this speaker's sermons? Describe the world they build through figurative language. Sub-categorize into named domains where patterns are strong. Quote at least one representative metaphor per domain, with sermon title.

#### 4. What He Argues vs. What He Takes for Granted

What does this speaker treat as self-evident without argument? What does he argue for explicitly? Identify the load-bearing assumptions that underpin the teaching — the things newcomers or skeptical listeners would need to have established before the rest of the corpus can land.

#### 5. How Much Personal Experience He Brings In

How much does this speaker draw on personal experience? Characterize the nature and frequency of self-disclosure. Is the personal material illustrative, confessional, or authoritative? Name specific anecdotes and what they are used to demonstrate.

#### 6. The Dangers and Failures He Warns Against Most

What dangers, sins, errors, or failure modes does this speaker warn against most often? Produce a ranked or categorized map of their hazard landscape with supporting examples. Note whether the primary danger in his imagination is dramatic (apostasy, gross sin) or quiet (drift, assimilation, slow erosion).

#### 7. How He Uses the Bible

Beyond the raw citation data in the Metrics Dashboard, analyze how this speaker uses Scripture. Does he quote for proof, for atmosphere, for narrative, or for exposition? Identify his go-to texts and explain why those texts recur. Cite specific passages and sermons. Note any significant absences.

#### 8. Where He Is Strongest as a Communicator

Identify 3–5 specific, textually grounded rhetorical strengths. Each strength should be named, described, and illustrated with a quote or sermon example.

#### 9. What Would Make Him Noticeably Stronger

Numbered list of exactly 5 growth opportunities. Each should be specific, constructive, and grounded in observed patterns — not generic homiletical advice. Frame each as something achievable, not a personality transplant.

1.
2.
3.
4.
5.

#### 10. Theological Guardrails & Potential Risks

Two-part section. Be direct. This is the most honest section in the evaluation — the place where patterns that feel like strengths may carry costs that only become visible over a full corpus.

**Canonical Breadth:**
What significant portions of the canon (books, genres, testaments) appear rarely or not at all in this corpus? Name the absence and explain what the listener is therefore not receiving. Is the gap a genuine blind spot, a reasonable focus given the corpus, or a structural habit? If a major genre is missing (e.g., lament, apocalyptic, wisdom), name what its absence costs pastorally.

**Topic Concentration & Theological Blind Spots:**
Is there a dominant theme that appears so frequently it may crowd out equally important biblical emphases? Name it. Then name what gets less airtime than it deserves — and what kind of listener is therefore underserved. Is there any UCG-distinctive doctrinal area where this speaker's handling is notably thin, potentially one-sided, or where a thoughtful listener would wish for more nuance? Cite at least one specific example from the corpus. This is not a critique of UCG doctrine — it is an honest assessment of whether the speaker engages the full complexity of their own tradition.

---

### PART TWO — VOICE, TEXTURE & DEVELOPMENT

#### S1. Voice and Rhythm on the Page

Describe the speaker's vocal and rhetorical rhythm as it appears in the text. Is the prose staccato or flowing? Where do they accelerate, pause, or repeat for effect? Note specific verbal habits (tag questions, self-corrections, pacing devices). Quote a representative passage.

#### S2. How He Opens Sermons

How does this speaker begin sermons? Analyze the opening moves across the corpus: do they open with Scripture, story, question, problem statement, or announcement? Identify the dominant pattern and its effect. Name specific strong openings and weak ones by sermon title.

#### S3. Teaching Mode vs. Relational Mode

Estimate the ratio of didactic (teaching/explaining) content to relational (connecting/caring/sharing) content across the corpus. Include a ratio estimate (e.g., 70/30). Explain what drives this ratio and what it signals about the speaker's self-understanding. Note specific moments where the relational voice breaks through the teaching mode.

#### S4. The Emotional Register He Works In

What emotions does this speaker express, evoke, or suppress? Describe the emotional palette of the corpus — what is present, what is absent, and what the ceiling and floor are. Do not use arrow notation (→) to describe the range. Write as prose. Cite specific examples of the high and low points of emotional engagement. Note what is conspicuously absent and what that absence costs pastorally.

#### S5. How Much of Himself He Puts on the Line

Does this speaker include themselves in the challenges they present? Do they speak at the room, with the room, or as a fellow traveler? Cite specific instances of self-implication — both what is present and what is notably absent.

#### S6. How He Relates to the People in the Room

Present three characterizations at three different levels — voice, relationship, calling — as was done in the Executive Summary, but here with textual grounding for each. Quote specific sermon moments that support each characterization. Note the tension or interplay between the three if they pull in different directions.

#### S7. How He Handles Hard Texts and Uncomfortable Topics

How does this speaker address difficult texts, theological tensions, or uncomfortable contemporary issues? Does he resolve tension quickly, sit with it, avoid it, or reframe it? Cite at least one example of a difficult moment and how it was handled. Note where the pivot from problem to answer comes earlier than the weight of the problem warrants.

#### S8. His Most Distinctive Theological Contribution

Name and describe this speaker's most distinctive theological contribution — the idea, emphasis, or framework that is uniquely theirs within this corpus. This is not a generic summary of their theology; it is the specific thing that sets them apart. Name it concretely. Explain what it changes about the listener's understanding of the Christian life if it's received.

#### S9. What a First-Time Listener Would Notice and Feel

Write a short narrative (100–150 words) from the perspective of someone encountering this speaker for the first time. What would they notice immediately? What would surprise them? What impression would they leave with?

#### S10. How He Has Changed Over the Years

Divide the speaker's corpus chronologically into three eras. For each era, describe what was distinctive about their preaching at that stage. Identify the trajectory: has the speaker grown more complex, more direct, more personal, more Scriptural? Cite specific sermons or dates as anchors. Name a direction for the next chapter.

**Era 1 — [YYYY–YYYY]:**

**Era 2 — [YYYY–YYYY]:**

**Era 3 — [YYYY–YYYY]:**

#### S11. Who He Is Actually Preaching To

Who is this speaker actually preaching to? Drawing only on signals present in the corpus — the failure modes they warn against, the assumptions they never argue, the vocabulary they use without definition, the illustrations they reach for — construct a concrete profile of the people in the room.

Address the following in a single flowing prose section (150–200 words):

- **Demographic:** Estimate age range, likely tenure in the faith, and any education or occupation signals visible in the corpus (word choice, illustration domains, economic warnings, cultural references).
- **Spiritual profile:** What does this person struggle with? What do they hunger for that this speaker consistently provides? What do they take for granted — the doctrinal furniture that is never argued because this audience already accepts it?
- **Served exceptionally well:** Name the listener types this speaker reaches with unusual effectiveness, and explain why the fit is strong.
- **The gap:** Name the listener types this speaker is less well-suited for — who gets left behind, underserved, or implicitly excluded by this speaker's defaults. Be specific about what in the speaker's approach creates the gap.

Keep every claim grounded in actual corpus signals. Do not produce a generic member profile — produce the specific profile implied by this speaker's choices.

---

### CONCLUSION

Final section, after PART TWO. Three paragraphs.

**Paragraph 1 — What Only He Does:** What does this speaker do that no one else in this corpus does? Name the specific gift, the distinctive move, the unrepeatable quality. This paragraph must be particular, not generic.

**Paragraph 2 — The Distance Between Now and His Ceiling:** Describe the gap between this speaker's current form and their potential. What is the gap made of — habit, theology, temperament, or context? Be honest and constructive.

**Paragraph 3 — The Single Change That Would Matter Most:** If this speaker could change one thing that would most transform their work, what is it? State it plainly in a single sentence, then explain why this one change and not another.

---

## Version History

- **v1** — 3-sermon TXT close read; no structured metrics; no named comparison rule; no Executive Summary
- **v2** — 20% stratified TXT sample; named comparison rule added; S10 (Development Arc), Executive Summary, Metrics Dashboard, and Conclusion sections added
- **v2.1** — S11 (Ideal Audience Profile) added to PART TWO, after S10
- **v3** — Metrics Dashboard redesigned: clean value-only tables + "Key Readings" interpretive bullets + YoY trend table; PART TWO S1–S4 written from direct TXT close read (5 most-recent sermons) rather than Grok extraction mediation
- **v3.1** — Executive Summary restructured (three characterizations at voice/relationship/calling levels + 6 conversational bullets); QUICK CARD blue box added; Sermons Assessed section added; all section titles made descriptive; arrow notation prohibited; congregation/pastor ontology removed; Panel 3 Key Readings simplified; Conclusion section titles clarified; table rendering fixed in .docx renderer
- **v3.2** — QUICK CARD renamed to SNAPSHOT; SNAPSHOT expanded to four labeled subsections (SPEAKER, CORE MESSAGE, CORPUS SNAPSHOT, EVALUATED); metrics tables upgraded to 3-column format (Metric | What It Measures | Value); "Key Readings" bullets replaced with "In Context" prose paragraphs that interpret data in context rather than listing observations; Panel 1 renamed Delivery Metrics; Preaching Style Breakdown and Top Books % column added to Panel 2; Additional Corpus Details block added to Panel 3; pipeline run date added to dashboard header
- **v3.3** — KEY TAKEAWAYS section added (after Executive Summary, before Sermons Assessed): 5 strengths, 3 growth edges, single highest-leverage recommendation; Section 10 THEOLOGICAL GUARDRAILS & POTENTIAL RISKS added to Part One (after Section 9, before Part Two): canonical breadth + topic concentration/blind spots; UCG doctrinal context rule added to Rules block
