# Course Outline

## Course Goal
Enable MCP students to design, critique, and evaluate agentic AI workflows for urban planning and transportation research tasks.

## Target Audience
- University of Pennsylvania Master of City Planning (MCP) students
- Graduate students in planning, transportation, and urban analytics
- Early-stage researchers designing empirical planning studies

## Prerequisites
- Basic Python familiarity (variables, loops, functions)
- Basic understanding of LLMs and prompting
- Familiarity with research design concepts (validity, reliability, bias)
- No prior agent-framework experience required

## Session Format (120 minutes total, including break)
- 110 minutes instruction + activities
- 10 minute break
- One core hands-on Python workflow build + one shorter Claude Code comparison demo
- *Note: Claude Code (used in Tutorial B) refers to Anthropic's command-line agentic tool that executes multi-step tasks via natural language prompts rather than hand-written code.*

## 1) Framing: What Agentic Workflows Are (10 minutes)
### Goal
Build a shared vocabulary and define the standards for research-grade use.
### Key concepts
- LLM vs AI system vs agentic workflow
- Core components: planner, tool use, memory/context, evaluator
- "Research-grade" outputs vs exploratory outputs
### Activity
- Instructor walkthrough of one workflow diagram and opening prompt: "What counts as publishable or defensible evidence in planning research?"
### Output artifact
- Shared concept map / vocabulary sheet used throughout class
### Assessment prompt
*(Cold call or quick show of hands)*
- What is one feature that makes a workflow "agentic" rather than a single prompt?

## 2) When Agentic AI Helps (and Hurts) in Planning Research (10 minutes)
### Goal
Clarify where agentic workflows create value and where they introduce unacceptable risk.
### Key concepts
- Speed, scale, reproducibility, and auditability tradeoffs
- Common failure modes: hallucination, weak sourcing, over-automation, hidden assumptions
- **Concrete failure example:** Instructor presents one case where an agentic workflow produced plausible-but-wrong planning data (e.g., a city GDP figure confidently extracted from a stale or misidentified source) — use this to anchor all subsequent failure-mode discussions
- Data licensing and terms of use: automated collection may violate source restrictions even when data appears publicly accessible
- Equity risk: agentic workflows can systematically underrepresent data-poor geographies (smaller cities, rural areas, Global South municipalities) — outputs may appear complete while being silently biased
### Activity
- Instructor shows a manual vs agent-assisted workflow comparison for one planning research task
### Output artifact
- Two-column list: "good candidates" vs "high-risk uses"
### Assessment prompt
*(Pair-share for 2 minutes, then 1–2 students share aloud)*
- Name one planning task where agentic AI is useful and one where human judgment must dominate.

## 3) Urban Planning Use Cases and Validity Clinic (20 minutes)
### Goal
Connect agentic workflow design to domain-relevant planning and transportation tasks.
### Key concepts
- Policy monitoring, socioeconomic indicators, safety indicators, comparative city benchmarking
- Construct validity, cross-source consistency, and measurement drift
- Evidence quality in academic vs practice settings
### Activity
- Three short case snapshots:
1. City-level GDP collection and validation
2. Road safety metric extraction and normalization
3. Multi-source evidence synthesis for planning memos
- Small-group discussion: identify one major validity risk and one validation check for each case
### Output artifact
- Domain task matrix: task type, likely data sources, validity risks, validation checks
### Assessment prompt
*(Small-group discussion output — groups share one answer each)*
- Which case is hardest to automate reliably, and why?

## 4) Tutorial A (Core Build): Python Agentic Workflow for City GDP Extraction (35 minutes)
### Goal
Build and inspect a compact Python workflow that searches, extracts, and validates city-level GDP values.
### Key concepts
- Multi-step workflow design: plan -> search -> extract -> validate -> review
- Source prioritization and domain filtering
- Schema checks and confidence labeling
- Human-in-the-loop verification for low-confidence records
### Activity
1. Define task schema and success criteria (fields, units, year, evidence link)
2. Run a prepared Python/Colab workflow skeleton for search + extraction
3. Add or inspect a simple validation layer (source quality + missing/format checks)
4. Review low-confidence outputs and document assumptions/failures
### Output artifact
- Runnable Python notebook / script workflow
- Structured output table (`city`, `year`, `GDP`, `source`, `confidence`)
- Brief method note (provenance, assumptions, known limitations)
### Assessment prompt
*(Pair-share for 2 minutes, then 1–2 students share aloud)*
- Which validation step most improved trustworthiness in your workflow?

-- break (10 minutes) --

## 5) Tutorial B (Comparison Demo): Claude Code Workflow for the Same Task (20 minutes)
### Goal
Compare a no-code / prompt-driven workflow to the Python workflow using the same GDP extraction task.
### Key concepts
- Planning-first decomposition
- Skill-guided execution and tool use
- Prompt/process logging as an audit trail
- Where abstraction improves speed but reduces methodological control
### Activity
1. Define objective, output schema, and evidence requirements
2. Run a guided Claude Code workflow for a small subset of cities
3. Inspect outputs, evidence links, and low-confidence cases
4. Identify what is easier vs harder than the Python workflow
### Output artifact
- Prompt-driven workflow log
- Sample structured output table with evidence links
- Short critique note on reliability, transparency, and scalability
### Assessment prompt
*(Pairs compare notes from Tutorials A and B, share one contrast)*
- What did the no-code workflow make easier, and what did it make harder to verify? *(Note: Section 6 will use these observations to build the decision rubric — keep notes.)*

## 6) Comparison Workshop: Choosing the Right Workflow (5 minutes)
### Goal
Help students select an appropriate workflow style for a specific planning research task.
### Key concepts
- Tradeoffs: control, speed, reproducibility, maintenance, transparency
- Matching workflow style to task stakes and data quality
### Activity
- Side-by-side comparison of Python vs Claude Code using a decision rubric
- Students pick one workflow for a hypothetical planning assignment and justify the choice
### Output artifact
- Workflow selection rubric / decision checklist
### Assessment prompt
*(Individual written reflection — 1 minute)*
- For a semester project, which workflow would you start with and why?

## 7) Limitations, Governance, and Next Steps (10 minutes)
### Goal
Consolidate learning and define safe adoption steps for graduate research projects.
### Key concepts
- Source attribution, transparency, and audit trails
- Reproducibility and documentation requirements
- Data licensing and terms of use: confirm automated collection is permitted by source before using outputs in research
- Communicating uncertainty to readers and supervisors: confidence scores and method notes must be surfaced in final deliverables, not buried in the workflow log
- Ethical and methodological limits of agentic workflows in planning research
### Activity
- Return to the concept map from Section 1: what would you revise or add now that you've built and compared workflows?
- Whole-class debrief + exit ticket: one workflow use case, one risk, one safeguard
### Output artifact
- Practical checklist for using agentic workflows in MCP research projects
### Assessment prompt
*(Written exit ticket — 2 minutes)*
- What safeguard is non-negotiable before using agent-generated results in an academic deliverable?

## Overall Learning Outcomes
By the end of the class, learners can:
1. Define the core components of an agentic AI workflow
2. Identify planning research tasks that are appropriate (or inappropriate) for agentic automation
3. Build and evaluate a basic code-first workflow for structured evidence collection
4. Critique a no-code workflow in terms of rigor, transparency, and control
5. Choose a workflow style and validation strategy appropriate to task stakes

## Instructor Notes (Optional, for delivery prep)
- Pre-load a working Colab notebook or local environment to avoid setup delays during class.
- Use the same GDP task in both tutorials so comparisons are concrete.
- Prepare a partially completed output table in case live tool calls fail or run slowly.
- Tutorial A is 35 minutes but covers four distinct steps — scope steps 3 and 4 tightly; if time runs short, demonstrate the validation layer rather than having all students build it independently.
- Print or display the Section 1 concept map so it can be physically revisited in Section 7. A wall-posted version works well for the closing debrief.
- Have a concrete failure example ready for Section 2 (e.g., a screenshot of an agent output with a plausible but wrong GDP figure and its misidentified source). This anchors abstract failure-mode discussion in something students can see.
