# Tutorial 2: Agent-Driven Data Collection (No-Code / Skill-Guided)

## Purpose
This tutorial shows how a fully agent-driven workflow can improve research outputs when the agent is given:
- a clear task prompt,
- explicit research constraints, and
- reusable skills (process rules).

Students will compare three runs of the same city GDP task:
1. A weak baseline prompt
2. A stronger research-grade prompt
3. A skill-guided prompt (planning + source triage + validation)

The goal is to make the difference between **prompting** and **workflow discipline** visible.


## Learning Goals (Student-Facing)
By the end of this tutorial, students should be able to:
1. Compare baseline vs skill-guided agent outputs for the same research task.
2. Explain why explicit workflow structure improves reliability and transparency.
3. Identify weak sources, missing evidence, and uncertainty flags in agent outputs.
4. Argue when a no-code workflow is sufficient and when code-first methods are better.

## Folder Structure
- `prompts/`: sample prompts used in the live demo
- `skills/`: compact tutorial skills (`SKILL.md`) to guide agent behavior
- `examples/`: optional comparison rubric and note templates for classroom discussion

## Skills in Agent Environments (Mini-Explanation)
In this tutorial, a **skill** is a reusable set of instructions (usually a `SKILL.md` file) that teaches the agent how to approach a task consistently.

Skills are useful because they:
- reduce prompt repetition,
- encode workflow discipline (planning, validation, triage),
- make outputs more consistent across runs,
- make the process easier to audit and teach.

This is the key distinction students should see:
- **Prompt only**: asks for an answer
- **Prompt + skills**: defines a process for producing and checking the answer

## Skill Setup and Demo (Including Superpowers)
This project uses two types of skills during the tutorial:
- **Superpowers skills** (general process skills, installed in the Codex environment)
- **Tutorial-local skills** (the three skills in this folder, designed for the class demo)

### 1) Superpowers setup (Codex environment)
Run the bootstrap command once:
```bash
~/.codex/superpowers/.codex/superpowers-codex bootstrap
```

Then load a skill when needed:
```bash
~/.codex/superpowers/.codex/superpowers-codex use-skill superpowers:brainstorming
```

Other useful examples to show students (optional):
- `superpowers:writing-plans` (planning structured work before implementation)
- `superpowers:systematic-debugging` (debugging workflow discipline)
- `superpowers:using-superpowers` (meta guidance on when and how to use skills)

### 2) Tutorial-local skills (this folder)
These are the class-specific skills used in the skill-guided prompt:
- `skills/planning-research-extraction/SKILL.md`
- `skills/web-search-source-triage/SKILL.md`
- `skills/evidence-table-validation/SKILL.md`

Use them by either:
- pasting the key rules into the agent session, or
- asking the agent to read and follow them before executing the task.

