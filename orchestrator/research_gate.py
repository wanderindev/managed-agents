"""The research-gate experiment (#14): can a harnessed agent clear PIC's bar?

    python -m orchestrator.research_gate --topic "..." --subtopic "..." [...]

PIC's article pipeline gates research quality server-side: at least 4,000
words, a references heading, and at least two required subtopics substantively
covered. Today that bar is cleared by claude.ai's attended deep research. The
experiment this module enqueues asks whether a Claude Code agent with ordinary
web search, run unattended inside this harness, clears the same bar — graded
by a fresh context against the rubric, never by the author. Either answer is
worth knowing.

This is also the reference consumer of the #14 machinery, and it demonstrates
the rule that makes rubric grading honest: the rubric is written HERE, at
planning time, before any output exists, and rides the run payload unchanged
through every write, grade, and revise in the chain. A rubric written after
seeing the output grades the output that exists rather than the output that
was wanted.

The score trajectory needs no extra machinery: every grading pass is its own
``rubric_verify`` run whose per-criterion verdicts sit in the event log, so
"cleared on pass 1" versus "cleared on pass 2" is a query, and the difference
between those two facts is the interesting part.
"""

import argparse
import logging
import re
import sys

from orchestrator import jobs
from orchestrator.db import connect
from orchestrator.log import create_run

logger = logging.getLogger(__name__)


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


#: PIC's research validation gate, restated as explicit criteria. Reused, not
#: invented: this is the bar the production pipeline already enforces.
PIC_RESEARCH_RUBRIC = [
    {
        "id": "word_count",
        "criterion": (
            "The document contains at least 4,000 words of substantive prose,"
            " excluding the references section."
        ),
        "check": "Compute the body word count; state the number.",
    },
    {
        "id": "references",
        "criterion": (
            "A references (or sources) heading is present, and its entries"
            " correspond to sources actually cited in the text."
        ),
        "check": "Find the heading; spot-check entries against in-text citations.",
    },
    {
        "id": "subtopic_coverage",
        "criterion": (
            "At least two of the required subtopics are substantively covered:"
            " multiple paragraphs of specific, sourced material each, not a"
            " mention under a heading."
        ),
        "check": "Judge each required subtopic's treatment; name which qualify.",
    },
]


def enqueue(
    conn,
    *,
    topic: str,
    subtopics: list[str],
    repo: str = "feliu-dev",
    slug: str | None = None,
) -> int:
    """Create the experiment's opening run. Returns the run id.

    The artifact lives on a scratch branch under docs/ (a tracked path — both
    repos gitignore ai_generated_content, which a first draft of this learned
    the cheap way, from the dreaming job's audit rather than a failed push).
    """
    slug = slug or _slugify(topic)[:60]
    payload = {
        "repo": repo,
        "topic": topic,
        "subtopics": subtopics,
        "rubric": PIC_RESEARCH_RUBRIC,
        "artifact_path": f"docs/experiments/research-gate-{slug}.md",
    }
    with conn.transaction():
        run_id = create_run(
            conn, jobs.RESEARCH_WRITE_KIND, f"research-gate:{slug}", payload
        )
    logger.info(
        "enqueued research-gate experiment %s (run %s, %s subtopics)",
        slug,
        run_id,
        len(subtopics),
    )
    return run_id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument(
        "--subtopic",
        action="append",
        required=True,
        help="repeatable; the rubric requires substantive coverage of >= 2",
    )
    parser.add_argument("--repo", default="feliu-dev")
    parser.add_argument("--slug", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s"
    )
    with connect() as conn:
        enqueue(
            conn,
            topic=args.topic,
            subtopics=args.subtopic,
            repo=args.repo,
            slug=args.slug,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
