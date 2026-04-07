import os

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Local PROCESSED_DIR removed — output is returned as a string and uploaded
# to Azure Blob by the caller (pipeline_grok.py / batch_grok.py).

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url="https://api.x.ai/v1",
)

MODEL = os.getenv("GROK_MODEL", "grok-3-fast")
MAX_TOKENS = 32000  # Single call must hold cleaned transcript + all analysis sections


# ─────────────────────────────────────────────────
# Single-call system prompt
# ─────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are an expert sermon editor and analyst. You will receive a raw ASR (speech-to-text) transcript of a sermon.

Produce ALL of the following sections in a single response, in this exact order:

---

## Cleaned Transcript

First, produce the full cleaned transcript:
1. Fix all speech recognition errors, including misheard words, wrong proper nouns, garbled phrases, and biblical names.
2. Remove all filler words and verbal tics: um, uh, you know, like, right, okay (when used as filler), so (when used as a sentence-starter filler).
3. Add natural paragraph breaks where the speaker shifts topic, pauses for emphasis, or moves to a new point.
4. Keep the FULL content — do not summarize, shorten, or omit anything. This should read as a polished narrative article.
5. Do not add sub-headings inside the transcript. Output clean prose only.

---

## Scripture References

Extract every scripture reference or quotation from the sermon. For each one:
- Identify the book, chapter, and verse(s)
- Provide the citation in standard NKJV format (e.g., John 3:16, Romans 8:28–30)
- Include the text as quoted or paraphrased by the speaker

Output as a numbered list:
1. [Citation] — [text as quoted or paraphrased in the sermon]

If no scriptures are found, say "No scripture references identified."

---

## Calls to Action

List every call to action the speaker makes — specific things the congregation is urged to do, believe, change, or commit to. Be direct and specific. Format as a bulleted list.

---

## Key Teaching Points

Identify the 5–7 most important points the speaker makes. For each point, write 1–3 sentences summarizing its content and why it matters to the sermon's overall message.

Format:
1. **[Point title]** — [clear summary of the point and its significance]
2. **[Point title]** — [summary]
...

## Thematic Summary

[One sentence capturing the sermon's central teaching or message.]"""


# ─────────────────────────────────────────────────
# Core Grok caller
# ─────────────────────────────────────────────────

def _grok_call(system_prompt, user_content):
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        temperature=0.3,
        max_tokens=MAX_TOKENS,
    )
    return resp.choices[0].message.content.strip()


def _parse_section(text, heading):
    """Extract content under a ## heading, up to the next ## heading or end of text."""
    import re
    pattern = rf"## {re.escape(heading)}\s*\n(.*?)(?=\n## |\Z)"
    m = re.search(pattern, text, re.DOTALL)
    return m.group(1).strip() if m else ""


# ─────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────

def process_transcript(transcript_path, slug, metadata=None, force=False):
    """
    Run the Grok prompt chain on a transcript file.
    Returns the processed markdown as a string on success, or None on failure.
    The caller (pipeline_grok.py) is responsible for uploading to Azure Blob.

    transcript_path: local path to a .txt file (caller writes temp file from Blob).
    force: accepted for backward compatibility; has no effect (skip logic removed).
    """
    try:
        with open(transcript_path, "r", encoding="utf-8") as f:
            raw_transcript = f.read().strip()
    except Exception as e:
        print(f"  Error reading transcript {transcript_path}: {e}")
        return None

    if not raw_transcript:
        print(f"  Warning: transcript is empty for {slug}")
        return None

    meta = metadata or {}
    title = meta.get("title", slug)
    speaker = meta.get("speaker", "Unknown")
    congregation = meta.get("congregation", "Unknown")
    date = meta.get("date", "")
    duration = meta.get("duration", "")
    page_url = meta.get("page_url", "")

    print(f"  Running single-call prompt on: {title}")

    try:
        print("    Calling Grok (single pass)...")
        result = _grok_call(_SYSTEM_PROMPT, f"RAW TRANSCRIPT:\n\n{raw_transcript}")
    except Exception as e:
        print(f"  Prompt call failed for {slug}: {e}")
        return None

    cleaned = _parse_section(result, "Cleaned Transcript")
    scriptures = _parse_section(result, "Scripture References")
    cta_content = _parse_section(result, "Calls to Action")
    thematic_content = _parse_section(result, "Thematic Summary")
    persuasive_content = _parse_section(result, "Key Teaching Points")

    # Wrap with headings
    thematic_section = f"## Thematic Summary\n\n{thematic_content}" if thematic_content else ""
    cta_section = f"## Calls to Action\n\n{cta_content}" if cta_content else ""
    persuasive_section = f"## Key Teaching Points\n\n{persuasive_content}" if persuasive_content else ""

    # Compose final markdown
    # Order: header → Thematic Summary → Calls to Action → --- → Persuasive Points → Scripture References → --- → Cleaned Transcript
    source_line = f"**Source:** [{page_url}]({page_url})" if page_url else ""
    md_content = f"""# {title}

**Speaker:** {speaker}
**Congregation:** {congregation}
**Date:** {date}
**Duration:** {duration}
{source_line}

---

{thematic_section}

{cta_section}

---

{persuasive_section}

## Scripture References

{scriptures}

---

## Cleaned Transcript

{cleaned}
"""

    print(f"  Processed markdown built: {slug}")
    return md_content
