# -*- coding: utf-8 -*-
import os, json, time, requests
import pandas as pd
from pathlib import Path
from openevals.llm import create_llm_as_judge   # pip install openevals

# ========= Config =========
API_BASE    = os.getenv("DOCS_API", "http://localhost:5000")
DATASET     = os.getenv("EVAL_DATASET", "eval/rag_eval_questions.jsonl")
OUT_DIR     = os.getenv("EVAL_OUTDIR",  "eval/outputs")
JUDGE_MODEL = os.getenv("EVAL_JUDGE_MODEL", "openai:o3-mini")
POLL_TIMEOUT_S = int(os.getenv("EVAL_TIMEOUT", "120"))

# ========= Prompts de juez =========
HELPFULNESS_PROMPT = """
You are grading **Helpfulness** of a model answer with respect to the user's question.

Question:
{inputs}

Model answer:
{outputs}

Guidelines:
- Relevance to the question.
- Completeness and directness.
- Actionability/clarity (concise, unambiguous).
Ignore formatting and minor grammar.

Return strict JSON:
{{"score": <float between 0.0 and 1.0>, "reasoning": "<short reason>"}}
"""

GROUNDEDNESS_PROMPT = """
You are grading **Groundedness**: to what extent is the model answer supported by the retrieved context?

Question:
{inputs}

Retrieved context (may have multiple snippets):
\"\"\"{context}\"\"\"


Model answer:
{outputs}

Scoring:
- 1.0 = fully supported / cited in context
- 0.5 = partially supported / some speculation
- 0.0 = unsupported or contradicts the context

Return strict JSON:
{{"score": <float between 0.0 and 1.0>, "reasoning": "<short reason>"}}
"""

helpfulness  = create_llm_as_judge(prompt=HELPFULNESS_PROMPT,  model=JUDGE_MODEL)
groundedness = create_llm_as_judge(prompt=GROUNDEDNESS_PROMPT, model=JUDGE_MODEL)

# ========= Helpers =========
def post_question(q, session="eval"):
    r = requests.post(
        f"{API_BASE}/jobportal/querydoc",
        json={"sessionid": session, "searchquery": q, "context": "eval"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["reqid"]

def poll_full(reqid, timeout_s=POLL_TIMEOUT_S):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        r = requests.get(f"{API_BASE}/jobportal/answer_full",
                         params={"reqid": reqid}, timeout=15)
        if r.status_code == 200:
            return r.json()
        time.sleep(1.0)
    return {"status": "timeout"}

def iter_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def parse_eval(res, default=0.0):
    """Acepta tuple(score,reason) o dict con score/reasoning."""
    if isinstance(res, tuple):
        s = res[0] if len(res) > 0 else default
        try: s = float(s)
        except: s = default
        r = res[1] if len(res) > 1 else ""
        return s, r
    if isinstance(res, dict):
        s = res.get("score", res.get("value", default))
        try: s = float(s)
        except: s = default
        r = res.get("reasoning") or res.get("reason","")
        return s, r
    return default, ""

# ========= Main =========
def main():
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    rows = []
    for ex in iter_jsonl(DATASET):
        qid = ex.get("id")
        q   = ex.get("question","").strip()
        if not q:
            continue

        try:
            reqid = post_question(q)
            res   = poll_full(reqid)
        except Exception as e:
            rows.append({
                "id": qid, "question": q, "answer": f"<error:{e}>",
                "helpfulness": 0.0, "groundedness": 0.0,
                "h_reason": "request error", "g_reason": "request error"
            })
            continue

        if "answer" not in res:
            rows.append({
                "id": qid, "question": q, "answer": "<timeout/pending>",
                "helpfulness": 0.0, "groundedness": 0.0,
                "h_reason": "no answer", "g_reason": "no context"
            })
            continue

        answer = (res.get("answer") or "").strip()
        ctxs   = res.get("contexts", []) or []
        parts = []
        for c in ctxs[:5]:
            parts.append(c.get("text","") if isinstance(c, dict) else str(c))
        context_text = "\n---\n".join([p for p in parts if p])

        # --- Llamadas a los jueces ---
        h_res = helpfulness(inputs=q, outputs=answer)
        g_res = groundedness(inputs=q, outputs=answer, context=context_text)

        h_score, h_reason = parse_eval(h_res, 0.0)
        g_score, g_reason = parse_eval(g_res, 0.0)

        rows.append({
            "id": qid, "question": q, "answer": answer,
            "helpfulness": h_score, "groundedness": g_score,
            "h_reason": h_reason, "g_reason": g_reason
        })

    df = pd.DataFrame(rows)
    raw_path = f"{OUT_DIR}/help_ground_raw.csv"
    df.to_csv(raw_path, index=False)

    summary = {
        "n": int(len(df)),
        "helpfulness_mean": round(float(df["helpfulness"].mean()) if len(df) else 0.0, 3),
        "groundedness_mean": round(float(df["groundedness"].mean()) if len(df) else 0.0, 3),
    }
    with open(f"{OUT_DIR}/help_ground_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("✅ OK")
    print("• Detalle:", raw_path)
    print("• Resumen:", f"{OUT_DIR}/help_ground_summary.json")

if __name__ == "__main__":
    main()
