import os, json, time, requests, statistics
import pandas as pd
from pathlib import Path
from openevals.llm import create_llm_as_judge

API = os.getenv("DOCS_API", "http://localhost:5000")
DATASET = "eval/data/questions.jsonl"   # ← usa el que creaste
OUT_DIR = "eval/outputs"

# ---------- Prompts ----------
CORRECTNESS_PROMPT = """
You are grading **Correctness** of an answer against a ground-truth.
Question: {inputs}
Ground truth answer: {reference}
Model answer: {outputs}

Score from 0.0 to 1.0 where 1.0 = fully correct.
Return JSON: {{"score": <float>, "reasoning": "<short reason>"}}.
"""

GROUNDEDNESS_PROMPT = """
You are grading **Groundedness**: does the answer align with the retrieved context?
Question: {inputs}
Retrieved context:
\"\"\"{context}\"\"\"
Model answer: {outputs}

Score from 0.0 to 1.0 where 1.0 = fully supported by the context.
Return JSON: {{"score": <float>, "reasoning": "<short reason>"}}.
"""

correctness  = create_llm_as_judge(prompt=CORRECTNESS_PROMPT,  model="openai:o3-mini")
groundedness = create_llm_as_judge(prompt=GROUNDEDNESS_PROMPT, model="openai:o3-mini")

def post_question(q, session="eval"):
    r = requests.post(f"{API}/jobportal/querydoc",
                      json={"sessionid": session, "searchquery": q, "context": "eval"},
                      timeout=30)
    r.raise_for_status()
    return r.json()["reqid"]

def poll_full(reqid, timeout_s=90):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        r = requests.get(f"{API}/jobportal/answer_full", params={"reqid": reqid}, timeout=15)
        if r.status_code == 200:
            return r.json()
        time.sleep(1.2)
    return {"status":"timeout"}

def load_dataset(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def agg_mean(xs):
    xs = [x for x in xs if isinstance(x,(int,float))]
    return round(sum(xs)/len(xs), 3) if xs else None

def parse_eval(res, default=0.0):
    if isinstance(res, tuple):
        s = res[0] if len(res)>0 else default
        try: s=float(s)
        except: s=default
        r = res[1] if len(res)>1 else ""
        return s, r
    if isinstance(res, dict):
        s = res.get("score", res.get("value", default))
        try: s=float(s)
        except: s=default
        r = res.get("reasoning") or res.get("reason","")
        return s, r
    return default, ""

Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

rows = []
for ex in load_dataset(DATASET):
    qid, qtxt = ex["id"], ex["question"]
    ref = ex.get("reference_answer","")

    reqid = post_question(qtxt)
    res = poll_full(reqid)

    if "answer" not in res:
        rows.append({"id":qid,"question":qtxt,"ref":ref,"answer":"<timeout>","correctness":0.0,"groundedness":0.0})
        continue

    answer = res["answer"]
    ctxs = res.get("contexts",[])
    context_text = "\n---\n".join([ (c.get("text","") if isinstance(c,dict) else str(c)) for c in ctxs[:4]])

    c_res = correctness(inputs=qtxt, outputs=answer, reference=ref)
    g_res = groundedness(inputs=qtxt, outputs=answer, context=context_text)

    c_score, c_reason = parse_eval(c_res, 0.0)
    g_score, g_reason = parse_eval(g_res, 0.0)

    rows.append({
        "id": qid,
        "question": qtxt,
        "ref": ref,
        "answer": answer,
        "correctness": c_score,
        "groundedness": g_score,
        "c_reason": c_reason,
        "g_reason": g_reason
    })

df = pd.DataFrame(rows)
df.to_csv(f"{OUT_DIR}/openevals_raw.csv", index=False)

summary = {
    "n": len(df),
    "correctness_mean": agg_mean(df["correctness"].tolist()),
    "groundedness_mean": agg_mean(df["groundedness"].tolist()),
}
with open(f"{OUT_DIR}/openevals_summary.json","w",encoding="utf-8") as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)

print("✅ Listo: eval/outputs/openevals_raw.csv y openevals_summary.json")
