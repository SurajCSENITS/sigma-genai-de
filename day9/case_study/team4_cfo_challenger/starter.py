import json
import os
import sys
from datetime import UTC, datetime

import duckdb
import pandas as pd
import streamlit as st

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))
from bedrock_helper import call_nova_lite, call_nova_pro


BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "..", "shared", "sigma_platform.duckdb")
VERDICT_PATH = os.path.join(BASE_DIR, "verdict.json")


st.set_page_config(page_title="CFO Challenger", layout="wide")
st.title("CFO Challenger")
st.caption("Sigma DataTech AI Ops Platform - Day 9")


@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


conn = get_connection()


def df(sql: str) -> pd.DataFrame:
    return conn.execute(sql).fetchdf()


def scalar(sql: str):
    return conn.execute(sql).fetchone()[0]


def money(value: float) -> str:
    return f"INR {value:,.2f}"


def pct(value: float) -> str:
    return f"{value:.2f}%"


def run_ai_call(fn, system: str, user: str, fallback: str, max_tokens: int = 1300) -> str:
    try:
        return fn(system, user, max_tokens=max_tokens)
    except Exception as exc:
        st.info(f"Using deterministic demo text because Bedrock was unavailable: {exc}")
        return fallback


def table_text(data: pd.DataFrame) -> str:
    return data.to_csv(index=False)


overview_sql = """
select
    round(sum(total_revenue), 2) as gold_revenue,
    sum(total_txns) as gold_txns,
    sum(unique_customers) as customer_day_count,
    round(sum(total_revenue) / nullif(sum(total_txns), 0), 2) as revenue_per_txn,
    round(sum(total_txns * failure_rate_pct / 100) / nullif(sum(total_txns), 0) * 100, 2) as failure_rate_pct
from gold_daily_summary
"""

merchant_sql = """
select
    merchant_name,
    category,
    city,
    round(total_revenue, 2) as total_revenue,
    txn_count,
    failure_rate_pct,
    round(total_revenue / nullif(txn_count, 0), 2) as revenue_per_txn
from gold_merchant_performance
order by total_revenue desc
"""

category_sql = """
select
    category,
    round(sum(total_revenue), 2) as total_revenue,
    sum(txn_count) as txn_count,
    round(sum(total_revenue) / (select sum(total_revenue) from gold_merchant_performance) * 100, 2) as revenue_share_pct,
    round(sum(txn_count * failure_rate_pct / 100) / nullif(sum(txn_count), 0) * 100, 2) as failure_rate_pct
from gold_merchant_performance
group by category
order by total_revenue desc
"""

daily_sql = """
select
    report_date,
    round(total_revenue, 2) as total_revenue,
    total_txns,
    unique_customers,
    unique_merchants,
    failure_rate_pct
from gold_daily_summary
order by report_date
"""

overview = df(overview_sql).iloc[0].to_dict()
merchants = df(merchant_sql)
categories = df(category_sql)
daily = df(daily_sql)


fallback_briefing = f"""
1. Gold-layer revenue for the CEO briefing is {money(overview["gold_revenue"])} across {int(overview["gold_txns"])} transactions, with average revenue per transaction of {money(overview["revenue_per_txn"])}.
2. E-Commerce is the biggest category at {money(float(categories.loc[categories["category"] == "E-Commerce", "total_revenue"].iloc[0]))}, representing {pct(float(categories.loc[categories["category"] == "E-Commerce", "revenue_share_pct"].iloc[0]))} of Gold revenue.
3. Flipkart is the top merchant with {money(float(merchants.iloc[0]["total_revenue"]))} from {int(merchants.iloc[0]["txn_count"])} transactions.
4. The strongest apparent growth signal is Flipkart moving from INR 1,450.00 on 2024-01-19 to INR 3,400.00 on 2024-01-25, a 134.48% increase.
5. Gold shows zero completed transactions with zero amount, so the CEO could treat recognized revenue as clean.
"""

briefing_prompt = f"""
You are writing a Monday morning one-page CEO revenue briefing for Sigma DataTech.
Use the supplied DuckDB Gold metrics. Write exactly five bullets with specific numbers.
Include one insight about trend or momentum, but do not mention that one claim may be challenged.

Gold overview:
{table_text(pd.DataFrame([overview]))}

Revenue by category:
{table_text(categories)}

Top merchants:
{table_text(merchants)}

Daily summary:
{table_text(daily)}
"""


claims = [
    {
        "id": "gold_total",
        "claim": "Gold recognized INR 13,161.00 revenue across 14 transactions, or INR 940.07 per transaction.",
        "status": "VERIFIED",
        "query": overview_sql,
        "verdict": "The aggregate is correct for the Gold layer. The calculation matches the daily summary totals.",
    },
    {
        "id": "flipkart_growth",
        "claim": "Flipkart shows strong momentum because revenue grew 134.48% from 2024-01-19 to 2024-01-25.",
        "status": "MISLEADING",
        "query": """
select
    merchant_name,
    transaction_date,
    count(*) as txn_count,
    round(sum(case when status = 'COMPLETED' then amount else 0 end), 2) as completed_revenue
from silver_transactions
where merchant_name = 'Flipkart'
group by merchant_name, transaction_date
order by transaction_date
""",
        "verdict": "The percentage growth is mathematically correct, but it is based on only two completed transactions on two isolated days. That is not enough evidence for a trend.",
    },
    {
        "id": "zero_value_quality",
        "claim": "Gold has no zero-value completed transactions, so recognized revenue is clean.",
        "status": "WRONG",
        "query": """
select
    transaction_id,
    amount,
    status,
    merchant_id,
    customer_id,
    transaction_date,
    payment_method
from bronze_transactions
where status = 'COMPLETED'
  and amount = 0
order by transaction_date
""",
        "verdict": "Gold/Silver hide the issue, but Bronze contains TXN019: a completed transaction for INR 0.00. It is invisible in revenue totals, yet it violates the business definition of a valid completed order.",
    },
]

fallback_challenges = """
1. Show me the data behind the Gold total and average. Are those numbers calculated from Gold daily totals or guessed from merchant rankings?
2. Show me every Flipkart transaction date used for the growth claim. Two points are not a trend.
3. Show me the raw Bronze rows for zero-value completed transactions. A Gold-only check can miss records filtered out upstream.
"""

cfo_prompt = f"""
You are Sigma DataTech's skeptical CFO.
Challenge exactly three specific claims from this CEO briefing.
For each challenge, ask "Show me the data." Keep the tone direct and finance-focused.

Briefing:
{fallback_briefing}
"""


st.subheader("Executive Snapshot")
metric_cols = st.columns(4)
metric_cols[0].metric("Gold Revenue", money(overview["gold_revenue"]))
metric_cols[1].metric("Gold Transactions", f"{int(overview['gold_txns'])}")
metric_cols[2].metric("Revenue / Txn", money(overview["revenue_per_txn"]))
metric_cols[3].metric("Failure Rate", pct(overview["failure_rate_pct"]))

chart_cols = st.columns([1.1, 1])
with chart_cols[0]:
    st.bar_chart(categories.set_index("category")["total_revenue"])
with chart_cols[1]:
    st.dataframe(merchants, hide_index=True, width="stretch")

st.divider()

round1, round2, round3, slide = st.tabs(
    ["Round 1: AI Briefing", "Round 2: CFO Challenge", "Round 3: Fact Check", "What AI Got Wrong"]
)

with round1:
    st.subheader("Nova Pro CEO Briefing")
    if st.button("Generate AI briefing", type="primary"):
        st.session_state["briefing"] = run_ai_call(
            call_nova_pro,
            "You are a concise executive briefing writer for a CEO.",
            briefing_prompt,
            fallback_briefing,
            max_tokens=1200,
        )
    st.markdown(st.session_state.get("briefing", fallback_briefing))
    with st.expander("Gold queries used for the briefing"):
        st.code(overview_sql, language="sql")
        st.code(category_sql, language="sql")
        st.code(merchant_sql, language="sql")
        st.code(daily_sql, language="sql")

with round2:
    st.subheader("Nova Lite CFO Challenge")
    if st.button("Generate CFO challenge"):
        st.session_state["challenge"] = run_ai_call(
            call_nova_lite,
            "You are a skeptical CFO who challenges AI-generated business claims.",
            cfo_prompt,
            fallback_challenges,
            max_tokens=900,
        )
    st.markdown(st.session_state.get("challenge", fallback_challenges))

with round3:
    st.subheader("DuckDB Fact Check")
    for item in claims:
        status_color = {
            "VERIFIED": "green",
            "MISLEADING": "orange",
            "WRONG": "red",
        }[item["status"]]
        st.markdown(f"#### :{status_color}[{item['status']}] {item['claim']}")
        result = df(item["query"])
        st.dataframe(result, hide_index=True, width="stretch")
        st.code(item["query"].strip(), language="sql")
        st.write(item["verdict"])

with slide:
    st.subheader("What AI Got Wrong")
    st.markdown(
        """
The most convincing bad insight is the Flipkart growth claim: revenue increased from
INR 1,450.00 to INR 3,400.00, so the 134.48% growth number is correct.

It is still statistically invalid. The claim uses two transactions on two days and
treats them as a trend. With this sample size, one large order can manufacture a
growth story that may disappear the next day.
"""
    )
    st.markdown(
        """
Additional data needed: at least several weeks of daily merchant revenue, order counts,
average order value, refund/cancellation rates, and whether the same customer or campaign
caused the spike.
"""
    )
    st.metric("Trust score for AI-generated business insight", "58%")
    st.write(
        "Reasoning: the AI can summarize Gold aggregates correctly, but it overstates trend confidence and misses upstream records that Gold/Silver filtered out."
    )

    verdict = {
        "module": "Team 4 - CFO Challenger",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "trust_score_pct": 58,
        "claims": [
            {
                "id": item["id"],
                "claim": item["claim"],
                "status": item["status"],
                "query": item["query"].strip(),
                "verdict": item["verdict"],
            }
            for item in claims
        ],
        "what_ai_got_wrong": {
            "claim": "Flipkart shows strong momentum because revenue grew 134.48% from 2024-01-19 to 2024-01-25.",
            "why_it_is_misleading": "The math is right, but two isolated completed transactions are not a statistically reliable trend.",
            "additional_data_needed": [
                "More daily revenue observations",
                "Order counts by day",
                "Average order value distribution",
                "Refund and cancellation rates",
                "Campaign or customer concentration context",
            ],
        },
        "bronze_quality_trap": {
            "transaction_id": "TXN019",
            "issue": "Completed transaction with zero amount is present in Bronze but absent from Gold/Silver revenue reporting.",
            "business_risk": "Aggregate revenue can look clean while an invalid completed order slips past the CEO briefing.",
        },
    }
    with open(VERDICT_PATH, "w") as f:
        json.dump(verdict, f, indent=2)
    st.success(f"Verdict saved to {VERDICT_PATH}")
    st.json(verdict)
