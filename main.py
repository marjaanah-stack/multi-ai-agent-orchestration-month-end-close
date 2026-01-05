import os
import pandas as pd
from typing import TypedDict
from threading import Thread
from fastapi import FastAPI, Request
import uvicorn
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg_pool import ConnectionPool
import psycopg
from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY"),
    base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
)

DB_URI = os.environ.get("DATABASE_URL")
pool = ConnectionPool(conninfo=DB_URI, max_size=20)
checkpointer = PostgresSaver(pool)

with psycopg.connect(DB_URI, autocommit=True) as setup_conn:
    PostgresSaver(setup_conn).setup()

class AgentState(TypedDict):
    bank_data: list
    erp_data: list
    matches: list
    unmatched_items: list
    ai_suggestion: str
    button_options: list
    user_choice: str
    audit_result: dict

def get_unmatched_from_db():
    with psycopg.connect(DB_URI) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT id, "Description", "Amount" FROM bank_statement WHERE status = \'UNMATCHED\'')
            return [{"id": row[0], "desc": row[1], "amount": row[2]} for row in cur.fetchall()]

def update_bank_statement_status(description, new_status):
    with psycopg.connect(DB_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE bank_statement SET status = %s WHERE "Description" = %s',
                (new_status, description)
            )
            conn.commit()
            print(f"📝 Updated bank_statement: '{description}' -> {new_status}")

def matchmaker_node(state: AgentState):
    print("🤖 Matchmaker is running...")
    
    unmatched_rows = get_unmatched_from_db()
    print(f"📋 Found {len(unmatched_rows)} UNMATCHED rows in bank_statement")

    if not unmatched_rows:
        print("🏁 All items have been processed!")
        return {"matches": [], "unmatched_items": []}
    
    first_item = unmatched_rows[0]
    print(f"➡️ Processing: '{first_item['desc']}' (${first_item['amount']})")
    
    return {
        "matches": [], 
        "unmatched_items": [first_item]
    }

def should_continue(state: AgentState):
    if len(state['unmatched_items']) > 0:
        return "human_review"
    return END

import requests

def send_to_slack(item, ai_suggestion, button_options):
    webhook_url = os.environ.get("N8N_WEBHOOK_URL")
    if not webhook_url:
        print("❌ N8N_WEBHOOK_URL not configured")
        return False
    
    payload = {
        "transaction": {
            "description": item.get("desc", "Unknown"),
            "amount": item.get("amount", 0)
        },
        "ai_suggestion": ai_suggestion,
        "button_options": button_options
    }
    
    print(f"📤 Attempting to send to: {webhook_url}")
    print(f"📦 Payload: {payload}")
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        print(f"📬 n8n Response Status: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SENT TO SLACK")
            return True
        else:
            print(f"❌ n8n Error Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Slack webhook error: {e}")
        return False

def human_review_node(state: AgentState):
    print("⏸️ PAUSED: Waiting for Maaja to review unmatched items in Slack...")
    return state

def get_categories_from_db():
    with psycopg.connect(DB_URI) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM categories ORDER BY name")
            return [row[0] for row in cur.fetchall()]

def investigator_node(state: AgentState):
    import json
    print("🔍 Investigator is analyzing unmatched items...")
    categories = get_categories_from_db()
    print(f"📋 Loaded {len(categories)} categories from database: {categories}")
    unmatched = state.get('unmatched_items', [])
    
    if not unmatched:
        return {"ai_suggestion": "No unmatched items to categorize.", "button_options": []}
    
    items_text = "\n".join([f"- {item['desc']}: ${item['amount']}" for item in unmatched])
    
    prompt = f"""You are a financial analyst. Given the following unmatched bank transactions, suggest the most appropriate category for each from this list: {categories}

Unmatched transactions:
{items_text}

You must respond with valid JSON in this exact format:
{{
    "reasoning": "Your detailed analysis explaining why you chose these categories",
    "top_categories": ["First Best Category", "Second Best Category"]
}}

The top_categories must contain exactly 2 category names from the provided list, ordered by how well they match the transaction. Choose the most relevant categories based on the transaction description."""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    
    content = response.choices[0].message.content
    print(f"💡 Raw AI Response: {content}")
    
    clean_content = content.strip()
    if clean_content.startswith("```json"):
        clean_content = clean_content[7:]
    if clean_content.startswith("```"):
        clean_content = clean_content[3:]
    if clean_content.endswith("```"):
        clean_content = clean_content[:-3]
    clean_content = clean_content.strip()
    
    try:
        parsed = json.loads(clean_content)
        ai_suggestion = parsed.get("reasoning", content)
        button_options = parsed.get("top_categories", categories[:2])
    except json.JSONDecodeError:
        ai_suggestion = content
        button_options = categories[:2]
    
    print(f"📝 AI Suggestion: {ai_suggestion}")
    print(f"🔘 Button Options: {button_options}")
    
    if unmatched:
        success = send_to_slack(unmatched[0], ai_suggestion, button_options)
        if not success:
            print("🚨 CRITICAL: Failed to send to Slack. Check n8n webhook configuration.")
    
    return {"ai_suggestion": ai_suggestion, "button_options": button_options}

def save_reconciled_transaction(desc, amount, category, status, audit_flags):
    with psycopg.connect(DB_URI) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reconciled_transactions (description, amount, category, status, audit_flags)
                VALUES (%s, %s, %s, %s, %s)
            """, (desc, amount, category, status, audit_flags))
            conn.commit()

def auditor_node(state: AgentState):
    print("🔎 Auditor is verifying the decision...")
    user_choice = state.get('user_choice', '')
    unmatched = state.get('unmatched_items', [])
    
    if not unmatched:
        print("✅ All items processed!")
        return {"audit_result": {"items": [], "total_processed": 0}, "unmatched_items": []}
    
    item = unmatched[0]
    remaining_unmatched = unmatched[1:]
    
    desc = item['desc']
    amount = float(item['amount'])
    flags = []
    status = 'RECONCILED'
    
    if abs(amount) > 5000:
        flags.append('MATERIALITY: Requires Secondary Sign-off (amount > $5,000)')
        status = 'PENDING_SECONDARY_SIGNOFF'
        print(f"⚠️ MATERIALITY FLAG: {desc} (${amount}) requires secondary sign-off")
    
    if user_choice == 'Interest Income' and amount < 0:
        flags.append('LOGIC ERROR: Interest Income cannot be negative')
        status = 'LOGIC_ERROR'
        print(f"❌ LOGIC ERROR: {desc} - Interest Income selected but amount is negative (${amount})")
    
    audit_flag_str = '; '.join(flags) if flags else None
    
    save_reconciled_transaction(desc, amount, user_choice, status, audit_flag_str)
    update_bank_statement_status(desc, status)
    if status == 'RECONCILED':
        print(f"✅ RECONCILED: {desc} -> {user_choice}")
    else:
        print(f"🚩 FLAGGED: {desc} -> {status}")
    
    audit_result = {
        "description": desc,
        "amount": amount,
        "category": user_choice,
        "status": status,
        "flags": flags
    }
    
    remaining_in_db = get_unmatched_from_db()
    
    if remaining_in_db:
        print(f"🔄 {len(remaining_in_db)} more UNMATCHED items in database. Looping back to Matchmaker...")
    else:
        print("🏁 All items have been processed!")
    
    return {
        "audit_result": {"item": audit_result, "remaining": len(remaining_in_db)},
        "unmatched_items": remaining_in_db,
        "user_choice": "",
        "ai_suggestion": "",
        "button_options": []
    }

def should_loop_back(state: AgentState):
    remaining = state.get('unmatched_items', [])
    if len(remaining) > 0:
        return "investigator"
    return END

workflow = StateGraph(AgentState)
workflow.add_node("matchmaker", matchmaker_node)
workflow.add_node("investigator", investigator_node)
workflow.add_node("human_review", human_review_node)
workflow.add_node("auditor", auditor_node)
workflow.set_entry_point("matchmaker")
workflow.add_conditional_edges(
    "matchmaker",
    should_continue,
    {
        "human_review": "investigator",
        END: END
    }
)
workflow.add_edge("investigator", "human_review")
workflow.add_edge("human_review", "auditor")
workflow.add_conditional_edges(
    "auditor",
    should_loop_back,
    {
        "investigator": "investigator",
        END: END
    }
)

app = workflow.compile(checkpointer=checkpointer, interrupt_before=["human_review"])
print("✅ Graph Compiled with 'Human-in-the-Loop' safety trigger.")

api = FastAPI()

@api.get("/")
def root():
    return {"message": "Financial Reconciliation API is running"}

@api.get("/check-status")
def check_status():
    config = {"configurable": {"thread_id": "DEC_2025_RECON"}}
    state = app.get_state(config)

    if state.next:
        return {
            "status": "PAUSED",
            "at_node": state.next,
            "unmatched_items": state.values.get("unmatched_items", []),
            "ai_suggestion": state.values.get("ai_suggestion", ""),
            "button_options": state.values.get("button_options", [])
        }
    return {"status": "RUNNING_OR_COMPLETE"}

@api.post("/resend-to-slack")
def resend_to_slack():
    config = {"configurable": {"thread_id": "DEC_2025_RECON"}}
    state = app.get_state(config)
    
    if not state.next:
        return {"error": "No pending review to resend"}
    
    unmatched = state.values.get("unmatched_items", [])
    ai_suggestion = state.values.get("ai_suggestion", "")
    button_options = state.values.get("button_options", [])
    
    if unmatched:
        success = send_to_slack(unmatched[0], ai_suggestion, button_options)
        return {"status": "SENT" if success else "FAILED", "transaction": unmatched[0]}
    
    return {"error": "No unmatched items found"}

@api.post("/run-reconciliation")
def run_reconciliation():
    bank_list = pd.read_csv('bank_statement.csv').to_dict('records')
    erp_list = pd.read_csv('erp_ledger.csv').to_dict('records')
    config = {"configurable": {"thread_id": "DEC_2025_RECON"}}
    initial_state = {"bank_data": bank_list, "erp_data": erp_list, "matches": [], "unmatched_items": []}
    
    results = []
    for event in app.stream(initial_state, config):
        results.append(list(event.keys())[0])
    
    state = app.get_state(config)
    if state.next:
        return {"status": "PAUSED", "at_node": state.next, "nodes_processed": results}
    return {"status": "COMPLETE", "nodes_processed": results}

@api.post("/submit-choice")
def submit_choice(category: str):
    config = {"configurable": {"thread_id": "DEC_2025_RECON"}}
    
    state = app.get_state(config)
    if not state.next:
        return {"error": "No pending review. The graph is not paused."}
    
    print(f"📥 Received user choice from n8n: {category}")
    
    app.update_state(config, {"user_choice": category})
    
    results = []
    for event in app.stream(None, config):
        results.append(list(event.keys())[0])
    
    final_state = app.get_state(config)
    audit_result = final_state.values.get("audit_result", {})
    
    return {
        "status": "COMPLETE",
        "nodes_processed": results,
        "audit_result": audit_result
    }

@api.post("/finalize-reconciliation")
async def finalize_reconciliation(request: Request):
    body = await request.json()
    print(f"📥 Received payload from n8n: {body}")
    
    status = body.get("status", "")
    
    if status == "RECONCILED":
        print("⚠️ CFO OVERRIDE DETECTED. Bypassing strict validation.")
        
        with psycopg.connect(DB_URI) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE reconciled_transactions 
                    SET status = 'RECONCILED', audit_flags = 'CFO Override'
                    WHERE id = 1
                """)
                if cur.rowcount == 0:
                    cur.execute("""
                        INSERT INTO reconciled_transactions (description, amount, category, status, audit_flags)
                        VALUES ('Transaction #1', 0, 'CFO Override', 'RECONCILED', 'CFO Override')
                    """)
                conn.commit()
        
        config = {"configurable": {"thread_id": "DEC_2025_RECON"}}
        app.update_state(config, {"user_choice": "RECONCILED"})
        
        results = []
        try:
            for event in app.stream(None, config):
                results.append(list(event.keys())[0])
        except Exception as e:
            print(f"Graph already complete or error: {e}")
        
        return {
            "status": "RECONCILED",
            "message": "CFO Override applied. Transaction #1 reconciled.",
            "nodes_processed": results
        }
    
    return {"status": "RECEIVED", "payload": body}

def run_initial_reconciliation():
    bank_list = pd.read_csv('bank_statement.csv').to_dict('records')
    erp_list = pd.read_csv('erp_ledger.csv').to_dict('records')
    config = {"configurable": {"thread_id": "DEC_2025_RECON"}}
    initial_state = {"bank_data": bank_list, "erp_data": erp_list, "matches": [], "unmatched_items": []}

    for event in app.stream(initial_state, config):
        print(f"--- Node Processed: {list(event.keys())[0]} ---")

    state = app.get_state(config)
    if state.next:
        print(f"🚨 SYSTEM ALERT: The graph has PAUSED at {state.next}. It is waiting for your approval.")

if __name__ == "__main__":
    Thread(target=run_initial_reconciliation, daemon=True).start()
    print("🚀 STARTING SERVER ON PORT 5000...")
    uvicorn.run(api, host="0.0.0.0", port=5000)