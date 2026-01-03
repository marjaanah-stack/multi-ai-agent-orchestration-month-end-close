import os
import pandas as pd
from typing import TypedDict
from threading import Thread
from fastapi import FastAPI
import uvicorn
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg_pool import ConnectionPool
import psycopg

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

def matchmaker_node(state: AgentState):
    print("🤖 Matchmaker is running...")
    bank_df = pd.DataFrame(state['bank_data'])
    erp_df = pd.DataFrame(state['erp_data'])

    current_matches = []
    current_unmatched = []

    for _, row in bank_df.iterrows():
        match = erp_df[erp_df['Amount'] == row['Amount']]

        if not match.empty:
            current_matches.append({"desc": row['Description'], "amount": row['Amount'], "status": "MATCHED"})
        else:
            current_unmatched.append({"desc": row['Description'], "amount": row['Amount']})

    return {
        "matches": current_matches, 
        "unmatched_items": current_unmatched
    }

def should_continue(state: AgentState):
    if len(state['unmatched_items']) > 0:
        return "human_review"
    return END

def human_review_node(state: AgentState):
    print("⏸️ PAUSED: Waiting for Maaja to review unmatched items in Slack...")
    return state

workflow = StateGraph(AgentState)
workflow.add_node("matchmaker", matchmaker_node)
workflow.add_node("human_review", human_review_node)
workflow.set_entry_point("matchmaker")
workflow.add_conditional_edges(
    "matchmaker",
    should_continue,
    {
        "human_review": "human_review",
        END: END
    }
)
workflow.add_edge("human_review", END)

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
            "unmatched_items": state.values.get("unmatched_items", [])
        }
    return {"status": "RUNNING_OR_COMPLETE"}

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
if __name__ == "__main__":
    # This line tells the server to use Port 5000 specifically
    if __name__ == "__main__":
        print("🚀 STARTING SERVER ON PORT 5000...")
        import uvicorn
        uvicorn.run(api, host="0.0.0.0", port=5000)