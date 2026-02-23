import boto3
import json
import sys
from collections import defaultdict
from strands import Agent, tool
from bedrock_agentcore.runtime import BedrockAgentCoreApp

# --- INITIALIZE CLIENTS ---
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')


# ─────────────────────────────────────────────
# HELPER: Fetch DynamoDB Schema
# ─────────────────────────────────────────────
def fetch_dynamodb_schema() -> str:
    table = dynamodb.Table("be_cig_metadata")
    try:
        response = table.scan()
        items = response.get('Items', [])

        print(f"[DEBUG] Found {len(items)} items in DynamoDB")
        if items:
            print(f"[DEBUG] First item keys: {list(items[0].keys())}")

        schema_map = defaultdict(lambda: {"description": "No description", "columns": []})
        
        for item in items:
            pk = str(item.get('PK') or item.get('pk') or '').strip()
            sk = str(item.get('SK') or item.get('sk') or '').strip()

            if not pk.startswith("TABLE#"):
                continue

            table_name = pk.split("TABLE#")[1]
            #print(f"Table Name: after split {table_name}")
            if sk == "METADATA":
                schema_map[table_name]['description'] = item.get('description', 'No description provided')
            elif sk.startswith("COLUMN#"):
                col_name = sk.split("COLUMN#")[1]
                #print(f"Column Name: after aplit {col_name}")
                schema_map[table_name]['columns'].append(col_name)
        #print(f"Scanned Map: after adding table and columns{schema_map}")
        output_lines = []
        for t_name, data in schema_map.items():
            output_lines.append(f"Table: {t_name}")
            output_lines.append(f"Description: {data['description']}")
            output_lines.append(f"Columns: {', '.join(data['columns'])}")
            output_lines.append("---")
        #print(f" output lines {output_lines}")
        formatted_schema = "\n".join(output_lines)
        #print(f"[DEBUG] Schema: {formatted_schema}")
        print(f"[DEBUG] Items found: {len(items)}")
        if formatted_schema:
            print(f"[DEBUG] Schema loaded successfully.")
        else:
            print("[DEBUG] Schema is empty.")

        return formatted_schema if formatted_schema else "No schema metadata found."

    except Exception as e:
        error_msg = f"DYNAMODB ERROR: {str(e)}"
        print(f"[ERROR] {error_msg}")
        return error_msg

'''

def filter_schema_by_tables(full_schema: str, table_names: list) -> str:
    """Returns only schema blocks for the given table names."""
    if not table_names:
        return full_schema

    filtered_lines = []
    blocks = full_schema.strip().split("---")
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        for table_name in table_names:
            if table_name.lower() in block.lower():
                filtered_lines.append(block)
                filtered_lines.append("---")
                break
    return "\n".join(filtered_lines) if filtered_lines else full_schema

'''
# ─────────────────────────────────────────────
# SUB-AGENTS
# ─────────────────────────────────────────────
_planner_agent = Agent(
    name="PlannerAgent",
    model="anthropic.claude-3-sonnet-20240229-v1:0",
    system_prompt="""
    You are a Database Planner Agent.
    You will be provided with a Database Schema describing available tables and a User Question.

    YOUR TASK:
    1. Identify which tables from the schema are required to answer the User Question.
    2. Return ONLY a valid JSON list containing the exact table names.
    3. NO conversational text. NO triple backticks. ONLY the JSON array.
    """
)

_sql_generator_agent = Agent(
    name="SQLGeneratorAgent",
    model="anthropic.claude-3-sonnet-20240229-v1:0",
    system_prompt="""
    You are an expert SQL Query Generator Agent.
    You will be provided with:
    - A filtered Database Schema (only the relevant tables)
    - The User's original question

    YOUR TASK:
    1. Generate a single, syntactically correct SQL SELECT query that answers the User Question.
    2. Use only the tables and columns provided in the schema.
    3. IMPORTANT: All table names MUST be prefixed with the schema name 'be_cig.' (e.g., be_cig.table_name).
    4. Use standard SQL syntax (compatible with Amazon Redshift / PostgreSql).
    5. Add table aliases for readability where appropriate.
    6. Return ONLY the raw SQL query. NO explanations, NO markdown, NO triple backticks    """
)


# ─────────────────────────────────────────────
# TOOLS (wrapping sub-agents for Orchestrator)
# ─────────────────────────────────────────────
@tool
def plan_tables(db_schema: str, user_question: str) -> str:
    """
    Given the full database schema and a user question, identifies and returns
    a JSON list of relevant table names needed to answer the question.

    Args:
        db_schema: The full database schema string with table names, descriptions, and columns.
        user_question: The natural language question from the user.

    Returns:
        A JSON array string of relevant table names, e.g. '["table_a", "table_b"]'.
    """
    #print(f"DB Schema: {db_schema}")  (all table,des,columns)
    print(">>> [Orchestrator] Calling PlannerAgent tool...")
    prompt = f"DATABASE SCHEMA:\n{db_schema}\n\nUSER QUESTION: {user_question}"
    response = _planner_agent(prompt)
    #print(f"Planner Agent Response: {response}")

    content = str(response).strip()
    for wrapper in ["```json", "```"]:
        if content.startswith(wrapper):
            content = content[len(wrapper):]
    if content.endswith("```"):
        content = content[:-3]
    return content.strip()


@tool
def generate_sql(db_schema: str, user_question: str) -> str:
    """
    Given a filtered database schema and a user question, generates and returns
    a valid SQL SELECT query that answers the question.

    Args:
        db_schema: A filtered schema string containing only the relevant tables and their columns.
        user_question: The natural language question from the user.

    Returns:
        A raw SQL SELECT query string (no markdown, no explanation).
    """
    print(">>> [Orchestrator] Calling SQLGeneratorAgent tool...")
    prompt = (
        f"RELEVANT DATABASE SCHEMA:\n{db_schema}\n\n"
        f"USER QUESTION: {user_question}\n\n"
        f"Generate the SQL query:"
    )
    response = _sql_generator_agent(prompt)
    
    content = str(response).strip()
    for wrapper in ["```sql", "```"]:
        if content.startswith(wrapper):
            content = content[len(wrapper):]
    if content.endswith("```"):
        content = content[:-3]
    return content.strip()


# ─────────────────────────────────────────────
# ORCHESTRATION AGENT
# ─────────────────────────────────────────────
orchestration_agent = Agent(
    name="OrchestratorAgent",
    model="anthropic.claude-3-sonnet-20240229-v1:0",
    system_prompt="""
    You are an Orchestration Agent responsible for answering user questions about a database
    by coordinating two specialized sub-agents:

    1. plan_tables  — Identifies which database tables are needed to answer a question.
    2. generate_sql — Generates a SQL query using the relevant tables and schema.

    YOUR WORKFLOW (always follow this order):
    Step 1: Call plan_tables(schema, user_question) to get the list of relevant table names.
    Step 2: Call generate_sql(filtered_schema, user_question) to generate the SQL query.
    Step 3: Return ONLY the final SQL query as your response. No extra explanation.
    """,
    tools=[plan_tables, generate_sql],
)


# ─────────────────────────────────────────────
# AGENTCORE ENTRYPOINT
# ─────────────────────────────────────────────
app = BedrockAgentCoreApp()

@app.entrypoint
def handler(payload):
    """
    Entrypoint for Bedrock AgentCore.
    The Orchestration Agent manages the full pipeline:
      DynamoDB Schema → plan_tables → filter schema → generate_sql → return SQL
    """
    # 1. Extract user question
    user_input = payload.get("prompt", "").strip()
    if not user_input:
        user_input = " "

    print(f"\n>>> Incoming User Input: {user_input}")

    # 2. Fetch schema from DynamoDB
    schema_info = fetch_dynamodb_schema()
    print(f">>> Schema fetched. Length: {len(schema_info)} chars")
    #print(f"Schema:{schema_info}")
    # --- DEBUG MODE: skip agents and return raw schema ---
    if str(payload.get("debug", "")).lower() == "true":
        print(">>> DEBUG MODE ACTIVE: Returning raw schema.")
        return {"response": f"DEBUG RAW SCHEMA:\n{schema_info}"}
    # -----------------------------------------------------

    # 3. Orchestration Agent runs the full pipeline
    print(">>> Orchestration Agent starting pipeline...")
    orchestrator_prompt = (
        f"DATABASE SCHEMA:\n{schema_info}\n\n"
        f"USER QUESTION: {user_input}\n\n"
        f"Use your tools to identify the relevant tables, filter the schema, "
        f"and produce the final SQL query."
    )

    orchestrator_response = orchestration_agent(orchestrator_prompt)
    #print(f">>> Orchestration Agent Response: {orchestrator_response}")

    # 4. Clean and return SQL output
    sql_content = str(orchestrator_response).strip()
    for wrapper in ["```sql", "```"]:
        if sql_content.startswith(wrapper):
            sql_content = sql_content[len(wrapper):]
    if sql_content.endswith("```"):
        sql_content = sql_content[:-3]
    sql_content = sql_content.strip()

    #print(f">>> Final SQL:\n{sql_content}")

    return {
        "response": sql_content,
    }


if __name__ == "__main__":
    if "--local" in sys.argv:
        user_query = " ".join([a for a in sys.argv[1:] if a != "--local"])
        if not user_query:
            user_query = ""

        print("\n--- RUNNING LOCAL TEST ---")
        print(f"User Query: {user_query}")
        result = handler({"prompt": user_query})
        print("\n[FINAL RESULT]")
        print(result.get("response", ""))
        print("--- TEST COMPLETE ---")
    else:
        app.run()
