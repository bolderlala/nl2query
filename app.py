"""
NL2Query — Natural Language to Database Query Translator
MSBA Class 8 Demo: Same data, six database languages, live execution.

Usage:  streamlit run app.py
"""

import streamlit as st
import streamlit.components.v1 as components
import anthropic
import json
import os
from databases import init_all, SQLDatabase, ColumnFamilyDatabase, DocumentDatabase, KeyValueDatabase, GraphDatabase, VectorDatabase
from seed_data import SAMPLE_QUESTIONS, STUDENTS, COURSES, ENROLLMENTS

st.set_page_config(page_title="NL2Query — MSBA Demo", page_icon="🗄️", layout="wide")

# ── Load API key ─────────────────────────────────────────────────────────────

from pathlib import Path

def _get_secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return None


def load_api_key():
    val = _get_secret("ANTHROPIC_API_KEY")
    if val:
        return val
    if os.environ.get("ANTHROPIC_API_KEY"):
        return os.environ["ANTHROPIC_API_KEY"]
    for loc in ["anthropic_api_key.txt", os.path.join("..", "anthropic_api_key.txt")]:
        path = os.path.join(os.path.dirname(__file__), loc)
        if os.path.exists(path):
            return open(path).read().strip()
    return ""


def check_password():
    correct = _get_secret("CLASS_PASSWORD")
    if not correct:
        return True

    if st.session_state.get("authenticated"):
        return True

    st.title("🗄️ NL2Query")
    st.markdown("**MSBA Class 8 Demo** — enter the class password to continue.")
    pwd = st.text_input("Class password:", type="password")
    if pwd and pwd == correct:
        st.session_state["authenticated"] = True
        st.rerun()
    elif pwd:
        st.error("Incorrect password. Check Canvas for the class password.")
    return False

# ── Initialize databases (cached across reruns) ─────────────────────────────

@st.cache_resource
def get_databases():
    return init_all()

DB_ORDER = ["sql", "column", "document", "kv", "graph", "vector"]
DB_CLASSES = {
    "sql": SQLDatabase, "column": ColumnFamilyDatabase, "document": DocumentDatabase,
    "kv": KeyValueDatabase, "graph": GraphDatabase, "vector": VectorDatabase,
}

# ── System prompt ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a database query translator for a graduate analytics course.
You translate natural language into executable queries for a specific database engine.

CRITICAL RULES:
1. Output ONLY the executable query. No explanation, no markdown fences, no backticks, no commentary.
2. Use ONLY the exact table/column/field names from the schema provided.
3. The query must be directly executable against the schema — do not invent fields.

Database-specific syntax rules:
- Relational Database (SQLite): Standard SQL. Tables: students, courses, enrollments. Use JOINs when needed.
- Wide Column Store (Cassandra): CQL syntax (SQL-like but NO JOINs). Tables: students (PK: student_id), courses_by_department (PK: department), enrollments_by_student (PK: student_id), enrollments_by_course (PK: course_id). Each table is denormalized — pick the right table for the query. WHERE must filter on partition key. Use SELECT ... FROM table WHERE partition_key = value. Supports AND, IN, ORDER BY, LIMIT. IMPORTANT: Only filter on columns that exist in the target table. Each table has different columns — check the schema. If a query needs data from multiple tables, write separate SELECT statements (one per table). Majors are 'Business Analytics' and 'Data Science'. Departments are 'MSBA' and 'Darden'. If a question asks about topics or interests, search by the closest matching major, not by the topic name itself.
- Document Database (MontyDB): Output a Python expression starting with db["collection"].find({filter}). Use MongoDB operators ($gt, $gte, $lt, $lte, $eq, $ne, $in, $regex, $and, $or, $elemMatch). For sorting add .sort("field", 1 or -1). IMPORTANT: MontyDB does NOT support aggregate(), projection operators ($), or dot-notation in sort paths. Only use find() with a filter dict and optional .sort("top_level_field", direction). For queries about nested enrollment data, use $elemMatch to filter and return the full student documents. When filtering the same field multiple times (e.g., two $elemMatch on "enrollments"), you MUST use $and: [{"enrollments": {"$elemMatch": ...}}, {"enrollments": {"$elemMatch": ...}}]. Never repeat the same key in a Python dict literal.
- Key-Value Store (Redis): Output one Redis command per line. Available commands: GET, HGETALL, HGET, SMEMBERS, KEYS, ZRANGEBYSCORE, ZRANGE, ZREVRANGE. Keys follow patterns: student:{id}, course:{id}, enrollment:{sid}:{cid}, student:{id}:courses, course:{id}:students, scores:{course_id}. IMPORTANT: KEYS only returns key names, not data. To show actual data, use HGETALL for each relevant key. For questions about all students, output HGETALL student:1 through HGETALL student:8 (one per line). For questions about specific courses or scores, use the appropriate key with HGETALL or ZRANGEBYSCORE. Always return the data, not just key names.
- Graph Database (Kuzu): MATCH/WHERE/RETURN Cypher syntax. Node labels: Student, Course. Relationship: ENROLLED_IN. Properties on nodes use exact field names from schema. Always end with semicolon.
- Vector Database (ChromaDB): Output a Python expression: client.get_collection("collection_name").query(...). Collections: student_profiles (has student bios), course_catalog (has course descriptions). Use query_texts for semantic search, n_results for limit. IMPORTANT: Vector search finds by meaning, not exact values. Always rely on query_texts for the main search. Only add a where filter for simple exact metadata matches (e.g., major or department). Never filter on scores, GPA ranges, or numeric comparisons in where — those don't work well with vector search. When in doubt, omit the where clause and let semantic search do the work."""


def _show_kv_result(result):
    if isinstance(result, (dict, list)):
        st.json(result)
    else:
        st.code(str(result))


def _render_graph_interactive():
    nodes_js = []
    spacing_y = 65
    for i, s in enumerate(STUDENTS):
        y = i * spacing_y
        nodes_js.append(
            f'{{id: "s{s["id"]}", label: "{s["name"]}\\n{s["major"]} | GPA {s["gpa"]}", '
            f'x: -300, y: {y}, fixed: false, '
            f'group: "student", title: "Student {s["id"]}\\nEmail: {s["email"]}\\nYear: {s["year"]}"}}'
        )
    course_ys = [65, 155, 245, 335, 425]
    for i, c in enumerate(COURSES):
        nodes_js.append(
            f'{{id: "c{c["id"]}", label: "{c["name"]}\\n{c["department"]} | {c["instructor"]}", '
            f'x: 300, y: {course_ys[i]}, fixed: false, '
            f'group: "course", title: "Course {c["id"]}\\nCredits: {c["credits"]}"}}'
        )
    edges_js = []
    for e in ENROLLMENTS:
        edges_js.append(
            f'{{from: "s{e["student_id"]}", to: "c{e["course_id"]}", '
            f'label: "{e["score"]}", title: "Score: {e["score"]}\\nSemester: {e["semester"]}"}}'
        )
    html = f"""
    <html>
    <head>
      <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
      <style>
        #graph {{ width: 100%; height: 520px; border: 1px solid #e0e0e0; border-radius: 8px; background: #fafafa; }}
      </style>
    </head>
    <body>
      <div id="graph"></div>
      <script>
        var nodes = new vis.DataSet([{', '.join(nodes_js)}]);
        var edges = new vis.DataSet([{', '.join(edges_js)}]);
        var container = document.getElementById('graph');
        var data = {{ nodes: nodes, edges: edges }};
        var options = {{
          groups: {{
            student: {{
              shape: 'box',
              color: {{ background: '#B3D9F2', border: '#5BA3D9', highlight: {{ background: '#89C4F4', border: '#3498DB' }} }},
              font: {{ size: 11, face: 'Helvetica' }},
              borderWidth: 2,
              borderWidthSelected: 3
            }},
            course: {{
              shape: 'box',
              color: {{ background: '#B3E6B3', border: '#5BAF5B', highlight: {{ background: '#82D882', border: '#27AE60' }} }},
              font: {{ size: 11, face: 'Helvetica' }},
              borderWidth: 2,
              borderWidthSelected: 3
            }}
          }},
          edges: {{
            arrows: {{ to: {{ enabled: true, scaleFactor: 0.5 }} }},
            color: {{ color: '#aaa', highlight: '#E74C3C' }},
            font: {{ size: 9, color: '#666', strokeWidth: 2, strokeColor: '#fff' }},
            smooth: {{ type: 'cubicBezier', forceDirection: 'horizontal', roundness: 0.4 }}
          }},
          physics: {{ enabled: false }},
          interaction: {{
            hover: true,
            tooltipDelay: 100,
            dragNodes: true,
            dragView: true,
            zoomView: true
          }}
        }};
        var network = new vis.Network(container, data, options);
        network.fit({{ padding: 40 }});
      </script>
    </body>
    </html>
    """
    components.html(html, height=540)


def _render_schema(key, db):
    if key == "sql":
        st.markdown("""
**3 normalized tables linked by foreign keys:**

| `students` | | `enrollments` | | `courses` |
|:---:|:---:|:---:|:---:|:---:|
| 🔑 student_id | | student_id (FK) → | | 🔑 course_id |
| name | | course_id (FK) → | | name |
| email | | score | | department |
| major | | semester | | credits |
| gpa | | | | instructor |
| year | | | | description |
| bio | | | | |
""")
    elif key == "column":
        st.markdown("""
**4 denormalized tables — each designed for one query pattern (no JOINs):**
""")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("""
`students` — *lookup by student*
| Column | Type |
|--------|------|
| 🔑 **student_id** | INT (partition key) |
| name, email | TEXT |
| major | TEXT |
| gpa | FLOAT |
| year | INT |
| bio | TEXT |

`enrollments_by_student` — *"what courses does student X take?"*
| Column | Type |
|--------|------|
| 🔑 **student_id** | INT (partition key) |
| course_id | INT (clustering key) |
| student_name, course_name | TEXT |
| score | INT |
| semester | TEXT |
""")
        with c2:
            st.markdown("""
`courses_by_department` — *lookup by department*
| Column | Type |
|--------|------|
| 🔑 **department** | TEXT (partition key) |
| course_id | INT (clustering key) |
| name | TEXT |
| credits | INT |
| instructor | TEXT |
| description | TEXT |

`enrollments_by_course` — *"who is enrolled in course Y?"*
| Column | Type |
|--------|------|
| 🔑 **course_id** | INT (partition key) |
| student_id | INT (clustering key) |
| student_name, course_name | TEXT |
| score | INT |
| semester | TEXT |
""")
        st.markdown("*Course IDs: 101=Managing Big Data, 102=Predictive Analytics, 103=Digital Transformation with AI, 104=Marketing Analytics, 105=Deep Learning & NLP*")
    elif key == "document":
        st.markdown("""
**2 collections — enrollments nested inside student documents:**

```json
// Collection: students
{
  "_id": 1,
  "name": "Alice Chen",
  "email": "ac1@virginia.edu",
  "major": "Business Analytics",
  "gpa": 3.9,
  "year": 2026,
  "bio": "Passionate about machine learning and...",
  "enrollments": [          ← nested array
    { "course_id": 101, "course_name": "Managing Big Data",
      "score": 92, "semester": "Fall 2025" },
    ...
  ]
}

// Collection: courses
{ "_id": 101, "name": "Managing Big Data", "department": "MSBA",
  "credits": 3, "instructor": "Prof. Li",
  "description": "Data lakes, cloud infrastructure, and..." }
```
""")
    elif key == "kv":
        st.markdown("""
**Flat key-value pairs with different data structures:**

| Key Pattern | Type | Example Value |
|-------------|------|--------------|
| `student:{id}` | Hash | `{name: "Alice Chen", gpa: "3.9", bio: "...", ...}` |
| `course:{id}` | Hash | `{name: "Managing Big Data", description: "...", ...}` |
| `enrollment:{sid}:{cid}` | Hash | `{score: "92", semester: "Fall 2025"}` |
| `student:{id}:courses` | Set | `{"101", "102", "105"}` |
| `course:{id}:students` | Set | `{"1", "2", "4", ...}` |
| `scores:{course_id}` | Sorted Set | members ranked by score |
""")
    elif key == "graph":
        st.markdown("""
**Nodes and relationships:**

```
(Student)──── ENROLLED_IN {score, semester} ────>(Course)
```

| Student Node | | Course Node |
|:---:|:---:|:---:|
| student_id | | course_id |
| name | → ENROLLED_IN → | name |
| email, major | score, semester | department |
| gpa, year | | credits, instructor |
| bio | | course_desc |
""")
        _render_graph_interactive()
    elif key == "vector":
        st.markdown("""
**2 collections — text auto-embedded as vectors:**

| Collection | Document (embedded as vector) | Metadata |
|------------|-------------------------------|----------|
| `student_profiles` | student bio text | name, major, gpa, year |
| `course_catalog` | course description text | name, department, instructor |

*Queries find semantically similar items — "machine learning" matches bios about ML even without exact keywords.*
""")
        try:
            sp = db.client.get_collection("student_profiles").get(
                ids=["student_1", "student_2"], include=["documents", "embeddings"]
            )
            cc = db.client.get_collection("course_catalog").get(
                ids=["course_101"], include=["documents", "embeddings"]
            )
            st.markdown("**Sample embeddings** (each document → a high-dimensional vector):")
            for i, sid in enumerate(sp["ids"]):
                emb = sp["embeddings"][i]
                dim = len(emb)
                preview = ", ".join(f"{v:.4f}" for v in emb[:6])
                st.code(
                    f'"{sp["documents"][i]}"\n'
                    f"  → [{preview}, ... ] ({dim} dimensions)",
                    language="text",
                )
            if cc["ids"]:
                emb = cc["embeddings"][0]
                dim = len(emb)
                preview = ", ".join(f"{v:.4f}" for v in emb[:6])
                st.code(
                    f'"{cc["documents"][0]}"\n'
                    f"  → [{preview}, ... ] ({dim} dimensions)",
                    language="text",
                )
        except Exception:
            pass
    else:
        st.code(db.get_schema(), language="text")


DB_TEACHING_NOTES = {
    "sql": "**Relational databases** excel at filtering on any column (`WHERE gpa > 3.7`), joining tables, and aggregations. This flexibility comes from the structured schema and query optimizer.",
    "column": "**Wide column stores** can only efficiently filter on the **partition key**. Other filters require scanning all partitions (ALLOW FILTERING), which is slow at scale. That's why we denormalize data into multiple tables — each designed for one query pattern.",
    "document": "**Document databases** can filter on any field, including nested arrays. But without JOINs, related data must be embedded (nested) inside documents or looked up separately.",
    "kv": "**Key-value stores** only support lookups by **exact key** — there's no way to say \"find all students where GPA > 3.7\". To answer that, you must fetch every student and filter client-side. This is the trade-off for ultra-fast O(1) key lookups.",
    "graph": "**Graph databases** excel at traversing relationships (\"students enrolled in the same courses\") but are not optimized for bulk attribute filtering like \"GPA > 3.7\" across all nodes.",
    "vector": "**Vector databases** search by **semantic similarity**, not exact values. A query like \"GPA above 3.7\" gets matched to student bios about academic achievement — the results may *relate* to the topic but won't give exact numeric filtering. Use relational or document databases for precise attribute queries.",
}

NO_RESULTS_HINTS = {
    "sql": "Check that table/column names match the schema exactly (students, courses, enrollments). String values need single quotes (e.g., WHERE name = 'Alice Chen').",
    "column": "CQL requires filtering on the partition key. Make sure you're querying the right table for your question (e.g., enrollments_by_student for student lookups, courses_by_department for department lookups). No JOINs allowed.",
    "document": "Check that field names match the schema. Use MongoDB operators like $gte, $lte, $elemMatch for nested arrays. Collection names: 'students', 'courses'.",
    "kv": "Keys follow the pattern student:{id}, course:{id}, scores:{course_id}. Use numeric IDs, not names. Check available commands: HGETALL, SMEMBERS, KEYS, ZRANGEBYSCORE.",
    "graph": "Node labels are Student and Course (capitalized). Relationship is ENROLLED_IN. Property names must match the schema exactly. End queries with semicolon.",
    "vector": "Vector search works on meaning, not exact matches. Collections: student_profiles (bios), course_catalog (descriptions). Use query_texts for semantic search.",
}


def translate(client, question, db):
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Database type: {db.name}\n\nSchema:\n{db.get_schema()}\n\nQuestion: {question}\n\nQuery:",
        }],
    )
    return response.content[0].text.strip()


# ── Password gate ───────────────────────────────────────────────────────────

if not check_password():
    st.stop()

# ── Sidebar ──────────────────────────────────────────────────────────────────

api_key = load_api_key()

st.sidebar.markdown("### 📚 The Six Data Models")
st.sidebar.markdown("""
| Data Model | Best For |
|------------|----------|
| 📊 **Relational** (SQLite) | Structured data, joins, aggregations |
| 📋 **Wide Column** (Cassandra) | Denormalized, partition-key queries |
| 📄 **Document** (MontyDB) | Flexible schemas, nested data |
| ⚡ **Key-Value** (Redis) | Fast lookups by key |
| 🕸️ **Graph** (Kuzu) | Relationships, traversals |
| 🧭 **Vector** (ChromaDB) | Semantic similarity search |
""")

st.sidebar.divider()
st.sidebar.markdown("### 🔗 The Real Software")
st.sidebar.markdown("""
| Demo | Production Software |
|------|-------------------|
| SQLite | [PostgreSQL](https://www.postgresql.org/) / [MySQL](https://www.mysql.com/) |
| Cassandra | [Apache Cassandra](https://cassandra.apache.org/) / [ScyllaDB](https://www.scylladb.com/) |
| MontyDB | [MongoDB](https://www.mongodb.com/) |
| Redis | [Redis](https://redis.io/) / [Valkey](https://valkey.io/) |
| Kuzu | [Neo4j](https://neo4j.com/) / [Kuzu](https://kuzudb.com/) |
| ChromaDB | [Pinecone](https://www.pinecone.io/) / [Weaviate](https://weaviate.io/) |
""")
st.sidebar.divider()
st.sidebar.markdown("### 💡 Teaching Point")
st.sidebar.markdown(
    "*Same 8 students, 5 courses, 22 enrollments — "
    "stored and queried six completely different ways. "
    "The data model shapes the query language.*"
)

# ── Main ─────────────────────────────────────────────────────────────────────

st.title("🗄️ NL2Query")
st.markdown("**Same data. Six databases. Live queries.**  ·  MSBA Class 8 Demo")

with st.expander("📖 How to Use This Demo"):
    st.markdown("""
### What This App Does

This app translates a **plain-English question** into executable queries for **six different database types** — and runs them live against real embedded databases loaded with the same dataset.

### Steps

1. **Type a question** in the text box below (or pick a sample from the dropdown)
2. **Claude AI** translates your question into six query languages simultaneously
3. **Click "Run"** on any tab to execute the query against a real database
4. **Click "Run All"** at the bottom to compare results side by side

### The Six Data Models

| | Data Model | Query Language | Best For |
|---|------------|---------------|----------|
| 📊 | **Relational** (SQLite) | SQL with JOINs | Structured data, aggregations |
| 📋 | **Wide Column** (Cassandra) | CQL — no JOINs, partition keys | High-volume, denormalized reads |
| 📄 | **Document** (MontyDB) | MongoDB-style find() | Flexible schemas, nested data |
| ⚡ | **Key-Value** (Redis) | GET/SET/HGETALL commands | Ultra-fast lookups by key |
| 🕸️ | **Graph** (Kuzu) | Cypher pattern matching | Relationships, traversals |
| 🧭 | **Vector** (ChromaDB) | Semantic similarity search | Finding by meaning, not keywords |

### Sample Questions to Try

- *Find all students with a GPA above 3.7*
- *Which students scored above 90 in Predictive Analytics?*
- *Show all courses taught by Prof. Mousavi*
- *Find students enrolled in both Managing Big Data and Predictive Analytics*
- *What is the average score for each course?*
- *Who got the highest score in Deep Learning & NLP?*
- *List all Business Analytics majors and their courses*
- *Find students whose interests relate to machine learning*

### Exploring the Schemas

Click **"View the dataset & schemas for all 6 databases"** below the question box to see:
- **The Data** tab — the raw dataset (8 students, 5 courses, 22 enrollments)
- **Each database tab** — how that data model stores the same data, with visual schema diagrams and example queries

When you run a query, each result tab also has a **schema expander** showing the relevant schema for that database.

### Key Takeaway

**The same question produces very different syntax depending on the data model.**
Relational databases use JOINs. Document databases use nested JSON filters. Key-value stores use direct key lookups.
Graph databases match relationship patterns. Vector databases search by semantic similarity.
The data model determines the query language — and understanding this is what makes you a polyglot data professional.
""")

st.divider()

# Input
col_q, col_s = st.columns([3, 1])
with col_q:
    user_q = st.text_input("Ask a question about the student data:",
                           placeholder="e.g., Find all students with a GPA above 3.7")
with col_s:
    st.markdown("**Try a sample:**")
    sample = st.selectbox("Samples", [""] + SAMPLE_QUESTIONS, label_visibility="collapsed")

question = user_q or sample

# Data & schema viewer
with st.expander("📋 View the dataset & schemas for all 6 databases"):
    dbs = get_databases()
    data_tab, *schema_tabs = st.tabs(["📊 The Data"] + [f"{DB_CLASSES[k].icon} {DB_CLASSES[k].name}" for k in DB_ORDER])
    with data_tab:
        st.markdown("**8 students, 5 courses, 22 enrollments — the same data in every database.**")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Students**")
            st.dataframe([{"ID": s["id"], "Name": s["name"], "Major": s["major"], "GPA": s["gpa"], "Year": s["year"], "Bio": s["bio"]} for s in STUDENTS], use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**Courses**")
            st.dataframe([{"ID": c["id"], "Name": c["name"], "Dept": c["department"], "Instructor": c["instructor"], "Description": c["description"]} for c in COURSES], use_container_width=True, hide_index=True)
        st.markdown("**Enrollments**")
        st.dataframe([{"Student": e["student_id"], "Course": e["course_id"], "Score": e["score"], "Semester": e["semester"]} for e in ENROLLMENTS], use_container_width=True, hide_index=True)
    for tab, key in zip(schema_tabs, DB_ORDER):
        db = dbs[key]
        with tab:
            st.markdown(f"**{db.description}**")
            _render_schema(key, db)
            st.markdown("**Example query:**")
            st.code(db.example_query(), language=db.lang)

st.divider()

# ── Translation & Execution ─────────────────────────────────────────────────

if not question:
    st.markdown("### 👋 Type a question above to see it translated into 6 query languages and executed live.")
    for row_keys in [DB_ORDER[:3], DB_ORDER[3:]]:
        cols = st.columns(3)
        for col, key in zip(cols, row_keys):
            cls = DB_CLASSES[key]
            with col:
                st.markdown(f"### {cls.icon}")
                st.markdown(f"**{cls.name}**")
                st.caption(cls.description)

elif not api_key:
    st.error("**API key not configured.** If you're running locally, place an `anthropic_api_key.txt` file in the `nl2query/` folder.")

else:
    client = anthropic.Anthropic(api_key=api_key)
    dbs = get_databases()
    st.markdown(f'### 🔍 *"{question}"*')

    cache_key = f"q_{question}"
    if cache_key not in st.session_state:
        queries = {}
        bar = st.progress(0, text="Translating...")
        for i, key in enumerate(DB_ORDER):
            db = dbs[key]
            bar.progress((i + 1) / len(DB_ORDER), text=f"Translating → {db.name}...")
            try:
                queries[key] = translate(client, question, db)
            except Exception as e:
                queries[key] = f"# Error: {e}"
        bar.empty()
        st.session_state[cache_key] = queries

    queries = st.session_state[cache_key]

    tabs = st.tabs([f"{DB_CLASSES[k].icon} {DB_CLASSES[k].name}" for k in DB_ORDER])

    for tab, key in zip(tabs, DB_ORDER):
        db = dbs[key]
        query = queries[key]
        with tab:
            with st.expander(f"📋 {db.name} Schema"):
                st.caption(db.description)
                _render_schema(key, db)
            st.markdown(f"**Generated Query:**")
            edited = st.code(query, language=db.lang)

            if st.button(f"▶ Run", key=f"run_{key}"):
                try:
                    results = db.run_query(query)
                    if results:
                        st.markdown(f"**Results** ({len(results)} rows):")
                        if key == "kv":
                            for r in results:
                                st.caption(f"`{r['command']}`")
                                _show_kv_result(r["result"])
                        elif key == "document":
                            st.json(results)
                        else:
                            st.dataframe(results, use_container_width=True, hide_index=True)
                    else:
                        st.info("Query returned no results.")
                        st.caption(f"💡 **Hint:** {NO_RESULTS_HINTS.get(key, '')}")
                except Exception as e:
                    st.error(f"Execution error: {e}")
                    st.caption(f"💡 **Hint:** {NO_RESULTS_HINTS.get(key, '')}")
                st.info(f"💡 {DB_TEACHING_NOTES.get(key, '')}")

    # Run All button
    st.divider()
    if st.button("▶ Run All — Compare Results Side by Side", type="primary"):
        for row_keys in [DB_ORDER[:3], DB_ORDER[3:]]:
            cols = st.columns(3)
            for col, key in zip(cols, row_keys):
                db = dbs[key]
                query = queries[key]
                with col:
                    st.markdown(f"**{db.icon} {db.name}**")
                    st.code(query, language=db.lang)
                    try:
                        results = db.run_query(query)
                        if results:
                            if key == "kv":
                                for r in results:
                                    st.caption(f"`{r['command']}`")
                                    _show_kv_result(r["result"])
                            elif key == "document":
                                st.json(results)
                            else:
                                st.dataframe(results, use_container_width=True, hide_index=True, height=250)
                        else:
                            st.info("No results")
                            st.caption(f"💡 {NO_RESULTS_HINTS.get(key, '')}")
                    except Exception as e:
                        st.error(str(e)[:100])
                        st.caption(f"💡 {NO_RESULTS_HINTS.get(key, '')}")
                    st.caption(f"💡 {DB_TEACHING_NOTES.get(key, '')}")
            st.divider()

    st.divider()
    st.caption(
        "💡 **Same question, same data, six different query languages.** "
        "Relational databases use JOINs. Wide column stores use denormalized tables with partition keys. "
        "Document databases use nested JSON filters. Key-value stores use direct key lookups. "
        "Graph databases match relationship patterns. Vector databases search by semantic similarity."
    )
