# NL2Query — Natural Language to Database Query Translator

A teaching demo that translates natural-language questions into executable queries for **six different database types** — and runs them live against real embedded databases loaded with the same dataset.

Built for the MSBA program at UVA McIntire School of Commerce.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-1.37+-red)
![License](https://img.shields.io/badge/License-MIT-green)

## How It Works

1. **Type a question** in plain English (e.g., "Find all students with a GPA above 3.7")
2. **Claude AI** translates it into six query languages simultaneously
3. **Each query executes** against a real embedded database
4. **Compare results** side by side across all six data models

## Quick Start (for students)

**Step 1 — Install dependencies** (one time only)

```bash
cd nl2query
pip install -r requirements.txt
```

**Step 2 — Launch the app**

```bash
streamlit run app.py
```

**Step 3 — Enter the API key**

When the app opens, paste the API key (shared on Canvas) into the sidebar input box. That's it — start asking questions!

> **Optional:** To avoid pasting the key every time, save it to a file called `anthropic_api_key.txt` in the `nl2query/` folder. The app will load it automatically on future runs.

Open http://localhost:8501 in your browser.

## The Six Data Models

Each data model represents a fundamentally different way of organizing and querying data. All six databases are **embedded** — they run locally inside the Python process with no external servers, no cloud connections, and no playground links required.

### Relational Database (SQLite)

| | |
|---|---|
| **Data Model** | Structured tables with rows and columns, linked by foreign keys |
| **Software** | SQLite — Python's built-in `sqlite3` module, no installation needed |
| **How data is stored** | 3 normalized tables (`students`, `courses`, `enrollments`). The enrollments table uses foreign keys to link students to courses. Data is combined at query time using JOINs. |

```python
self.conn = sqlite3.connect(":memory:")  # in-memory database
c.execute("CREATE TABLE students (student_id INT PRIMARY KEY, ...)")
c.execute("INSERT INTO students VALUES (?,?,?,?,?,?)", (...))
# Queries run via: self.conn.execute(query).fetchall()
```

**Example query:**
```sql
SELECT s.name, c.name AS course, e.score
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id
WHERE e.score > 85;
```

### Wide Column Store (Cassandra)

| | |
|---|---|
| **Data Model** | Denormalized tables organized by partition key — no JOINs allowed |
| **Software** | A Python CQL parser over in-memory dictionaries (emulates Cassandra's query model) |
| **How data is stored** | 4 denormalized tables, each designed to answer one type of query. `students` (by student_id), `courses_by_department` (by department), `enrollments_by_student` (by student_id), `enrollments_by_course` (by course_id). Data is duplicated across tables so each query only hits one table. |

```python
self.tables = {}  # dict-of-dicts as the column family store
self.tables["enrollments_by_student"] = {
    "partition_key": "student_id",
    "rows": [
        {"student_id": 1, "course_id": 101, "student_name": "Alice Chen",
         "course_name": "Managing Big Data", "score": 92, "semester": "Fall 2025"},
        ...
    ],
}
# Queries parsed from CQL:
#   SELECT ... FROM table WHERE partition_key = value
```

**Example query:**
```sql
SELECT student_name, course_name, score
FROM enrollments_by_student
WHERE student_id = 1;
```

### Document Database (MontyDB)

| | |
|---|---|
| **Data Model** | Flexible JSON documents with nested fields and arrays |
| **Software** | MontyDB — a pure-Python library that implements MongoDB's query API |
| **How data is stored** | 2 collections (`students`, `courses`). Enrollments are embedded as a nested array inside each student document — no JOINs needed because related data lives together. |

```python
self.client = MontyClient(":memory:")  # in-memory MongoDB
self.db = self.client["msba"]
doc = {"_id": 1, "name": "Alice", "enrollments": [
  {"course_name": "Managing Big Data", "score": 92}, ...
]}
self.db["students"].insert_one(doc)
# Queries run via: db["students"].find({"gpa": {"$gte": 3.7}})
```

**Example query:**
```javascript
db["students"].find({"gpa": {"$gte": 3.7}})
```

### Graph Database (Kuzu)

| | |
|---|---|
| **Data Model** | Nodes (entities) connected by relationships (edges) |
| **Software** | Kuzu — an embedded graph database (like SQLite, but for graphs). Uses Cypher, the same query language as Neo4j. |
| **How data is stored** | Student and Course nodes connected by `ENROLLED_IN` relationships. Each relationship carries score and semester as properties. Traversal queries are natural — "find all courses a student is enrolled in" is just following edges. |

```python
self.db = kuzu.Database("_db_files/kuzu_db")  # local file
self.conn = kuzu.Connection(self.db)
conn.execute("CREATE NODE TABLE Student (...)")
conn.execute("CREATE REL TABLE ENROLLED_IN (FROM Student TO Course, ...)")
conn.execute("CREATE (s:Student {name: $name, ...})", params)
# Queries run via: conn.execute("MATCH (s:Student)...")
```

**Example query:**
```cypher
MATCH (s:Student)-[e:ENROLLED_IN]->(c:Course)
WHERE e.score > 85
RETURN s.name, c.name AS course, e.score;
```

### Key-Value Store (Redis)

| | |
|---|---|
| **Data Model** | Simple key-to-value mappings — the simplest data model |
| **Software** | A Python dictionary that emulates Redis commands (no external Redis server needed) |
| **How data is stored** | Flat key-value pairs using Redis naming conventions. Each student, course, and enrollment is a separate hash (e.g., `student:1`). Sets track which courses a student takes (`student:1:courses`). Sorted sets rank students by score per course (`scores:102`). |

```python
self.store = {}  # plain Python dict as the data store
self.store["student:1"] = {"name": "Alice Chen", "gpa": "3.9", ...}
self.store["student:1:courses"] = {"101", "102", "105"}  # a set
self.store["scores:102"] = {"1": 88, "3": 90, ...}  # sorted set
# Queries parsed from Redis commands:
#   HGETALL student:1 -> returns the hash
#   ZRANGEBYSCORE scores:102 85 100 -> score range query
```

**Example query:**
```bash
HGETALL student:1
SMEMBERS student:1:courses
ZRANGEBYSCORE scores:102 85 100
```

### Vector Database (ChromaDB)

| | |
|---|---|
| **Data Model** | Text converted into numerical vectors (embeddings) that capture meaning |
| **Software** | ChromaDB — an embedded vector database. Automatically converts text into embeddings using a built-in model. |
| **How data is stored** | 2 collections — `student_profiles` (student bios) and `course_catalog` (course descriptions). Each text document is automatically embedded as a vector. Metadata (name, major, gpa) stored alongside for filtering. |

```python
self.client = chromadb.Client()  # in-memory vector store
col = self.client.get_or_create_collection("student_profiles")
col.add(
    ids=["student_1", ...],
    documents=["Passionate about machine learning...", ...],  # auto-embedded
    metadatas=[{"name": "Alice Chen", "gpa": 3.9}, ...]
)
# Queries run via:
# col.query(query_texts=["machine learning"], n_results=3)
```

**Example query:**
```python
client.get_collection("student_profiles").query(
    query_texts=["machine learning and AI"], n_results=3
)
```

## The Dataset

All six databases are loaded with the **exact same data**: 8 students, 5 courses, and 22 enrollments. This lets you compare how each data model stores and queries identical information.

### Students (8 records)

| ID | Name | Major | GPA | Year |
|----|------|-------|-----|------|
| 1 | Alice Chen | Business Analytics | 3.9 | 2026 |
| 2 | Brian Kim | Business Analytics | 3.5 | 2026 |
| 3 | Carla Diaz | Data Science | 3.7 | 2026 |
| 4 | David Patel | Business Analytics | 3.2 | 2025 |
| 5 | Elena Rossi | Data Science | 3.8 | 2025 |
| 6 | Frank Okafor | Business Analytics | 3.6 | 2026 |
| 7 | Grace Liu | Data Science | 4.0 | 2025 |
| 8 | Henry Nguyen | Business Analytics | 3.4 | 2026 |

### Courses (5 records)

| ID | Course Name | Dept | Instructor |
|----|-------------|------|------------|
| 101 | Managing Big Data | MSBA | Prof. Li |
| 102 | Predictive Analytics | MSBA | Prof. Mousavi |
| 103 | Digital Transformation with AI | MSBA | Prof. Wright |
| 104 | Marketing Analytics | Darden | Prof. Venkatesan |
| 105 | Deep Learning & NLP | MSBA | Prof. Mousavi |

### Enrollments (22 records)

Each enrollment links a student to a course with a score and semester. Scores range from 68 to 98. Semesters span Fall 2024 through Spring 2026.

## How Each Data Model Stores the Same Data

| Data Model | Storage Approach | Query Strength |
|------------|-----------------|----------------|
| 📊 **Relational** (SQLite) | 3 normalized tables linked by foreign keys | JOINs, aggregations (GROUP BY, AVG), sorting |
| 📋 **Wide Column** (Cassandra) | 4 denormalized tables, each for one query pattern | Lightning-fast partition-key lookups at scale |
| 📄 **Document** (MontyDB) | 2 collections with nested enrollments arrays | Flexible schemas, querying nested/embedded data |
| 🕸️ **Graph** (Kuzu) | Student/Course nodes connected by ENROLLED_IN edges | Relationship traversals and pattern matching |
| ⚡ **Key-Value** (Redis) | Flat key-value pairs (hashes, sets, sorted sets) | Sub-millisecond direct lookups by key |
| 🧭 **Vector** (ChromaDB) | Text embeddings of bios and descriptions | Semantic similarity ("find things like X") |

## Sample Questions to Try

Each question highlights a different database's strength. The icon shows which data model handles it best — and why.

| Question | Best DB | Why This DB Excels |
|----------|---------|-------------------|
| Find all students with a GPA above 3.7 | 📊 SQL | Simple WHERE filter — SQL's bread and butter |
| Which students scored above 90 in Predictive Analytics? | 📊 SQL | JOIN + WHERE — relational excels at combining tables |
| Show all courses taught by Prof. Mousavi | 📋 Wide Column | Partition-key lookup on `courses_by_department` |
| Find students enrolled in both Managing Big Data and Predictive Analytics | 🕸️ Graph | Pattern matching across two relationship edges |
| What is the average score for each course? | 📊 SQL | GROUP BY + aggregation is SQL's core strength |
| Who got the highest score in Deep Learning & NLP? | 📊 SQL | ORDER BY + LIMIT — classic relational query |
| List all Business Analytics majors and their courses | 📄 Document | Nested enrollments array lives inside student doc |
| Find students whose interests relate to machine learning | 🧭 Vector | Semantic similarity search on student bios |
| Find all students who share at least one course with Alice Chen | 🕸️ Graph | 2-hop traversal — natural in graph, clunky self-join in SQL |
| Look up the profile of student ID 1 | ⚡ Key-Value | Direct key lookup — KV's fastest operation |

## Teaching Notes

After running a query, each database tab displays a **teaching note** generated by Claude that explains:

- **What happened** — how this database handled the query and what query pattern it used
- **Why** — the fundamental design reason behind the behavior (e.g., "no secondary indexes," "partition-key lookup," "semantic similarity is approximate by design")
- **In production** — how real-world systems solve the same problem when the demo approach hits a limitation (e.g., Redis Search module, MongoDB aggregation pipelines, Cassandra SASI indexes)

### How to Use Teaching Notes for Learning

1. **Compare across all 6 databases.** Run the same question on all tabs (or click "Run All") and read the teaching notes side by side. Notice where each database shines and where it struggles — this builds intuition for choosing the right data model for a workload.
2. **Start with a question that favors one database.** Pick a sample question (the dropdown icon shows the best DB), then ask yourself: *why is this hard for the other five?* The teaching notes will explain each one's constraint.
3. **Pay attention to "In production" hints.** The demo uses embedded databases with limitations. Teaching notes bridge the gap to real-world systems — for example, MontyDB can't run aggregation pipelines, but production MongoDB can. These hints show what you'd actually do at scale.
4. **Use "Run All" for trade-off discussions.** The side-by-side view with all 6 teaching notes is ideal for classroom discussion or group exercises. Ask: *if you were building this system at scale, which database would you choose and why?*

## Project Structure

```
nl2query/
├── app.py              # Streamlit UI + Claude translation logic
├── databases.py        # Six database engine classes
├── seed_data.py        # Shared dataset (students, courses, enrollments)
├── requirements.txt    # Python dependencies
└── .gitignore
```

### `seed_data.py` — The Shared Dataset

Defines three lists that every database loads:

- **STUDENTS** (8 records): id, name, email, major, gpa, year, bio
- **COURSES** (5 records): id, name, department, credits, instructor, description
- **ENROLLMENTS** (22 records): student_id, course_id, score, semester

### `databases.py` — Six Embedded Database Engines

Each database is a Python class with the same interface:

- `__init__()` — creates the database in memory and loads seed data
- `get_schema()` — returns the schema description (sent to Claude for translation)
- `run_query(query_str)` — executes a query string and returns results as a list of dicts
- `example_query()` — returns a sample query for the UI

### `app.py` — Streamlit UI + Claude Translation

- **API key loading**: checks `ANTHROPIC_API_KEY` env var first, then falls back to a local `anthropic_api_key.txt` file (or `../anthropic_api_key.txt`)
- **Database initialization**: all 6 databases are created once via `@st.cache_resource` and reused across page reruns
- **Translation**: sends a system prompt with database-specific syntax rules + the target schema to Claude, which returns the executable query
- **Execution**: each generated query runs directly against the real embedded database
- **Caching**: translated queries are cached in `st.session_state` to avoid duplicate API calls

## Deployment (Streamlit Community Cloud)

To deploy so students just need a URL and password — no local setup:

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) and connect the repo
3. In **Settings > Secrets**, paste:
   ```toml
   ANTHROPIC_API_KEY = "sk-ant-..."
   CLASS_PASSWORD = "your-class-password"
   ```
4. Share the app URL and class password with students on Canvas

The password gate blocks access until students enter the correct password. Your API key stays server-side — students never see it. Set a [spend limit](https://console.anthropic.com/settings/limits) on your Anthropic account as a safety net.

## Running Locally

For local use, the app checks for the API key in this order:

1. **Streamlit secrets** (`.streamlit/secrets.toml`) — used by Streamlit Cloud
2. **`ANTHROPIC_API_KEY`** environment variable
3. **`anthropic_api_key.txt`** file in the app folder or parent directory

If `CLASS_PASSWORD` is not set in secrets, the password gate is skipped (convenient for local development).

## Key Takeaway

*The same question produces very different syntax depending on the data model.* Relational databases use tables and JOINs. Wide column stores use denormalized tables with partition keys. Document databases use nested JSON filters. Key-value stores use direct key lookups. Graph databases traverse relationship patterns. Vector databases search by semantic similarity. The data model determines the query language — and understanding this is what makes you a polyglot data professional.

## Requirements

- Python 3.10+
- An [Anthropic API key](https://console.anthropic.com/)

## License

MIT
