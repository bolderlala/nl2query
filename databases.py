"""
Six database engines — same data, different models.
Each class loads seed data and exposes run_query() + get_schema().
"""

import sqlite3
import json
import os
import shutil
from seed_data import STUDENTS, COURSES, ENROLLMENTS

DB_DIR = os.path.join(os.path.dirname(__file__), "_db_files")


def reset_db_dir():
    if os.path.exists(DB_DIR):
        shutil.rmtree(DB_DIR)
    os.makedirs(DB_DIR, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# 1. SQL — SQLite
# ═══════════════════════════════════════════════════════════════════════════

class SQLDatabase:
    name = "Relational Database (SQLite)"
    icon = "📊"
    lang = "sql"
    description = "Structured tables with rows and columns, linked by foreign keys. Query with SQL using JOINs and aggregations."
    playground = "https://sqliteonline.com/"

    SCHEMA_TEXT = """Tables:
  students (student_id INT PK, name TEXT, email TEXT, major TEXT, gpa REAL, year INT)
  courses  (course_id INT PK, name TEXT, department TEXT, credits INT, instructor TEXT)
  enrollments (student_id INT FK, course_id INT FK, score INT, semester TEXT)"""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._load()

    def _load(self):
        c = self.conn.cursor()
        c.execute("CREATE TABLE students (student_id INT PRIMARY KEY, name TEXT, email TEXT, major TEXT, gpa REAL, year INT)")
        c.execute("CREATE TABLE courses (course_id INT PRIMARY KEY, name TEXT, department TEXT, credits INT, instructor TEXT)")
        c.execute("CREATE TABLE enrollments (student_id INT, course_id INT, score INT, semester TEXT, FOREIGN KEY(student_id) REFERENCES students(student_id), FOREIGN KEY(course_id) REFERENCES courses(course_id))")
        for s in STUDENTS:
            c.execute("INSERT INTO students VALUES (?,?,?,?,?,?)", (s["id"], s["name"], s["email"], s["major"], s["gpa"], s["year"]))
        for co in COURSES:
            c.execute("INSERT INTO courses VALUES (?,?,?,?,?)", (co["id"], co["name"], co["department"], co["credits"], co["instructor"]))
        for e in ENROLLMENTS:
            c.execute("INSERT INTO enrollments VALUES (?,?,?,?)", (e["student_id"], e["course_id"], e["score"], e["semester"]))
        self.conn.commit()

    def run_query(self, query):
        rows = self.conn.execute(query).fetchall()
        if not rows:
            return []
        return [dict(r) for r in rows]

    def get_schema(self):
        return self.SCHEMA_TEXT

    def example_query(self):
        return """SELECT s.name, c.name AS course, e.score
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id
WHERE e.score > 85
ORDER BY e.score DESC;"""


# ═══════════════════════════════════════════════════════════════════════════
# 2. Wide Column Family — Cassandra-style (CQL over Python dicts)
# ═══════════════════════════════════════════════════════════════════════════

class ColumnFamilyDatabase:
    name = "Wide Column Store (Cassandra)"
    icon = "📋"
    lang = "sql"
    description = "Denormalized tables organized by partition key. No JOINs — data is duplicated to match query patterns."

    SCHEMA_TEXT = """Keyspace: msba

Table: students
  Partition Key: student_id (INT: 1-8)
  Columns: name TEXT, email TEXT, major TEXT, gpa FLOAT, year INT

Table: courses_by_department
  Partition Key: department (TEXT: 'MSBA', 'Darden')
  Clustering Key: course_id INT
  Columns: name TEXT, credits INT, instructor TEXT

Table: enrollments_by_student
  Partition Key: student_id (INT: 1-8)
  Clustering Key: course_id INT
  Columns: student_name TEXT, course_name TEXT, score INT, semester TEXT

Table: enrollments_by_course
  Partition Key: course_id (INT: 101=Managing Big Data, 102=Predictive Analytics, 103=Digital Transformation with AI, 104=Marketing Analytics, 105=Deep Learning & NLP)
  Clustering Key: student_id INT
  Columns: student_name TEXT, course_name TEXT, score INT, semester TEXT

RULES:
- No JOINs allowed. Each table is designed to answer one type of query.
- WHERE must filter on the partition key using its correct data type.
- Partition keys that are INT must use numeric values, not strings.
- Use =, >, <, >=, <= in WHERE. Can also filter on non-key columns."""

    def __init__(self):
        self.tables = {}
        self._load()

    def _load(self):
        course_map = {c["id"]: c["name"] for c in COURSES}
        student_map = {s["id"]: s["name"] for s in STUDENTS}

        self.tables["students"] = {
            "partition_key": "student_id",
            "rows": [
                {"student_id": s["id"], "name": s["name"], "email": s["email"],
                 "major": s["major"], "gpa": s["gpa"], "year": s["year"]}
                for s in STUDENTS
            ],
        }
        self.tables["courses_by_department"] = {
            "partition_key": "department",
            "rows": [
                {"department": c["department"], "course_id": c["id"],
                 "name": c["name"], "credits": c["credits"], "instructor": c["instructor"]}
                for c in COURSES
            ],
        }
        self.tables["enrollments_by_student"] = {
            "partition_key": "student_id",
            "rows": [
                {"student_id": e["student_id"], "course_id": e["course_id"],
                 "student_name": student_map[e["student_id"]],
                 "course_name": course_map[e["course_id"]],
                 "score": e["score"], "semester": e["semester"]}
                for e in ENROLLMENTS
            ],
        }
        self.tables["enrollments_by_course"] = {
            "partition_key": "course_id",
            "rows": [
                {"course_id": e["course_id"], "student_id": e["student_id"],
                 "student_name": student_map[e["student_id"]],
                 "course_name": course_map[e["course_id"]],
                 "score": e["score"], "semester": e["semester"]}
                for e in ENROLLMENTS
            ],
        }

    def run_query(self, query_str):
        try:
            return _exec_cql(self.tables, query_str)
        except Exception as e:
            return [{"error": str(e)}]

    def get_schema(self):
        return self.SCHEMA_TEXT

    def example_query(self):
        return """SELECT student_name, course_name, score
FROM enrollments_by_student
WHERE student_id = 1;"""


# ═══════════════════════════════════════════════════════════════════════════
# 3. Document DB — MontyDB (MongoDB syntax)
# ═══════════════════════════════════════════════════════════════════════════

class DocumentDatabase:
    name = "Document Database (MontyDB)"
    icon = "📄"
    lang = "javascript"
    description = "Flexible JSON documents with nested fields. No fixed schema — each document can have different structure."
    playground = "https://mongoplayground.net/"

    SCHEMA_TEXT = """Collection: students
Document structure:
{
  "_id": Number,
  "name": String,
  "email": String,
  "major": String,
  "gpa": Number,
  "year": Number,
  "enrollments": [
    { "course_id": Number, "course_name": String, "score": Number, "semester": String }
  ]
}

Collection: courses
{ "_id": Number, "name": String, "department": String, "credits": Number, "instructor": String }"""

    def __init__(self):
        from montydb import MontyClient
        self.client = MontyClient(":memory:")
        self.db = self.client["msba"]
        self._load()

    def _load(self):
        course_map = {c["id"]: c["name"] for c in COURSES}
        students_col = self.db["students"]
        courses_col = self.db["courses"]
        for s in STUDENTS:
            doc = {
                "_id": s["id"], "name": s["name"], "email": s["email"],
                "major": s["major"], "gpa": s["gpa"], "year": s["year"],
                "enrollments": []
            }
            for e in ENROLLMENTS:
                if e["student_id"] == s["id"]:
                    doc["enrollments"].append({
                        "course_id": e["course_id"],
                        "course_name": course_map[e["course_id"]],
                        "score": e["score"],
                        "semester": e["semester"],
                    })
            students_col.insert_one(doc)
        for c in COURSES:
            courses_col.insert_one({"_id": c["id"], "name": c["name"], "department": c["department"], "credits": c["credits"], "instructor": c["instructor"]})

    def run_query(self, query_str):
        try:
            env = {"db": self.db}
            exec(f"__result__ = list({query_str})", env)
            results = env["__result__"]
            return [_clean_doc(r) for r in results]
        except Exception as e:
            return [{"error": str(e)}]

    def get_schema(self):
        return self.SCHEMA_TEXT

    def example_query(self):
        return 'db["students"].find({"gpa": {"$gte": 3.7}})'


# ═══════════════════════════════════════════════════════════════════════════
# 3. Key-Value — shelve with Redis-style wrapper
# ═══════════════════════════════════════════════════════════════════════════

class KeyValueDatabase:
    name = "Key-Value Store (Redis)"
    icon = "⚡"
    lang = "bash"
    description = "Simple key-to-value mappings for ultra-fast lookups. Supports hashes, sets, and sorted sets."
    playground = "https://try.redis.io/"

    SCHEMA_TEXT = """Key patterns:
  student:{id}         → Hash  {name, email, major, gpa, year}
  course:{id}          → Hash  {name, department, credits, instructor}
  enrollment:{sid}:{cid} → Hash {score, semester}
  student:{id}:courses → Set   {course_id, course_id, ...}
  course:{id}:students → Set   {student_id, student_id, ...}
  scores:{course_id}   → Sorted Set  (member=student_id, score=score)

Commands: SET, GET, HSET, HGET, HGETALL, SADD, SMEMBERS, ZADD, ZRANGEBYSCORE, KEYS"""

    def __init__(self):
        self.store = {}
        self._load()

    def _load(self):
        for s in STUDENTS:
            key = f"student:{s['id']}"
            self.store[key] = {"name": s["name"], "email": s["email"], "major": s["major"], "gpa": str(s["gpa"]), "year": str(s["year"])}
        for c in COURSES:
            key = f"course:{c['id']}"
            self.store[key] = {"name": c["name"], "department": c["department"], "credits": str(c["credits"]), "instructor": c["instructor"]}
        for e in ENROLLMENTS:
            ekey = f"enrollment:{e['student_id']}:{e['course_id']}"
            self.store[ekey] = {"score": str(e["score"]), "semester": e["semester"]}
            sc_key = f"student:{e['student_id']}:courses"
            if sc_key not in self.store:
                self.store[sc_key] = set()
            self.store[sc_key].add(str(e["course_id"]))
            cs_key = f"course:{e['course_id']}:students"
            if cs_key not in self.store:
                self.store[cs_key] = set()
            self.store[cs_key].add(str(e["student_id"]))
            zkey = f"scores:{e['course_id']}"
            if zkey not in self.store:
                self.store[zkey] = {}
            self.store[zkey][str(e["student_id"])] = e["score"]

    def run_query(self, query_str):
        results = []
        for line in query_str.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            result = self._exec_command(line)
            results.append({"command": line, "result": result})
        return results

    def _exec_command(self, cmd):
        parts = _parse_redis_cmd(cmd)
        if not parts:
            return "(error) empty command"
        op = parts[0].upper()
        args = parts[1:]

        if op == "GET" and len(args) == 1:
            v = self.store.get(args[0])
            return json.dumps(v) if v else "(nil)"
        elif op == "HGETALL" and len(args) == 1:
            v = self.store.get(args[0])
            return v if isinstance(v, dict) else "(nil)"
        elif op == "HGET" and len(args) == 2:
            v = self.store.get(args[0])
            if isinstance(v, dict):
                return v.get(args[1], "(nil)")
            return "(nil)"
        elif op == "SMEMBERS" and len(args) == 1:
            v = self.store.get(args[0])
            return sorted(v) if isinstance(v, set) else "(nil)"
        elif op == "KEYS" and len(args) == 1:
            import fnmatch
            pattern = args[0]
            return sorted(k for k in self.store if fnmatch.fnmatch(k, pattern))
        elif op == "ZRANGEBYSCORE" and len(args) >= 3:
            zkey = args[0]
            lo = float('-inf') if args[1] == '-inf' else float(args[1])
            hi = float('inf') if args[2] == '+inf' else float(args[2])
            v = self.store.get(zkey)
            if isinstance(v, dict):
                return sorted(
                    [{"member": m, "score": s} for m, s in v.items() if lo <= s <= hi],
                    key=lambda x: x["score"]
                )
            return "(nil)"
        elif op == "ZRANGE" and len(args) >= 3:
            zkey = args[0]
            v = self.store.get(zkey)
            if isinstance(v, dict):
                sorted_items = sorted(v.items(), key=lambda x: x[1])
                start, stop = int(args[1]), int(args[2])
                if stop == -1:
                    stop = len(sorted_items)
                else:
                    stop += 1
                return [{"member": m, "score": s} for m, s in sorted_items[start:stop]]
            return "(nil)"
        elif op == "ZREVRANGE" and len(args) >= 3:
            zkey = args[0]
            v = self.store.get(zkey)
            if isinstance(v, dict):
                sorted_items = sorted(v.items(), key=lambda x: x[1], reverse=True)
                start, stop = int(args[1]), int(args[2])
                if stop == -1:
                    stop = len(sorted_items)
                else:
                    stop += 1
                return [{"member": m, "score": s} for m, s in sorted_items[start:stop]]
            return "(nil)"
        else:
            return f"(supported commands: GET, HGETALL, HGET, SMEMBERS, KEYS, ZRANGEBYSCORE, ZRANGE, ZREVRANGE)"

    def get_schema(self):
        return self.SCHEMA_TEXT

    def example_query(self):
        return """HGETALL student:1
SMEMBERS student:1:courses
ZRANGEBYSCORE scores:102 85 100
KEYS student:*"""


# ═══════════════════════════════════════════════════════════════════════════
# 4. Graph DB — Kuzu (Cypher)
# ═══════════════════════════════════════════════════════════════════════════

class GraphDatabase:
    name = "Graph Database (Kuzu)"
    icon = "🕸️"
    lang = "cypher"
    description = "Nodes connected by relationships. Pattern matching with Cypher to traverse connections."
    playground = "https://neo4j.com/sandbox"

    SCHEMA_TEXT = """Node labels:
  Student (student_id INT, name STRING, email STRING, major STRING, gpa DOUBLE, year INT)
  Course  (course_id INT, name STRING, department STRING, credits INT, instructor STRING)

Relationship types:
  ENROLLED_IN (score INT, semester STRING)  from Student to Course"""

    def __init__(self):
        import kuzu
        self.db_path = os.path.join(DB_DIR, "kuzu_db")
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
        self.db = kuzu.Database(self.db_path)
        self.conn = kuzu.Connection(self.db)
        self._load()

    def _load(self):
        self.conn.execute("CREATE NODE TABLE Student (student_id INT64, name STRING, email STRING, major STRING, gpa DOUBLE, year INT64, PRIMARY KEY(student_id))")
        self.conn.execute("CREATE NODE TABLE Course (course_id INT64, name STRING, department STRING, credits INT64, instructor STRING, PRIMARY KEY(course_id))")
        self.conn.execute("CREATE REL TABLE ENROLLED_IN (FROM Student TO Course, score INT64, semester STRING)")

        for s in STUDENTS:
            self.conn.execute(
                "CREATE (s:Student {student_id: $id, name: $name, email: $email, major: $major, gpa: $gpa, year: $year})",
                {"id": s["id"], "name": s["name"], "email": s["email"], "major": s["major"], "gpa": s["gpa"], "year": s["year"]}
            )
        for c in COURSES:
            self.conn.execute(
                "CREATE (c:Course {course_id: $id, name: $name, department: $dept, credits: $credits, instructor: $instructor})",
                {"id": c["id"], "name": c["name"], "dept": c["department"], "credits": c["credits"], "instructor": c["instructor"]}
            )
        for e in ENROLLMENTS:
            self.conn.execute(
                "MATCH (s:Student {student_id: $sid}), (c:Course {course_id: $cid}) CREATE (s)-[:ENROLLED_IN {score: $score, semester: $sem}]->(c)",
                {"sid": e["student_id"], "cid": e["course_id"], "score": e["score"], "sem": e["semester"]}
            )

    def run_query(self, query):
        try:
            result = self.conn.execute(query)
            columns = result.get_column_names()
            rows = []
            while result.has_next():
                row = result.get_next()
                rows.append(dict(zip(columns, row)))
            return rows
        except Exception as e:
            return [{"error": str(e)}]

    def get_schema(self):
        return self.SCHEMA_TEXT

    def example_query(self):
        return """MATCH (s:Student)-[e:ENROLLED_IN]->(c:Course)
WHERE e.score > 85
RETURN s.name, c.name AS course, e.score
ORDER BY e.score DESC;"""


# ═══════════════════════════════════════════════════════════════════════════
# 5. Vector DB — ChromaDB
# ═══════════════════════════════════════════════════════════════════════════

class VectorDatabase:
    name = "Vector Database (ChromaDB)"
    icon = "🧭"
    lang = "python"
    description = "Semantic similarity search using text embeddings. Find by meaning, not exact keywords."
    playground = "https://www.trychroma.com/"

    SCHEMA_TEXT = """Collection: student_profiles
Each document:
{
  "id": "student_{id}",
  "document": student bio text (embedded automatically),
  "metadata": { "name": String, "major": String, "gpa": Float, "year": Int }
}

Collection: course_catalog
Each document:
{
  "id": "course_{id}",
  "document": course description text (embedded automatically),
  "metadata": { "name": String, "department": String, "instructor": String }
}

Operations: collection.query(query_texts=["..."], n_results=N, where={...})"""

    def __init__(self):
        import chromadb
        self.client = chromadb.Client()
        self._load()

    def _load(self):
        students_col = self.client.get_or_create_collection("student_profiles")
        courses_col = self.client.get_or_create_collection("course_catalog")

        students_col.add(
            ids=[f"student_{s['id']}" for s in STUDENTS],
            documents=[s["bio"] for s in STUDENTS],
            metadatas=[{"name": s["name"], "major": s["major"], "gpa": s["gpa"], "year": s["year"]} for s in STUDENTS],
        )
        courses_col.add(
            ids=[f"course_{c['id']}" for c in COURSES],
            documents=[c["description"] for c in COURSES],
            metadatas=[{"name": c["name"], "department": c["department"], "instructor": c["instructor"]} for c in COURSES],
        )

    def run_query(self, query_str):
        try:
            env = {"client": self.client}
            exec(f"__result__ = {query_str}", env)
            raw = env["__result__"]
            if isinstance(raw, dict):
                docs = raw.get("documents", [[]])[0]
                metas = raw.get("metadatas", [[]])[0]
                dists = raw.get("distances", [[]])[0]
                ids = raw.get("ids", [[]])[0]
                return [
                    {"id": ids[i], "document": docs[i], "similarity": round(1 - dists[i], 3) if dists else None, **metas[i]}
                    for i in range(len(ids))
                ]
            return [{"result": str(raw)}]
        except Exception as e:
            return [{"error": str(e)}]

    def get_schema(self):
        return self.SCHEMA_TEXT

    def example_query(self):
        return 'client.get_collection("student_profiles").query(query_texts=["machine learning and AI"], n_results=3)'


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _clean_doc(doc):
    """Remove MontyDB internal fields and flatten nested structures for display."""
    if not isinstance(doc, dict):
        return doc
    cleaned = {}
    for k, v in doc.items():
        if k.startswith("_"):
            continue
        if isinstance(v, list) and v and isinstance(v[0], dict):
            cleaned[k] = json.dumps(v)
        else:
            cleaned[k] = v
    return cleaned


def _parse_redis_cmd(cmd):
    """Parse a Redis-style command string into parts, respecting quotes."""
    parts = []
    current = []
    in_quote = None
    for ch in cmd:
        if ch in ('"', "'") and not in_quote:
            in_quote = ch
        elif ch == in_quote:
            in_quote = None
        elif ch == ' ' and not in_quote:
            if current:
                parts.append(''.join(current))
                current = []
            continue
        else:
            current.append(ch)
    if current:
        parts.append(''.join(current))
    return parts


def _exec_cql(tables, query_str):
    """Parse and execute one or more simplified CQL SELECT statements."""
    import re
    statements = [s.strip().rstrip(";").strip() for s in re.split(r";(?:\s*\n|\s+(?=SELECT))", query_str.strip(), flags=re.IGNORECASE)]
    if len(statements) <= 1:
        statements = [s.strip().rstrip(";").strip() for s in re.split(r"\n(?=SELECT\s)", query_str.strip(), flags=re.IGNORECASE)]
    all_results = []
    for stmt in statements:
        if not stmt:
            continue
        result = _exec_single_cql(tables, stmt)
        all_results.extend(result)
    return all_results


def _exec_single_cql(tables, query_str):
    """Parse and execute a single CQL SELECT statement."""
    import re
    q = " ".join(query_str.strip().rstrip(";").split())
    q = re.sub(r"\s+ALLOW\s+FILTERING\s*$", "", q, flags=re.IGNORECASE)

    m = re.match(r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+GROUP\s+BY\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?$", q, re.IGNORECASE)
    if not m:
        return [{"error": "Could not parse CQL. Expected: SELECT ... FROM table [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT n]"}]

    select_clause, table_name, where_clause, group_clause, order_clause, limit_str = m.groups()
    table_name = table_name.lower()
    if table_name not in tables:
        return [{"error": f"Table '{table_name}' not found. Available: {', '.join(tables.keys())}"}]

    rows = list(tables[table_name]["rows"])

    if where_clause:
        conditions = re.split(r"\s+AND\s+", where_clause, flags=re.IGNORECASE)
        for cond in conditions:
            cond = cond.strip()

            in_m = re.match(r"(\w+)\s+IN\s*\((.+?)\)", cond, re.IGNORECASE)
            if in_m:
                col = in_m.group(1)
                raw_vals = [v.strip().strip("'\"") for v in in_m.group(2).split(",")]
                parsed_vals = []
                for v in raw_vals:
                    try:
                        parsed_vals.append(float(v) if "." in v else int(v))
                    except ValueError:
                        parsed_vals.append(v)

                def matches_in(row, col=col, vals=parsed_vals):
                    rv = row.get(col)
                    if rv is None:
                        return False
                    for v in vals:
                        if isinstance(rv, (int, float)) and isinstance(v, (int, float)):
                            if float(rv) == float(v):
                                return True
                        elif str(rv) == str(v):
                            return True
                    return False

                rows = [r for r in rows if matches_in(r)]
                continue

            cm = re.match(r"(\w+)\s*(=|!=|>=|<=|>|<)\s*(.+)", cond)
            if not cm:
                return [{"error": f"Cannot parse condition: {cond}"}]
            col, op, val = cm.group(1), cm.group(2), cm.group(3).strip().strip("'\"")
            try:
                num_val = float(val) if "." in val else int(val)
            except ValueError:
                num_val = None

            def matches(row, col=col, op=op, val=val, num_val=num_val):
                rv = row.get(col)
                if rv is None:
                    return False
                if isinstance(rv, (int, float)) and num_val is not None:
                    rv, cmp = float(rv), float(num_val)
                else:
                    rv, cmp = str(rv), val
                if op == "=":
                    return rv == cmp
                elif op == "!=":
                    return rv != cmp
                elif op == ">":
                    return rv > cmp
                elif op == "<":
                    return rv < cmp
                elif op == ">=":
                    return rv >= cmp
                elif op == "<=":
                    return rv <= cmp
                return False

            rows = [r for r in rows if matches(r)]

    agg_pattern = re.compile(r"(COUNT|AVG|SUM|MIN|MAX)\s*\(\s*(\*|\w+)\s*\)", re.IGNORECASE)
    has_agg = agg_pattern.search(select_clause)

    if has_agg and group_clause:
        group_cols = [c.strip() for c in group_clause.split(",")]
        groups = {}
        for r in rows:
            gk = tuple(r.get(c) for c in group_cols)
            groups.setdefault(gk, []).append(r)
        result_rows = []
        for gk, group_rows in groups.items():
            row_out = dict(zip(group_cols, gk))
            for token in re.split(r",\s*", select_clause):
                token = token.strip()
                am = agg_pattern.match(token)
                if am:
                    func, col = am.group(1).upper(), am.group(2)
                    vals = [r.get(col, 0) for r in group_rows] if col != "*" else [1] * len(group_rows)
                    if func == "COUNT":
                        row_out[token] = len(vals)
                    elif func == "AVG":
                        row_out[token] = round(sum(vals) / len(vals), 2) if vals else 0
                    elif func == "SUM":
                        row_out[token] = sum(vals)
                    elif func == "MIN":
                        row_out[token] = min(vals)
                    elif func == "MAX":
                        row_out[token] = max(vals)
            result_rows.append(row_out)
        rows = result_rows
    elif has_agg:
        row_out = {}
        for token in re.split(r",\s*", select_clause):
            token = token.strip()
            am = agg_pattern.match(token)
            if am:
                func, col = am.group(1).upper(), am.group(2)
                vals = [r.get(col, 0) for r in rows] if col != "*" else [1] * len(rows)
                if func == "COUNT":
                    row_out[token] = len(vals)
                elif func == "AVG":
                    row_out[token] = round(sum(vals) / len(vals), 2) if vals else 0
                elif func == "SUM":
                    row_out[token] = sum(vals)
                elif func == "MIN":
                    row_out[token] = min(vals)
                elif func == "MAX":
                    row_out[token] = max(vals)
        return [row_out] if row_out else []
    else:
        if order_clause:
            parts = order_clause.split()
            order_col = parts[0]
            desc = len(parts) > 1 and parts[1].upper() == "DESC"
            rows.sort(key=lambda r: r.get(order_col, 0), reverse=desc)

        if limit_str:
            rows = rows[:int(limit_str)]

        if select_clause.strip() != "*":
            cols = [c.strip() for c in select_clause.split(",")]
            rows = [{c: r.get(c) for c in cols if c in r} for r in rows]

    return rows


def init_all():
    """Initialize all 6 databases and return them."""
    reset_db_dir()
    return {
        "sql": SQLDatabase(),
        "column": ColumnFamilyDatabase(),
        "document": DocumentDatabase(),
        "kv": KeyValueDatabase(),
        "graph": GraphDatabase(),
        "vector": VectorDatabase(),
    }
