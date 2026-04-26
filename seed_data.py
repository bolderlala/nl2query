"""Unified seed data for all 5 database types."""

STUDENTS = [
    {"id": 1, "name": "Alice Chen", "email": "ac1@virginia.edu", "major": "Business Analytics", "gpa": 3.9, "year": 2026, "bio": "Passionate about machine learning and predictive modeling for healthcare applications"},
    {"id": 2, "name": "Brian Kim", "email": "bk2@virginia.edu", "major": "Business Analytics", "gpa": 3.5, "year": 2026, "bio": "Former consultant focused on customer analytics and marketing optimization"},
    {"id": 3, "name": "Carla Diaz", "email": "cd3@virginia.edu", "major": "Data Science", "gpa": 3.7, "year": 2026, "bio": "Background in NLP and text mining with interest in social media analytics"},
    {"id": 4, "name": "David Patel", "email": "dp4@virginia.edu", "major": "Business Analytics", "gpa": 3.2, "year": 2025, "bio": "Supply chain analyst exploring AI-driven demand forecasting"},
    {"id": 5, "name": "Elena Rossi", "email": "er5@virginia.edu", "major": "Data Science", "gpa": 3.8, "year": 2025, "bio": "Deep learning researcher working on computer vision for manufacturing quality control"},
    {"id": 6, "name": "Frank Okafor", "email": "fo6@virginia.edu", "major": "Business Analytics", "gpa": 3.6, "year": 2026, "bio": "Interested in financial analytics and algorithmic trading strategies"},
    {"id": 7, "name": "Grace Liu", "email": "gl7@virginia.edu", "major": "Data Science", "gpa": 4.0, "year": 2025, "bio": "AI ethics researcher focused on fairness in recommendation systems"},
    {"id": 8, "name": "Henry Nguyen", "email": "hn8@virginia.edu", "major": "Business Analytics", "gpa": 3.4, "year": 2026, "bio": "Digital marketing specialist building AI-powered customer segmentation tools"},
]

COURSES = [
    {"id": 101, "name": "Managing Big Data", "department": "MSBA", "credits": 3, "instructor": "Prof. Li", "description": "Data lakes, cloud infrastructure, and scalable data pipelines for AI"},
    {"id": 102, "name": "Predictive Analytics", "department": "MSBA", "credits": 3, "instructor": "Prof. Mousavi", "description": "Regression, classification, and machine learning for business forecasting"},
    {"id": 103, "name": "Digital Transformation with AI", "department": "MSBA", "credits": 3, "instructor": "Prof. Wright", "description": "Agentic AI, change management, and organizational adoption of AI systems"},
    {"id": 104, "name": "Marketing Analytics", "department": "Darden", "credits": 3, "instructor": "Prof. Venkatesan", "description": "Customer analytics, AI marketing canvas, and data-driven brand strategy"},
    {"id": 105, "name": "Deep Learning & NLP", "department": "MSBA", "credits": 3, "instructor": "Prof. Mousavi", "description": "Transformers, LLMs, fine-tuning, and text analytics for business"},
]

ENROLLMENTS = [
    {"student_id": 1, "course_id": 101, "score": 92, "semester": "Fall 2025"},
    {"student_id": 1, "course_id": 102, "score": 88, "semester": "Fall 2025"},
    {"student_id": 1, "course_id": 105, "score": 95, "semester": "Spring 2026"},
    {"student_id": 2, "course_id": 101, "score": 78, "semester": "Fall 2025"},
    {"student_id": 2, "course_id": 104, "score": 85, "semester": "Spring 2026"},
    {"student_id": 3, "course_id": 102, "score": 90, "semester": "Fall 2025"},
    {"student_id": 3, "course_id": 105, "score": 93, "semester": "Spring 2026"},
    {"student_id": 3, "course_id": 103, "score": 87, "semester": "Spring 2026"},
    {"student_id": 4, "course_id": 101, "score": 72, "semester": "Fall 2024"},
    {"student_id": 4, "course_id": 102, "score": 68, "semester": "Fall 2024"},
    {"student_id": 4, "course_id": 104, "score": 75, "semester": "Spring 2025"},
    {"student_id": 5, "course_id": 105, "score": 97, "semester": "Fall 2024"},
    {"student_id": 5, "course_id": 102, "score": 91, "semester": "Fall 2024"},
    {"student_id": 5, "course_id": 103, "score": 89, "semester": "Spring 2025"},
    {"student_id": 6, "course_id": 101, "score": 82, "semester": "Fall 2025"},
    {"student_id": 6, "course_id": 104, "score": 79, "semester": "Spring 2026"},
    {"student_id": 7, "course_id": 102, "score": 98, "semester": "Fall 2024"},
    {"student_id": 7, "course_id": 105, "score": 96, "semester": "Fall 2024"},
    {"student_id": 7, "course_id": 103, "score": 94, "semester": "Spring 2025"},
    {"student_id": 8, "course_id": 101, "score": 76, "semester": "Fall 2025"},
    {"student_id": 8, "course_id": 104, "score": 81, "semester": "Spring 2026"},
    {"student_id": 8, "course_id": 103, "score": 84, "semester": "Spring 2026"},
]

SAMPLE_QUESTIONS = [
    "Find all students with a GPA above 3.7",
    "Which students scored above 90 in Predictive Analytics?",
    "Show all courses taught by Prof. Mousavi",
    "Find students enrolled in both Managing Big Data and Predictive Analytics",
    "What is the average score for each course?",
    "Who got the highest score in Deep Learning & NLP?",
    "List all Business Analytics majors and their courses",
    "Find students whose interests relate to machine learning",
]
